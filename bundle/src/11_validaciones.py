# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Validaciones de punta a punta
# MAGIC
# MAGIC **Objetivo:** comprobar con datos que la solución hace lo que promete. Si alguna validación falla, el notebook (y el
# MAGIC job) terminan con error.
# MAGIC
# MAGIC | # | Validación | Criterio |
# MAGIC |---|---|---|
# MAGIC | 1 | Todas las fichas del volumen se leyeron y estructuraron | 100%, sin errores de análisis ni de extracción |
# MAGIC | 2 | La extracción es fiel al documento | ID y nombre correctos en ≥ 95% de las fichas |
# MAGIC | 3 | Se detectan los duplicados y conflictos plantados | ≥ 90% de los pares esperados |
# MAGIC | 4 | Se detectan las fichas incompletas | 100% |
# MAGIC | 5 | Toda diferencia entre el dominio declarado y el sugerido por la IA llega a revisión | 100% con hallazgo; concordancia ≥ 75% (informativa: la IA no es determinística) |
# MAGIC | 6 | El catálogo refleja las decisiones de revisión | IND-001 certificado; su duplicado retirado y como sinónimo |
# MAGIC | 7 | **La vista de métricas calcula IND-001 igual que un cálculo de referencia escrito a mano** | diferencia < 0.01 puntos en cada mes cerrado |
# MAGIC | 8 | Reglas de calidad y protocolo de contingencia evaluados | regla de contingencia de IND-001 presente y evaluada |
# MAGIC | 9 | Glosario y homologación | término oficial "Contacto Origen" con sus sinónimos |
# MAGIC | 10 | Agentes de Genie creados | 2 agentes |
# MAGIC
# MAGIC Las validaciones 2 a 5 usan la respuesta esperada de la muestra sintética; con fichas reales se reemplazan por una
# MAGIC muestra revisada manualmente (ver *Guía de implementación*, sección de validación).

# COMMAND ----------

import os
import re
import sys
import unicodedata

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
L, FU = f"`{catalogo}`.`{prefijo}_landing`", f"`{catalogo}`.`{prefijo}_fuentes`"
B, S, G = f"`{catalogo}`.`{prefijo}_bronze`", f"`{catalogo}`.`{prefijo}_silver`", f"`{catalogo}`.`{prefijo}_gold`"

raiz = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
sys.path.insert(0, f"/Workspace{raiz}")
from config.catalogo_fichas import INDICADORES  # noqa: E402

resultados = []


def validar(numero, nombre, ok, detalle):
    resultados.append((numero, nombre, "OK" if ok else "FALLA", str(detalle)))
    print(f"[{'OK' if ok else 'FALLA'}] {numero}. {nombre}: {detalle}")


def normal(t):
    t = unicodedata.normalize("NFKD", t or "").encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", t).strip()

# COMMAND ----------

# 1. Lectura completa
pdfs = [f for f in dbutils.fs.ls(f"/Volumes/{catalogo}/{prefijo}_landing/fichas_indicadores/recibidas") if f.name.endswith(".pdf")]
v = spark.sql(f"""SELECT count(*) AS n, count_if(id_indicador IS NULL) AS sin_id, count_if(error_extraccion IS NOT NULL) AS err
                  FROM {S}.fichas_vigentes""").first()
err_parse = spark.sql(f"SELECT count_if(cast(documento:error_status AS STRING) IS NOT NULL) FROM {B}.fichas_parseadas").first()[0]
validar(1, "Fichas leídas y estructuradas", v.n == len(pdfs) and v.sin_id == 0 and v.err == 0 and err_parse == 0,
        f"{v.n} de {len(pdfs)} PDF; sin ID: {v.sin_id}; errores de extracción: {v.err}; errores de análisis: {err_parse}")

# 2. Fidelidad de la extracción (ID y nombre)
esperado = {i["id"]: normal(i["nombre"]) for i in INDICADORES}
extraido = {r.id_indicador: normal(r.nombre) for r in spark.table(f"{S}.fichas_vigentes").collect()}
correctos = sum(1 for k, n in esperado.items() if extraido.get(k) == n)
validar(2, "Extracción fiel (ID y nombre)", correctos / len(esperado) >= 0.95, f"{correctos} de {len(esperado)}")

# COMMAND ----------

# 3. Duplicados y conflictos detectados
d = spark.sql(f"""WITH g AS (SELECT least(id_ficha, id_ficha_relacionada) a, greatest(id_ficha, id_ficha_relacionada) b, tipo_esperado
                             FROM {L}.respuesta_esperada_fichas_sinteticas WHERE id_ficha_relacionada IS NOT NULL
                               AND id_ficha IN (SELECT id_indicador FROM {S}.fichas_vigentes)),
                       h AS (SELECT least(id_ficha, id_ficha_relacionada) a, greatest(id_ficha, id_ficha_relacionada) b, tipo_hallazgo
                             FROM {G}.hallazgos_revision WHERE tipo_hallazgo IN ('DUPLICADO', 'CONFLICTO'))
                  SELECT count(*) AS esperados, count(h.a) AS detectados, count_if(h.tipo_hallazgo = g.tipo_esperado) AS tipo_correcto,
                         (SELECT count(*) FROM h LEFT ANTI JOIN g USING (a, b)) AS adicionales
                  FROM g LEFT JOIN h USING (a, b)""").first()
validar(3, "Duplicados y conflictos detectados", d.detectados / d.esperados >= 0.9,
        f"{d.detectados} de {d.esperados} pares; tipo correcto: {d.tipo_correcto}; pares adicionales propuestos por la IA: {d.adicionales}")

# 4. Fichas incompletas
i = spark.sql(f"""SELECT count(*) AS esperadas, count(h.id_ficha) AS detectadas
                  FROM {L}.respuesta_esperada_fichas_sinteticas g
                  LEFT JOIN {G}.hallazgos_revision h ON h.id_ficha = g.id_ficha AND h.tipo_hallazgo = 'INCOMPLETA'
                  WHERE g.tipo_esperado = 'INCOMPLETA'""").first()
validar(4, "Fichas incompletas detectadas", i.detectadas == i.esperadas, f"{i.detectadas} de {i.esperadas}")

# 5. Dominio: toda diferencia entre el dominio declarado y el sugerido debe llegar a revisión humana.
#    La concordancia varía entre ejecuciones (la IA no es determinística); se informa y se exige un mínimo de 75%.
fv = spark.table(f"{S}.fichas_vigentes").collect()
dom = {r.id_indicador: (r.dominio_sugerido or "") for r in fv}
coinciden = sum(1 for x in INDICADORES if normal(dom.get(x["id"])) == normal(x["dominio"]))
difieren = {r.id_indicador for r in fv if r.dominio_sugerido and normal(r.dominio_sugerido) != normal(r.dominio_declarado)}
con_hallazgo = {r.id_ficha for r in spark.sql(f"SELECT id_ficha FROM {G}.hallazgos_revision WHERE tipo_hallazgo = 'DOMINIO_A_CONFIRMAR'").collect()}
validar(5, "Diferencias de dominio enviadas a revisión", difieren <= con_hallazgo and coinciden / len(INDICADORES) >= 0.75,
        f"{len(difieren & con_hallazgo)} de {len(difieren)} diferencias con hallazgo; concordancia {coinciden} de {len(INDICADORES)}")

# COMMAND ----------

# 6. Catálogo
cat = {r.id_indicador: r for r in spark.table(f"{G}.catalogo_indicadores").collect()}
ok6 = (cat["IND-001"].estado_certificacion == "CERTIFICADO" and cat["IND-101"].estado_certificacion == "RETIRADO"
       and cat["IND-101"].reemplazada_por == "IND-001" and "IND-101" in (cat["IND-001"].fichas_unificadas or []))
estados = spark.sql(f"SELECT estado_certificacion, count(*) n FROM {G}.catalogo_indicadores GROUP BY ALL").collect()
validar(6, "Catálogo refleja las decisiones", ok6, {r.estado_certificacion: r.n for r in estados})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Vista de métricas vs. cálculo de referencia (IND-001)
# MAGIC El cálculo de referencia se escribe a mano a partir de la ficha: citas únicas con estatus del catálogo oficial,
# MAGIC sin reagendadas y sin cancelaciones del cliente con más de 24 horas de anticipación; tasa = asistidas / total × 100.

# COMMAND ----------

med = spark.sql(f"""SELECT vista, nombre FROM {G}.vistas_de_metricas_vigentes
                    WHERE id_indicador = 'IND-001' AND tipo = 'medida'""").first()
if med is None:
    validar(7, "Vista de métricas IND-001 = referencia", False, "No se creó la medida de IND-001")
else:
    comp = spark.sql(f"""
      WITH ref AS (
        SELECT date_trunc('MONTH', Fec_Cita) AS mes,
               100 * count(DISTINCT CASE WHEN Cod_Estatus = 'Asistió' THEN Id_Cita END) / count(DISTINCT Id_Cita) AS referencia
        FROM {FU}.tbl_citas_master
        WHERE Cod_Estatus IN ('Agendada', 'Asistió', 'Cancelada', 'No_Asistió')
          AND Cod_Tipificacion <> 'Reagendado'
          AND NOT (Cod_Estatus = 'Cancelada' AND timestampdiff(HOUR, Fec_Cancelacion, Fec_Cita) > 24)
        GROUP BY 1),
      mv AS (SELECT mes, MEASURE({med.nombre}) AS vista FROM {G}.{med.vista} GROUP BY mes)
      SELECT ref.mes, round(referencia, 4) AS referencia, round(vista, 4) AS vista, abs(referencia - vista) AS diferencia
      FROM ref JOIN mv ON ref.mes = mv.mes
      WHERE ref.mes < date_trunc('MONTH', current_date()) ORDER BY ref.mes""")
    display(comp)
    filas = comp.collect()
    maxdif = max(r.diferencia for r in filas) if filas else None
    validar(7, "Vista de métricas IND-001 = referencia", bool(filas) and maxdif < 0.01,
            f"{len(filas)} meses comparados; diferencia máxima {maxdif}")

# COMMAND ----------

# 8. Calidad y contingencia
c = spark.sql(f"""SELECT count(*) AS reglas, count_if(es_contingencia) AS contingencia FROM {G}.reglas_calidad WHERE id_indicador = 'IND-001'""").first()
e = spark.sql(f"SELECT * FROM {G}.estado_publicacion_indicadores WHERE id_indicador = 'IND-001'").first()
validar(8, "Reglas de calidad y contingencia", c.reglas >= 4 and c.contingencia >= 1 and e is not None,
        f"{c.reglas} reglas, {c.contingencia} de contingencia; publicar IND-001: {e.publicar if e else None}")

# 9. Glosario y homologación
h = spark.sql(f"""SELECT termino, sinonimos FROM {G}.glosario_negocio WHERE tipo_entrada = 'HOMOLOGACION' AND termino ILIKE '%contacto origen%'""").first()
validar(9, "Homologación de vocabulario", h is not None and len(h.sinonimos) >= 3, h.asDict() if h else "no encontrado")

# 10. Agentes de Genie
n = spark.table(f"{G}.agentes_genie").count()
validar(10, "Agentes de Genie", n == 2, f"{n} agentes")

# COMMAND ----------

res = spark.createDataFrame(resultados, "numero INT, validacion STRING, resultado STRING, detalle STRING")
res.write.mode("overwrite").saveAsTable(f"{G}.resultado_validaciones")
display(res)
fallas = [r for r in resultados if r[2] != "OK"]
assert not fallas, f"Validaciones con falla: {[f[1] for f in fallas]}"
print("Todas las validaciones pasaron.")
