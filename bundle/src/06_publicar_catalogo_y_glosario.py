# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Publicar el catálogo certificado, el glosario y los dominios
# MAGIC
# MAGIC **Objetivo:** convertir las fichas revisadas en la **fuente única de verdad** de los indicadores, dentro de Unity Catalog:
# MAGIC
# MAGIC | Resultado | Qué contiene |
# MAGIC |---|---|
# MAGIC | `gold.catalogo_indicadores` | Una fila por indicador con su estado: **CERTIFICADO**, **PENDIENTE_APROBACION**, **EN_REVISION**, **DEVUELTO_AL_AREA** o **RETIRADO** (absorbido por la ficha oficial, cuyo nombre queda como sinónimo) |
# MAGIC | `gold.glosario_negocio` | Términos del Bloque 5, reglas de **homologación de vocabulario** y la definición oficial de cada indicador certificado |
# MAGIC | `gold.uso_vocabulario_no_homologado` | Fichas que todavía usan términos que la homologación reemplazó |
# MAGIC | `gold.dominios` | Dominios de datos con su descripción e indicadores certificados |
# MAGIC | Etiquetas de Unity Catalog | Dominio en tablas de origen y catálogo, responsables de negocio y técnicos, certificación |
# MAGIC | Exportación para *Pages* | Archivos Markdown por dominio en `exportaciones/pages/` para importar el glosario en Unity Catalog Pages (Beta) |
# MAGIC
# MAGIC Documentación: [Etiquetas gobernadas](https://learn.microsoft.com/es-es/azure/databricks/admin/governed-tags/) ·
# MAGIC [Dominios](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/domains) ·
# MAGIC [Certificación de activos](https://learn.microsoft.com/es-es/azure/databricks/database-objects/tags) ·
# MAGIC [Pages (Beta)](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/pages)
# MAGIC
# MAGIC **Duración:** 2–4 minutos. Se puede ejecutar cada vez que haya nuevas decisiones de revisión.

# COMMAND ----------

import json
import os
import re
import sys
import unicodedata

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.text("modelo_llm", "databricks-claude-sonnet-4-6", "Modelo para la homologación de vocabulario")
dbutils.widgets.text("tag_dominio", "dominio_datos", "Clave de la etiqueta de dominio")
dbutils.widgets.text("grupo_revisores", "revisores_indicadores", "Grupo de revisores y consumidores")
dbutils.widgets.text("nombre_app", "revision-indicadores", "Nombre de la app de revisión")
dbutils.widgets.dropdown("aprobar_indicadores_automaticamente", "no", ["si", "no"], "Aprobar indicadores sin hallazgos automáticamente (solo demo)")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
modelo_llm = dbutils.widgets.get("modelo_llm").strip()
tag_dominio = dbutils.widgets.get("tag_dominio").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", tag_dominio), "La clave de la etiqueta solo admite letras, números y guion bajo."
grupo = dbutils.widgets.get("grupo_revisores").strip()
nombre_app = dbutils.widgets.get("nombre_app").strip()
aprobar_ind_auto = dbutils.widgets.get("aprobar_indicadores_automaticamente") == "si"

SILVER = f"`{catalogo}`.`{prefijo}_silver`"
GOLD = f"`{catalogo}`.`{prefijo}_gold`"
FU = f"`{catalogo}`.`{prefijo}_fuentes`"
w = WorkspaceClient()

raiz = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
with open(f"/Workspace{raiz}/config/dominios.json", encoding="utf-8") as f:
    DOMINIOS = json.load(f)


def slug(texto):
    t = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", t).strip("_")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Estado de cada ficha según las decisiones de revisión
# MAGIC Precedencia: **RETIRADO** (otra ficha quedó como oficial) > **DEVUELTO_AL_AREA** > **EN_REVISION** (tiene
# MAGIC hallazgos pendientes o escalados) > **CERTIFICADO** (sin hallazgos pendientes **y aprobado por el Data Owner**) >
# MAGIC **PENDIENTE_APROBACION** (sin hallazgos, pero todavía sin aprobación). Que la IA no encuentre problemas no basta para
# MAGIC certificar: la aprobación se registra en la app (pestaña *Aprobar indicadores*) y queda en
# MAGIC `gold.aprobaciones_indicadores`. Si la ficha cambia, su huella cambia y la aprobación anterior deja de valer.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW decision_vigente AS
  SELECT h.*, d.decision, d.id_ficha_oficial, d.revisor, d.fecha_decision
  FROM {GOLD}.hallazgos_revision h
  LEFT JOIN (SELECT * FROM {GOLD}.decisiones_revision
             QUALIFY row_number() OVER (PARTITION BY id_hallazgo ORDER BY fecha_decision DESC) = 1) d USING (id_hallazgo)""")

spark.sql(f"""CREATE OR REPLACE TEMP VIEW participacion AS
  SELECT id_ficha AS id, * FROM decision_vigente
  UNION ALL SELECT id_ficha_relacionada AS id, * FROM decision_vigente WHERE id_ficha_relacionada IS NOT NULL""")

spark.sql(f"""CREATE OR REPLACE TEMP VIEW estado_fichas AS
  SELECT f.id_indicador,
    max(CASE WHEN p.decision IN ('UNIFICAR', 'DEFINIR_OFICIAL') AND p.id_ficha_oficial <> f.id_indicador THEN p.id_ficha_oficial END) AS reemplazada_por,
    max(CASE WHEN p.tipo_hallazgo = 'INCOMPLETA' AND p.decision = 'DEVOLVER_AL_AREA' THEN 1 ELSE 0 END) = 1 AS devuelta,
    max(CASE WHEN p.tipo_hallazgo <> 'DOMINIO_A_CONFIRMAR' AND (p.estado = 'PENDIENTE' OR p.decision = 'ESCALAR_A_COMITE') THEN 1 ELSE 0 END) = 1 AS en_revision,
    max(CASE WHEN p.tipo_hallazgo = 'DOMINIO_A_CONFIRMAR' AND p.decision = 'ACEPTAR_DOMINIO_SUGERIDO' THEN 1 ELSE 0 END) = 1 AS usar_dominio_sugerido,
    array_join(collect_set(CASE WHEN p.tipo_hallazgo = 'INCOMPLETA' THEN array_join(p.diferencias, '; ') END), '; ') AS faltantes
  FROM {SILVER}.fichas_vigentes f LEFT JOIN participacion p ON p.id = f.id_indicador
  WHERE f.id_indicador IS NOT NULL
  GROUP BY f.id_indicador""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Catálogo de indicadores

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.aprobaciones_indicadores (
  id_indicador STRING, huella_ficha STRING COMMENT 'Huella de la ficha aprobada (si la ficha cambia, la aprobación deja de valer).',
  aprobado_por STRING, fecha_aprobacion TIMESTAMP)
COMMENT 'Aprobaciones de los Data Owners: un indicador sin hallazgos se certifica solo si tiene aprobación vigente.'""")

spark.sql(f"""CREATE OR REPLACE TABLE {GOLD}.catalogo_indicadores
  COMMENT 'Catálogo oficial de indicadores de negocio (fuente única de verdad). Solo los indicadores en estado CERTIFICADO deben usarse en reportes.'
  AS
  WITH base0 AS (
    SELECT f.*, e.reemplazada_por, e.devuelta, e.en_revision, e.usar_dominio_sugerido, e.faltantes,
           sha2(concat_ws('|', f.nombre, f.definicion_funcional, f.meta_umbral, f.criterios_exclusion, f.variable_a, f.variable_b,
                          f.formula, f.unidad_medida, f.tabla_origen, f.data_owner, f.data_steward, f.criterio_completitud,
                          f.criterio_validez, f.regla_unicidad, f.regla_oportunidad, f.protocolo_contingencia), 256) AS huella_ficha
    FROM {SILVER}.fichas_vigentes f JOIN estado_fichas e USING (id_indicador)
    QUALIFY row_number() OVER (PARTITION BY f.id_indicador ORDER BY f.fecha_modificacion DESC) = 1),
  base AS (
    SELECT b0.*, ap.aprobado_por, ap.fecha_aprobacion
    FROM base0 b0 LEFT JOIN (SELECT * FROM {GOLD}.aprobaciones_indicadores
                             QUALIFY row_number() OVER (PARTITION BY id_indicador, huella_ficha ORDER BY fecha_aprobacion DESC) = 1) ap
      ON ap.id_indicador = b0.id_indicador AND ap.huella_ficha = b0.huella_ficha),
  absorbidas AS (
    SELECT reemplazada_por AS id_indicador, collect_set(nombre) AS sinonimos, collect_set(id_indicador) AS fichas_unificadas
    FROM base WHERE reemplazada_por IS NOT NULL GROUP BY reemplazada_por)
  SELECT b.id_indicador, b.nombre,
         CASE WHEN b.usar_dominio_sugerido THEN b.dominio_sugerido ELSE coalesce(b.dominio_declarado, b.dominio_sugerido) END AS dominio,
         CASE WHEN b.reemplazada_por IS NOT NULL THEN 'RETIRADO'
              WHEN b.devuelta THEN 'DEVUELTO_AL_AREA'
              WHEN b.en_revision THEN 'EN_REVISION'
              WHEN b.aprobado_por IS NOT NULL THEN 'CERTIFICADO'
              ELSE 'PENDIENTE_APROBACION' END AS estado_certificacion,
         CASE WHEN b.reemplazada_por IS NOT NULL THEN concat('Unificado en ', b.reemplazada_por, '.')
              WHEN b.devuelta THEN concat('Devuelto al área para completar: ', b.faltantes, '.')
              WHEN b.en_revision THEN 'Tiene hallazgos pendientes de revisión.'
              WHEN b.aprobado_por IS NOT NULL THEN concat('Sin hallazgos pendientes; aprobado por ', b.aprobado_por, '.')
              ELSE 'Sin hallazgos pendientes; falta la aprobación del Data Owner.' END AS motivo_estado,
         b.reemplazada_por,
         array_except(coalesce(a.sinonimos, array()), array(b.nombre)) AS sinonimos, coalesce(a.fichas_unificadas, array()) AS fichas_unificadas,
         b.tipo_indicador, b.area_responsable, b.data_owner, b.data_steward,
         b.objetivo_estrategico, b.definicion_funcional, b.pregunta_negocio, b.meta_umbral, b.unidad_medida, b.criterios_exclusion,
         b.variable_a, b.variable_b, b.formula, b.frecuencia_calculo, b.frecuencia_publicacion, b.horario_operativo,
         b.sistema_origen, concat_ws('.', b.base_datos_origen, b.esquema_origen, b.tabla_origen) AS tabla_origen_documentada,
         b.tabla_origen, b.campos_origen, b.dimensiones, b.audiencia, b.frecuencia_revision, b.ultima_actualizacion,
         b.criterio_completitud, b.criterio_validez, b.regla_unicidad, b.regla_oportunidad, b.protocolo_contingencia,
         b.archivo AS archivo_ficha, CAST(NULL AS STRING) AS vista_de_metricas, current_timestamp() AS fecha_publicacion,
         b.huella_ficha, b.aprobado_por, b.fecha_aprobacion
  FROM base b LEFT JOIN absorbidas a USING (id_indicador)""")

if aprobar_ind_auto:  # solo demo: el "Data Owner" aprueba todo lo que no tiene hallazgos pendientes
    spark.sql(f"""INSERT INTO {GOLD}.aprobaciones_indicadores
                  SELECT id_indicador, huella_ficha, 'aprobación automática (demo)', current_timestamp()
                  FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'PENDIENTE_APROBACION'""")
    spark.sql(f"""UPDATE {GOLD}.catalogo_indicadores
                  SET estado_certificacion = 'CERTIFICADO', aprobado_por = 'aprobación automática (demo)', fecha_aprobacion = current_timestamp(),
                      motivo_estado = 'Sin hallazgos pendientes; aprobado por aprobación automática (demo).'
                  WHERE estado_certificacion = 'PENDIENTE_APROBACION'""")

comentarios = {
    "estado_certificacion": "CERTIFICADO, PENDIENTE_APROBACION, EN_REVISION, DEVUELTO_AL_AREA o RETIRADO.",
    "dominio": "Dominio de datos del indicador (confirmado en la revisión).",
    "sinonimos": "Nombres de fichas duplicadas que se unificaron en este indicador.",
    "reemplazada_por": "Si la ficha fue retirada, ID del indicador oficial que la reemplaza.",
    "vista_de_metricas": "Vista de métricas de Unity Catalog donde el indicador está implementado (si aplica).",
    "data_owner": "Puesto que brinda el visto bueno funcional del indicador.",
    "data_steward": "Puesto responsable de la implementación y ejecución del indicador.",
}
for c, t in comentarios.items():
    spark.sql(f"COMMENT ON COLUMN {GOLD}.catalogo_indicadores.{c} IS '{t}'")
display(spark.sql(f"SELECT estado_certificacion, count(*) AS indicadores FROM {GOLD}.catalogo_indicadores GROUP BY ALL ORDER BY ALL"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Glosario de negocio y homologación de vocabulario
# MAGIC Las reglas de homologación están en texto libre ("Los conceptos de A, B y C se unifican bajo el término D"). Con
# MAGIC `ai_query` y una respuesta estructurada las convertimos en datos: término oficial y términos que reemplaza.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {GOLD}.reglas_homologacion
  COMMENT 'Reglas de homologación de vocabulario extraídas de las fichas (término oficial y términos que reemplaza).'
  AS SELECT id_indicador, dominio, r.termino_oficial, r.terminos_reemplazados
  FROM (SELECT c.id_indicador, c.dominio,
               from_json(ai_query('{modelo_llm}',
                 concat('Extrae la regla de homologación de vocabulario del siguiente texto de una ficha de indicador. ',
                        'termino_oficial es el término único que se debe usar; terminos_reemplazados son los términos que ese término unifica o reemplaza. ',
                        'Si el texto solo dice que dos términos son sinónimos sin definir uno oficial, usa como oficial el primero. Texto: ', f.homologacion_vocabulario),
                 responseFormat => 'STRUCT<regla:STRUCT<termino_oficial:STRING, terminos_reemplazados:ARRAY<STRING>>>'),
               'termino_oficial STRING, terminos_reemplazados ARRAY<STRING>') AS r
        FROM {GOLD}.catalogo_indicadores c JOIN {SILVER}.fichas_vigentes f USING (id_indicador)
        WHERE c.estado_certificacion <> 'RETIRADO'
          AND lower(coalesce(f.homologacion_vocabulario, 'no aplica')) NOT IN ('no aplica', 'no aplica.', ''))""")
spark.table(f"{GOLD}.reglas_homologacion").createOrReplaceTempView("homologaciones")

spark.sql(f"""CREATE OR REPLACE TABLE {GOLD}.glosario_negocio
  COMMENT 'Glosario de negocio: términos definidos en las fichas, reglas de homologación de vocabulario y definiciones oficiales de indicadores certificados.'
  AS
  SELECT termino, tipo_entrada, first(definicion) AS definicion, array_distinct(flatten(collect_list(sinonimos))) AS sinonimos,
         first(dominio) AS dominio, array_sort(collect_set(id_indicador)) AS fichas_fuente
  FROM (
    SELECT g.termino, 'TERMINO' AS tipo_entrada, g.definicion, array() AS sinonimos, c.dominio, c.id_indicador
    FROM {GOLD}.catalogo_indicadores c JOIN {SILVER}.fichas_vigentes f USING (id_indicador)
    LATERAL VIEW explode(f.glosario) t AS g
    WHERE c.estado_certificacion <> 'RETIRADO' AND g.termino IS NOT NULL
      AND lower(g.termino) NOT RLIKE '^(sin t[eé]rminos|no aplica|por definir)'
    UNION ALL
    SELECT termino_oficial, 'HOMOLOGACION', concat('Término oficial que unifica: ', array_join(terminos_reemplazados, ', '), '.'),
           terminos_reemplazados, dominio, id_indicador
    FROM homologaciones WHERE termino_oficial IS NOT NULL
    UNION ALL
    SELECT nombre, 'INDICADOR', definicion_funcional, sinonimos, dominio, id_indicador
    FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'CERTIFICADO')
  GROUP BY termino, tipo_entrada""")

# Fichas vigentes que todavía usan un término reemplazado por la homologación
spark.sql(f"""CREATE OR REPLACE TABLE {GOLD}.uso_vocabulario_no_homologado
  COMMENT 'Fichas que usan términos reemplazados por una regla de homologación de vocabulario.'
  AS
  WITH reglas AS (SELECT termino_oficial, explode(terminos_reemplazados) AS termino_reemplazado FROM homologaciones
                  WHERE termino_oficial IS NOT NULL)
  SELECT DISTINCT c.id_indicador, c.nombre, c.area_responsable, r.termino_reemplazado AS termino_encontrado, r.termino_oficial
  FROM {GOLD}.catalogo_indicadores c JOIN {SILVER}.fichas_vigentes f USING (id_indicador) CROSS JOIN reglas r
  WHERE c.estado_certificacion <> 'RETIRADO'
    AND lower(r.termino_reemplazado) <> lower(r.termino_oficial)
    AND instr(lower(concat_ws(' ', f.nombre, f.definicion_funcional, f.pregunta_negocio, f.variable_a, f.variable_b)),
              lower(r.termino_reemplazado)) > 0""")
display(spark.table(f"{GOLD}.uso_vocabulario_no_homologado"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Dominios de datos

# COMMAND ----------

filas = [(d, slug(d), desc) for d, desc in DOMINIOS.items()] + \
        [("Gobierno de Datos", "gobierno_de_datos", "Catálogo de indicadores, glosario, revisión y calidad de la información.")]
spark.createDataFrame(filas, "dominio STRING, valor_etiqueta STRING, descripcion STRING").createOrReplaceTempView("dominios_base")
spark.sql(f"""CREATE OR REPLACE TABLE {GOLD}.dominios
  COMMENT 'Dominios de datos: valor de la etiqueta gobernada, descripción, indicadores certificados y responsables.'
  AS SELECT d.dominio, d.valor_etiqueta, d.descripcion,
            count_if(c.estado_certificacion = 'CERTIFICADO') AS indicadores_certificados,
            count(c.id_indicador) AS fichas_totales,
            array_sort(collect_set(c.data_owner)) AS data_owners
     FROM dominios_base d LEFT JOIN {GOLD}.catalogo_indicadores c ON c.dominio = d.dominio
     GROUP BY d.dominio, d.valor_etiqueta, d.descripcion""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Etiquetas de dominio, responsables y certificación
# MAGIC Cada tabla de origen y del catálogo recibe la etiqueta de dominio (`dominio_datos`), los responsables del Bloque 4 y,
# MAGIC las oficiales, la certificación de sistema (`system.certification_status = certified`).
# MAGIC
# MAGIC Las tablas de origen se toman de `config/mapeo_fuentes.json` (las mismas que usan las vistas de métricas y las reglas
# MAGIC de calidad). Etiquetar tablas de otros equipos requiere el privilegio `APPLY TAG` sobre ellas; si falta o la tabla no
# MAGIC existe, se muestra un aviso y el resto de la publicación continúa.
# MAGIC
# MAGIC **Evolución recomendada (no implementada en la demo):** crear los **Dominios** de Unity Catalog en la página
# MAGIC *Discover*. Cada Dominio se basa en una **etiqueta gobernada propia** (una clave por dominio, por ejemplo
# MAGIC `oportunidades` o `cartera_y_cobranza`, tomadas de `gold.dominios.valor_etiqueta`). Requiere permisos de cuenta; ver
# MAGIC la guía *Gobierno, costos y monitoreo*.

# COMMAND ----------

with open(f"/Workspace{raiz}/config/mapeo_fuentes.json", encoding="utf-8") as f:
    MAPEO = {k: v for k, v in json.load(f).items() if not k.startswith("_")}


def nombre_tabla(t):
    return t if "." in t else f"{FU}.`{t}`"


avisos = []  # se guardan en gold.avisos_publicacion para que no pasen inadvertidos


def etiquetar(objeto, etiquetas):
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in etiquetas.items() if v)
    try:
        spark.sql(f"ALTER TABLE {objeto} SET TAGS ({pares})")
    except Exception as e:  # tabla inexistente o sin APPLY TAG: no detiene la publicación
        avisos.append(("06", "etiqueta", objeto.replace("`", ""), str(e)[:300]))
        print(f"Aviso: no se pudo etiquetar {objeto}: {str(e)[:150]}")


certificados = spark.sql(f"""SELECT tabla_origen_documentada, dominio, data_owner, data_steward
                             FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'CERTIFICADO'""").collect()
for clave, m in MAPEO.items():
    usan = [c for c in certificados if (c.tabla_origen_documentada or "").lower() == clave.lower()]
    doms = [c.dominio for c in usan if c.dominio]
    dominio = m.get("dominio") or (slug(max(set(doms), key=doms.count)) if doms else None)
    etiquetas = {tag_dominio: dominio}
    if usan:  # responsables (Bloque 4) de los indicadores certificados que usan la tabla
        etiquetas.update({"responsable_negocio": slug(usan[0].data_owner or ""), "responsable_tecnico": slug(usan[0].data_steward or "")})
    etiquetar(nombre_tabla(m["tabla"]), etiquetas)
    for j in m.get("joins", []):
        etiquetar(nombre_tabla(j["tabla"]), {tag_dominio: j.get("dominio")})
for t in ["catalogo_indicadores", "glosario_negocio", "reglas_homologacion", "dominios", "hallazgos_revision", "decisiones_revision", "uso_vocabulario_no_homologado"]:
    etiquetar(f"{GOLD}.{t}", {tag_dominio: "gobierno_de_datos", "solucion": "gobierno_indicadores"})

# El catálogo oficial queda certificado
for t_cert in ("catalogo_indicadores", "glosario_negocio"):
    try:
        spark.sql(f"SET TAG ON TABLE {GOLD}.{t_cert} `system.certification_status` = `certified`")
    except Exception as e:  # requiere ASSIGN sobre la etiqueta de sistema system.certification_status
        avisos.append(("06", "certificacion", t_cert, str(e)[:300]))
        print(f"Aviso: no se pudo certificar {t_cert}: {str(e)[:150]}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.avisos_publicacion (notebook STRING, tipo STRING, objeto STRING, detalle STRING, fecha TIMESTAMP)
              COMMENT 'Acciones de gobierno (etiquetas, certificación, permisos) que no se pudieron aplicar; revísenla tras cada publicación.'""")
if avisos:
    spark.createDataFrame(avisos, "notebook STRING, tipo STRING, objeto STRING, detalle STRING") \
        .selectExpr("*", "current_timestamp() AS fecha").write.mode("append").saveAsTable(f"{GOLD}.avisos_publicacion")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Exportación para Unity Catalog Pages (Beta)
# MAGIC *Pages* es la capacidad de Unity Catalog para publicar definiciones de negocio organizadas por dominio; está en
# MAGIC **Beta** y se alimenta desde la interfaz (incluida una importación asistida a partir de documentos). Dejamos un
# MAGIC archivo Markdown por dominio, listo para esa importación. La tabla `glosario_negocio` sigue siendo la fuente.

# COMMAND ----------

ruta_pages = f"/Volumes/{catalogo}/{prefijo}_landing/fichas_indicadores/exportaciones/pages"
dbutils.fs.mkdirs(ruta_pages)
glosario = spark.table(f"{GOLD}.glosario_negocio").orderBy("dominio", "tipo_entrada", "termino").collect()
for dominio in sorted({g.dominio for g in glosario if g.dominio}):
    lineas = [f"# Glosario de negocio — {dominio}", ""]
    for g in [x for x in glosario if x.dominio == dominio]:
        lineas += [f"## {g.termino}", "", g.definicion or "", ""]
        if g.sinonimos:
            lineas += [f"**Sinónimos:** {', '.join(g.sinonimos)}", ""]
        lineas += [f"**Tipo:** {g.tipo_entrada.lower()} · **Fichas fuente:** {', '.join(g.fichas_fuente)}", ""]
    with open(f"{ruta_pages}/{slug(dominio)}.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))
print(f"Exportación para Pages en {ruta_pages}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

n_cert = spark.sql(f"SELECT count(*) FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'CERTIFICADO'").first()[0]
if aprobar_ind_auto:
    assert n_cert > 0, "No hay indicadores certificados"
pend = spark.sql(f"SELECT count(*) FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'PENDIENTE_APROBACION'").first()[0]
if pend:
    print(f"{pend} indicadores sin hallazgos esperan la aprobación del Data Owner (app de revisión, pestaña Aprobar indicadores).")
display(spark.sql(f"SELECT tipo_entrada, count(*) AS entradas FROM {GOLD}.glosario_negocio GROUP BY ALL"))
print(f"{n_cert} indicadores certificados. Siguiente paso: 07_generar_vistas_de_metricas")
