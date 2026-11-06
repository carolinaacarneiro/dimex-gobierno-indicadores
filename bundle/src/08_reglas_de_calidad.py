# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Reglas de calidad del Bloque 5, ejecutadas sobre los datos
# MAGIC
# MAGIC **Objetivo:** que las reglas de calidad que cada área escribe en su ficha (completitud, validez, unicidad,
# MAGIC oportunidad y protocolo de contingencia) **se ejecuten de verdad** sobre la tabla de origen en cada carga, y no
# MAGIC queden solo en el documento.
# MAGIC
# MAGIC 1. `ai_query` convierte el texto del Bloque 5 en reglas estructuradas (`gold.reglas_calidad`). Solo se procesan
# MAGIC    indicadores nuevos o cuyo Bloque 5 cambió; las demás reglas se reutilizan. **Recomendamos que el Data Steward
# MAGIC    revise y apruebe las reglas** (en particular `filtro` y `condicion_valida`, que son fragmentos SQL) antes de usarlas
# MAGIC    en producción.
# MAGIC 2. Un evaluador genérico traduce cada regla **aprobada** a SQL y la ejecuta sobre la **última carga** de la tabla de
# MAGIC    origen. Los fragmentos SQL (`filtro`, `condicion_valida`) solo se aceptan si usan columnas de la tabla y
# MAGIC    operadores simples (sin subconsultas ni sentencias). Para aprobar:
# MAGIC    `UPDATE <catalogo>.<prefijo>_gold.reglas_calidad SET aprobada = true, aprobada_por = current_user() WHERE id_regla = '<id>'`.
# MAGIC 3. Los resultados quedan en `gold.resultados_calidad` (historial) y `gold.estado_publicacion_indicadores` aplica el
# MAGIC    **protocolo de contingencia**: si falla una regla de contingencia, el indicador queda *en pausa* y la alerta SQL
# MAGIC    notifica al Steward.
# MAGIC
# MAGIC Documentación: [ai_query](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_query) ·
# MAGIC [Alertas de Databricks SQL](https://learn.microsoft.com/es-es/azure/databricks/sql/user/alerts/)

# COMMAND ----------

import json
import os
import re

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.text("modelo_llm", "databricks-claude-sonnet-4-6", "Modelo")
dbutils.widgets.dropdown("aprobar_reglas_automaticamente", "no", ["si", "no"], "Aprobar reglas nuevas automáticamente (solo demo)")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
modelo_llm = dbutils.widgets.get("modelo_llm").strip()
aprobar_auto = dbutils.widgets.get("aprobar_reglas_automaticamente") == "si"
GOLD = f"`{catalogo}`.`{prefijo}_gold`"
FU = f"`{catalogo}`.`{prefijo}_fuentes`"

raiz = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
with open(f"/Workspace{raiz}/config/mapeo_fuentes.json", encoding="utf-8") as f:
    MAPEO = {k.lower(): v for k, v in json.load(f).items() if not k.startswith("_")}


def nombre_tabla(t):
    return t if "." in t else f"{FU}.`{t}`"


def sql_str(t):
    return (t or "").replace("\\", "\\\\").replace("'", "\\'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Del texto de la ficha a reglas estructuradas

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.reglas_calidad (
  id_regla STRING, id_indicador STRING, tabla STRING COMMENT 'Tabla de Unity Catalog donde se evalúa la regla.',
  tipo STRING COMMENT 'COMPLETITUD, VALIDEZ, UNICIDAD u OPORTUNIDAD.',
  columnas ARRAY<STRING>, umbral_minimo_pct DOUBLE COMMENT 'Porcentaje mínimo de registros que deben cumplir.',
  valores_validos ARRAY<STRING>, condicion_valida STRING COMMENT 'Condición SQL que debe cumplir un registro válido (alternativa a valores_validos).',
  filtro STRING COMMENT 'Condición SQL que delimita los registros a evaluar (por ejemplo, solo citas asistidas).',
  columna_inicio STRING, columna_fin STRING, horas_maximas DOUBLE,
  es_contingencia BOOLEAN COMMENT 'Verdadero si su incumplimiento activa el protocolo de contingencia.',
  descripcion STRING, fecha_alta TIMESTAMP,
  huella_ficha STRING COMMENT 'Huella del Bloque 5 de la ficha de la que se extrajo la regla (si cambia, se vuelve a extraer).',
  aprobada BOOLEAN COMMENT 'Solo se evalúan las reglas aprobadas por el Data Steward.', aprobada_por STRING)
COMMENT 'Reglas de calidad de los indicadores certificados, derivadas del Bloque 5 de las fichas.'""")

# Migración: versiones anteriores de la tabla no tenían las columnas huella_ficha y aprobada
columnas_actuales = spark.table(f"{GOLD}.reglas_calidad").columns
if "huella_ficha" not in columnas_actuales:
    spark.sql(f"ALTER TABLE {GOLD}.reglas_calidad ADD COLUMNS (huella_ficha STRING COMMENT 'Huella del Bloque 5 de la ficha de la que se extrajo la regla.')")
if "aprobada" not in columnas_actuales:
    spark.sql(f"ALTER TABLE {GOLD}.reglas_calidad ADD COLUMNS (aprobada BOOLEAN COMMENT 'Solo se evalúan las reglas aprobadas por el Data Steward.', aprobada_por STRING)")

existentes = {r.id_indicador: r.huella_ficha for r in spark.table(f"{GOLD}.reglas_calidad").select("id_indicador", "huella_ficha").distinct().collect()}
certificados = spark.sql(f"""SELECT *, sha2(concat_ws('|', criterio_completitud, criterio_validez, regla_unicidad, regla_oportunidad,
                                                     protocolo_contingencia, tabla_origen_documentada), 256) AS huella_ficha
                             FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'CERTIFICADO'""").collect()
# Se extraen reglas para indicadores nuevos o cuyo Bloque 5 cambió; las reglas anteriores de esos indicadores se reemplazan
pendientes = [c for c in certificados
              if (c.tabla_origen_documentada or "").lower() in MAPEO and existentes.get(c.id_indicador) != c.huella_ficha]
print(f"Indicadores con reglas nuevas o modificadas por extraer: {[c.id_indicador for c in pendientes]}")

FORMATO = ("STRUCT<resultado:STRUCT<reglas:ARRAY<STRUCT<tipo:STRING, columnas:ARRAY<STRING>, umbral_minimo_pct:DOUBLE, valores_validos:ARRAY<STRING>, "
           "condicion_valida:STRING, filtro:STRING, columna_inicio:STRING, columna_fin:STRING, horas_maximas:DOUBLE, "
           "es_contingencia:BOOLEAN, descripcion:STRING>>>>")
INSTRUCCIONES = """Convierte las reglas de calidad de la ficha de un indicador en reglas estructuradas que se evaluarán sobre la tabla indicada.
Tipos: COMPLETITUD (columnas que no deben ser nulas; umbral_minimo_pct, 100 si no permite nulos; una regla por umbral distinto),
VALIDEZ (una columna; valores_validos si hay catálogo, o condicion_valida en SQL de Databricks; umbral_minimo_pct 100 si no se indica),
UNICIDAD (columnas de la llave; umbral_minimo_pct 100),
OPORTUNIDAD (columna_inicio y columna_fin timestamps de la tabla, horas_maximas como límite de falla, filtro SQL para los registros a los que aplica; umbral_minimo_pct 95 si no se indica).
Usa solo columnas que existen en el diccionario. Si una regla no se puede evaluar con las columnas disponibles, omítela.
es_contingencia = true solo para la regla cuyo incumplimiento dispara el protocolo de contingencia de la ficha (usa el umbral que indica el protocolo).
descripcion: frase breve en español."""

nuevas = []
for c in pendientes:
    m = MAPEO[c.tabla_origen_documentada.lower()]
    tabla = nombre_tabla(m["tabla"])
    dicc = "\n".join(f"- {r.col_name} ({r.data_type}): {r.comment or ''}" for r in spark.sql(f"DESCRIBE TABLE {tabla}").collect()
                     if r.col_name and not r.col_name.startswith("#"))
    texto = (f"{INSTRUCCIONES}\n\nDiccionario de la tabla:\n{dicc}\n\nFicha {c.id_indicador} - {c.nombre}\n"
             f"Completitud: {c.criterio_completitud}\nValidez: {c.criterio_validez}\nUnicidad: {c.regla_unicidad}\n"
             f"Oportunidad: {c.regla_oportunidad}\nProtocolo de contingencia: {c.protocolo_contingencia}")
    r = json.loads(spark.sql(f"SELECT ai_query('{modelo_llm}', '{sql_str(texto)}', responseFormat => '{FORMATO}') AS r").first().r)
    reglas = r["reglas"] if isinstance(r, dict) and "reglas" in r else r  # objeto interno o lista directa
    for i, regla in enumerate(reglas, start=1):
        nuevas.append({"id_regla": f"{c.id_indicador}-R{i:02d}", "id_indicador": c.id_indicador, "tabla": tabla.replace("`", ""),
                       "huella_ficha": c.huella_ficha, **regla, "aprobada": aprobar_auto,
                       "aprobada_por": "aprobación automática (demo)" if aprobar_auto else None})

if pendientes:
    spark.sql(f"DELETE FROM {GOLD}.reglas_calidad WHERE id_indicador IN ({', '.join(repr(c.id_indicador) for c in pendientes)})")
if nuevas:
    esquema = spark.table(f"{GOLD}.reglas_calidad").schema
    filas = [tuple(n.get(f.name) for f in esquema) for n in nuevas]
    (spark.createDataFrame(filas, schema=esquema).withColumn("fecha_alta", F.current_timestamp())
     .write.mode("append").saveAsTable(f"{GOLD}.reglas_calidad"))
display(spark.table(f"{GOLD}.reglas_calidad"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Evaluación sobre la última carga
# MAGIC Cada tipo de regla se traduce a una consulta SQL fija; así el resultado es reproducible y auditable.

# COMMAND ----------

def columna_carga(tabla):
    for m in MAPEO.values():
        if nombre_tabla(m["tabla"]).replace("`", "") == tabla:
            return m.get("columna_carga")


def lista(v):
    return ", ".join(f"'{sql_str(x)}'" for x in v)


PROHIBIDAS = re.compile(r"(;|--|/\*|\b(select|from|join|insert|update|delete|merge|drop|alter|create|grant|revoke|call|exec|union)\b)", re.I)


def fragmento_seguro(sql, tabla):
    """Acepta solo expresiones simples sobre columnas de la tabla: sin subconsultas, sentencias ni comentarios."""
    if not sql:
        return True
    if PROHIBIDAS.search(sql):
        return False
    columnas = {c.lower() for c in spark.table(tabla).columns}
    palabras = {w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", re.sub(r"'[^']*'", "", sql))}
    permitidas = {"and", "or", "not", "is", "null", "in", "between", "like", "rlike", "true", "false", "case", "when", "then",
                  "else", "end", "coalesce", "trim", "lower", "upper", "length", "current_date", "current_timestamp", "date_sub",
                  "timestampdiff", "hour", "day", "interval", "cast", "as", "string", "int", "double", "date", "timestamp",
                  "last_day", "date_trunc", "to_date", "datediff", "add_months", "year", "month", "abs", "round", "nvl", "ifnull"}
    return palabras <= columnas | permitidas


def consulta(r):
    cc = columna_carga(r.tabla)
    universo = f"SELECT * FROM {r.tabla}" + (f" WHERE {cc} = (SELECT max({cc}) FROM {r.tabla})" if cc else "")
    if r.filtro:
        universo = f"SELECT * FROM ({universo}) WHERE {r.filtro}"
    if r.tipo == "COMPLETITUD":
        cumple = " AND ".join(f"{c} IS NOT NULL" for c in r.columnas)
        return f"SELECT 100 * count_if({cumple}) / count(*) AS valor, count(*) AS n FROM ({universo})"
    if r.tipo == "VALIDEZ":
        col = r.columnas[0]
        cond = f"{col} IN ({lista(r.valores_validos)})" if r.valores_validos else r.condicion_valida
        return f"SELECT 100 * count_if({cond}) / count_if({col} IS NOT NULL) AS valor, count_if({col} IS NOT NULL) AS n FROM ({universo})"
    if r.tipo == "UNICIDAD":
        llave = ", ".join(r.columnas)
        return f"SELECT 100 * count(DISTINCT {llave}) / count(*) AS valor, count(*) AS n FROM ({universo}) WHERE {' AND '.join(f'{c} IS NOT NULL' for c in r.columnas)}"
    if r.tipo == "OPORTUNIDAD":
        return (f"SELECT 100 * count_if(timestampdiff(HOUR, {r.columna_inicio}, {r.columna_fin}) <= {r.horas_maximas}) / count(*) AS valor, "
                f"count(*) AS n FROM ({universo}) WHERE {r.columna_inicio} IS NOT NULL AND {r.columna_fin} IS NOT NULL")
    raise ValueError(f"Tipo de regla no soportado: {r.tipo}")


if aprobar_auto:  # solo demo: aprueba también las reglas existentes que no tenían aprobación
    spark.sql(f"UPDATE {GOLD}.reglas_calidad SET aprobada = true, aprobada_por = 'aprobación automática (demo)' WHERE aprobada IS NULL")
resultados = []
reglas_aprobadas = spark.table(f"{GOLD}.reglas_calidad").where("aprobada").collect()
pendientes_aprobacion = spark.table(f"{GOLD}.reglas_calidad").where("NOT coalesce(aprobada, false)").count()
if pendientes_aprobacion:
    print(f"{pendientes_aprobacion} reglas esperan la aprobación del Data Steward (UPDATE ... SET aprobada = true) y no se evalúan.")
for r in reglas_aprobadas:
    cols_tabla = {c.lower() for c in spark.table(r.tabla).columns}
    cols_regla = [c for c in list(r.columnas or []) + [r.columna_inicio, r.columna_fin] if c]
    if not (fragmento_seguro(r.filtro, r.tabla) and fragmento_seguro(r.condicion_valida, r.tabla)
            and all(c.lower() in cols_tabla for c in cols_regla)):
        resultados.append((r.id_regla, r.id_indicador, r.tabla, r.tipo, r.descripcion, None, r.umbral_minimo_pct, None, None,
                           bool(r.es_contingencia), "Fragmento SQL rechazado por la validación de seguridad"))
        continue
    try:
        fila = spark.sql(consulta(r)).first()
        valor = float(fila.valor) if fila.valor is not None else None
        umbral = r.umbral_minimo_pct if r.umbral_minimo_pct is not None else 100.0
        resultados.append((r.id_regla, r.id_indicador, r.tabla, r.tipo, r.descripcion, valor, umbral,
                           valor is not None and valor + 1e-9 >= umbral, int(fila.n), bool(r.es_contingencia), None))
    except Exception as e:
        resultados.append((r.id_regla, r.id_indicador, r.tabla, r.tipo, r.descripcion, None, r.umbral_minimo_pct, None, None,
                           bool(r.es_contingencia), str(e)[:300]))

esquema_res = ("id_regla STRING, id_indicador STRING, tabla STRING, tipo STRING, descripcion STRING, valor_obtenido_pct DOUBLE, "
               "umbral_pct DOUBLE, cumple BOOLEAN, registros_evaluados BIGINT, es_contingencia BOOLEAN, error STRING")
(spark.createDataFrame(resultados, esquema_res).withColumn("fecha_evaluacion", F.current_timestamp())
 .write.mode("append").option("mergeSchema", "true").saveAsTable(f"{GOLD}.resultados_calidad"))
spark.sql(f"COMMENT ON TABLE {GOLD}.resultados_calidad IS 'Historial de evaluaciones de las reglas de calidad (una fila por regla y ejecución).'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Protocolo de contingencia: ¿se puede publicar el indicador?

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {GOLD}.estado_publicacion_indicadores
  COMMENT 'Estado de publicación de cada indicador según su última evaluación de calidad (protocolo de contingencia).'
  AS WITH ultima AS (SELECT * FROM {GOLD}.resultados_calidad
                     QUALIFY fecha_evaluacion = max(fecha_evaluacion) OVER (PARTITION BY id_indicador))
  SELECT id_indicador, max(fecha_evaluacion) AS fecha_evaluacion,
         NOT coalesce(bool_or(es_contingencia AND NOT coalesce(cumple, false)), false) AS publicar,
         count_if(NOT coalesce(cumple, false)) AS reglas_incumplidas, count(*) AS reglas_evaluadas,
         array_join(collect_list(CASE WHEN es_contingencia AND NOT coalesce(cumple, false)
                    THEN concat(descripcion, ' (', round(valor_obtenido_pct, 2), '% vs. mínimo ', umbral_pct, '%)') END), '; ') AS motivo_pausa
  FROM ultima GROUP BY id_indicador""")
display(spark.table(f"{GOLD}.estado_publicacion_indicadores"))
display(spark.sql(f"""SELECT id_regla, tipo, descripcion, round(valor_obtenido_pct, 2) AS valor, umbral_pct, cumple, es_contingencia, error
                      FROM {GOLD}.resultados_calidad QUALIFY fecha_evaluacion = max(fecha_evaluacion) OVER () ORDER BY id_regla"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

sin_evaluar = [r for r in resultados if r[-1]]
if sin_evaluar:
    print(f"Aviso: {len(sin_evaluar)} reglas no se pudieron evaluar; revíselas en gold.reglas_calidad: {[r[0] for r in sin_evaluar]}")
assert spark.table(f"{GOLD}.reglas_calidad").count() > 0, "No se generaron reglas de calidad"
if not reglas_aprobadas:
    print("Aviso: no hay reglas aprobadas; apruebe las reglas en gold.reglas_calidad para evaluarlas.")
print("Reglas evaluadas. Siguiente paso: 09_crear_agentes_genie")
