# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Detectar duplicados, conflictos y fichas incompletas
# MAGIC
# MAGIC **Objetivo:** construir la **cola de revisión** (`<prefijo>_gold.hallazgos_revision`) que los responsables
# MAGIC resuelven en la app de revisión. Con ~600 fichas, comparar todas contra todas a mano (≈180,000 pares) no es viable;
# MAGIC por eso trabajamos en dos etapas:
# MAGIC
# MAGIC 1. **Búsqueda semántica (AI Search):** para cada ficha buscamos las 5 fichas más parecidas por significado
# MAGIC    (nombre, definición, variables, fórmula y exclusiones), aunque usen otras palabras. Solo los pares con similitud
# MAGIC    alta pasan a la etapa 2.
# MAGIC 2. **Juez con IA (`ai_query`):** un modelo de lenguaje compara cada par y decide si es **DUPLICADO** (mismo
# MAGIC    concepto y misma regla), **CONFLICTO** (mismo concepto con regla, umbral, exclusiones, fuente o meta distintos),
# MAGIC    **RELACIONADO** o **DISTINTO**, y explica las diferencias. La IA **propone**; la decisión es humana.
# MAGIC
# MAGIC Además se registran como hallazgos las **fichas incompletas** (reglas de completitud del pipeline) y los
# MAGIC **dominios a confirmar** (el dominio que declara el área no coincide con el que sugiere la IA).
# MAGIC
# MAGIC Documentación: [AI Search (antes Vector Search)](https://learn.microsoft.com/es-es/azure/databricks/ai-search/ai-search) ·
# MAGIC [ai_query](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_query) ·
# MAGIC [Salidas estructuradas](https://learn.microsoft.com/es-es/azure/databricks/machine-learning/model-serving/structured-outputs)
# MAGIC
# MAGIC **Es incremental:** los pares que ya se evaluaron no se vuelven a enviar al modelo, salvo que cambie el contenido de
# MAGIC alguna de las dos fichas (huella). **Duración:** 3–10 minutos
# MAGIC (la primera vez se crea el endpoint de AI Search).

# COMMAND ----------

import re
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, EndpointType,
                                                 PipelineType, VectorIndexType)
from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.text("modelo_llm", "databricks-claude-sonnet-4-6", "Modelo para el juez")
dbutils.widgets.text("modelo_embeddings", "databricks-qwen3-embedding-0-6b", "Modelo de embeddings")
dbutils.widgets.text("endpoint_vector_search", "indicadores-vs", "Endpoint de AI Search (Vector Search)")
dbutils.widgets.text("umbral_similitud", "0.60", "Similitud mínima para evaluar un par")
dbutils.widgets.dropdown("reiniciar_revision", "no", ["si", "no"], "Reiniciar la cola y las decisiones (solo demo)")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
modelo_llm = dbutils.widgets.get("modelo_llm").strip()
modelo_emb = dbutils.widgets.get("modelo_embeddings").strip()
endpoint_vs = dbutils.widgets.get("endpoint_vector_search").strip()
umbral = float(dbutils.widgets.get("umbral_similitud"))
reiniciar = dbutils.widgets.get("reiniciar_revision") == "si"

SILVER = f"`{catalogo}`.`{prefijo}_silver`"
GOLD = f"`{catalogo}`.`{prefijo}_gold`"
tabla_busqueda = f"{catalogo}.{prefijo}_gold.fichas_para_busqueda"
indice = f"{catalogo}.{prefijo}_gold.fichas_para_busqueda_indice"
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas de la revisión
# MAGIC `hallazgos_revision` es la cola; `decisiones_revision` guarda cada decisión con usuario y fecha (auditoría).

# COMMAND ----------

if reiniciar:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD}.hallazgos_revision")
    spark.sql(f"DROP TABLE IF EXISTS {GOLD}.decisiones_revision")
    spark.sql(f"DROP TABLE IF EXISTS {GOLD}.pares_evaluados")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.hallazgos_revision (
  id_hallazgo STRING NOT NULL COMMENT 'Identificador del hallazgo (tipo + fichas involucradas).',
  tipo_hallazgo STRING COMMENT 'DUPLICADO, CONFLICTO, INCOMPLETA o DOMINIO_A_CONFIRMAR.',
  id_ficha STRING COMMENT 'ID del indicador de la ficha.',
  nombre STRING, area STRING,
  id_ficha_relacionada STRING COMMENT 'ID del indicador con el que se compara (duplicados y conflictos).',
  nombre_relacionado STRING, area_relacionada STRING,
  similitud DOUBLE COMMENT 'Puntaje de similitud semántica de AI Search (0 a 1).',
  diferencias ARRAY<STRING> COMMENT 'Diferencias concretas entre las fichas o elementos faltantes.',
  justificacion STRING COMMENT 'Explicación del hallazgo (IA o regla de completitud).',
  recomendacion STRING COMMENT 'Acción sugerida al revisor.',
  ficha_sugerida_oficial STRING COMMENT 'Ficha que la IA sugiere como oficial (duplicados y conflictos).',
  prioridad STRING COMMENT 'Alta, Media o Baja.',
  estado STRING COMMENT 'PENDIENTE o RESUELTO.',
  fecha_deteccion TIMESTAMP)
COMMENT 'Cola de revisión humana de fichas de indicadores.'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.decisiones_revision (
  id_decision STRING NOT NULL, id_hallazgo STRING NOT NULL,
  decision STRING COMMENT 'UNIFICAR, DEFINIR_OFICIAL, MANTENER_AMBAS, ESCALAR_A_COMITE, DEVOLVER_AL_AREA, ACEPTAR, ACEPTAR_DOMINIO_SUGERIDO o MANTENER_DOMINIO_DECLARADO.',
  id_ficha_oficial STRING COMMENT 'Ficha que queda como oficial al unificar o definir la oficial.',
  comentario STRING, revisor STRING COMMENT 'Usuario que tomó la decisión.', fecha_decision TIMESTAMP)
COMMENT 'Decisiones de la revisión humana (auditoría).'""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.pares_evaluados (
  id_a STRING, id_b STRING, similitud DOUBLE, relacion STRING, fecha_evaluacion TIMESTAMP,
  huella_a STRING COMMENT 'Huella (hash) del contenido de la ficha A al evaluarse.', huella_b STRING)
COMMENT 'Pares de fichas ya evaluados por el juez de IA. Un par se vuelve a evaluar si cambia el contenido de alguna de las dos fichas.'""")

# Migración: versiones anteriores de pares_evaluados no tenían las huellas
if "huella_a" not in spark.table(f"{GOLD}.pares_evaluados").columns:
    spark.sql(f"ALTER TABLE {GOLD}.pares_evaluados ADD COLUMNS (huella_a STRING, huella_b STRING)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Texto de búsqueda por ficha e índice de AI Search
# MAGIC Usamos un índice **Delta Sync con embeddings administrados**: Databricks calcula los embeddings con el modelo
# MAGIC indicado y mantiene el índice sincronizado con la tabla (Change Data Feed). El modelo de embeddings es multilingüe
# MAGIC para comparar bien textos en español.

# COMMAND ----------

# La huella resume el contenido relevante de la ficha: si un área modifica su ficha, la huella cambia y sus pares y
# hallazgos se vuelven a evaluar (los hallazgos nuevos quedan PENDIENTES y el indicador vuelve a revisión).
(spark.table(f"{SILVER}.fichas_vigentes").where("id_indicador IS NOT NULL")
 .withColumn("huella", F.sha2(F.concat_ws("|", "nombre", "definicion_funcional", "meta_umbral", "criterios_exclusion", "variable_a",
                                          "variable_b", "formula", "unidad_medida", "tabla_origen", "data_owner", "data_steward",
                                          "criterio_completitud", "criterio_validez", "regla_unicidad", "regla_oportunidad",
                                          "protocolo_contingencia"), 256))
 .select("id_indicador", "nombre", "area_responsable", "huella",
         F.concat_ws("\n",
                     F.concat(F.lit("Indicador: "), F.col("nombre")),
                     F.concat(F.lit("Definición: "), F.coalesce("definicion_funcional", F.lit(""))),
                     F.concat(F.lit("Variable A: "), F.coalesce("variable_a", F.lit(""))),
                     F.concat(F.lit("Variable B: "), F.coalesce("variable_b", F.lit(""))),
                     F.concat(F.lit("Fórmula: "), F.coalesce("formula", F.lit(""))),
                     F.concat(F.lit("Exclusiones: "), F.coalesce("criterios_exclusion", F.lit(""))),
                     F.concat(F.lit("Unidad: "), F.coalesce("unidad_medida", F.lit("")))).alias("texto_busqueda"))
 .write.mode("overwrite").option("overwriteSchema", "true")
 .option("delta.enableChangeDataFeed", "true").saveAsTable(tabla_busqueda))
spark.sql(f"ALTER TABLE {tabla_busqueda} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
n_fichas = spark.table(tabla_busqueda).count()
print(f"{n_fichas} fichas para búsqueda")

# COMMAND ----------

if endpoint_vs not in [e.name for e in w.vector_search_endpoints.list_endpoints()]:
    print(f"Creando endpoint {endpoint_vs} (puede tardar unos minutos)...")
    w.vector_search_endpoints.create_endpoint_and_wait(name=endpoint_vs, endpoint_type=EndpointType.STANDARD)

try:
    w.vector_search_indexes.get_index(indice)
    w.vector_search_indexes.sync_index(indice)
except Exception as e:
    if "does not exist" not in str(e).lower() and "not found" not in str(e).lower() and "RESOURCE_DOES_NOT_EXIST" not in str(e):
        raise
    w.vector_search_indexes.create_index(
        name=indice, endpoint_name=endpoint_vs, primary_key="id_indicador", index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=tabla_busqueda, pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(name="texto_busqueda", embedding_model_endpoint_name=modelo_emb)],
            columns_to_sync=["id_indicador", "nombre", "area_responsable", "texto_busqueda"]))

# Esperar a que el índice termine de sincronizar todas las fichas
for _ in range(120):
    st = w.vector_search_indexes.get_index(indice).status
    if st.ready and (st.indexed_row_count or 0) >= n_fichas:
        break
    time.sleep(15)
else:
    raise TimeoutError(f"El índice no terminó de sincronizar: {st}")
print(f"Índice listo: {st.indexed_row_count} fichas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Pares candidatos (fichas más parecidas)

# COMMAND ----------

huellas = {r.id_indicador: r.huella for r in spark.table(tabla_busqueda).select("id_indicador", "huella").collect()}
pares = {}
for f in spark.table(tabla_busqueda).collect():
    r = w.vector_search_indexes.query_index(index_name=indice, columns=["id_indicador"], query_text=f.texto_busqueda, num_results=6)
    for fila in (r.result.data_array or []):
        vecino, score = fila[0], float(fila[-1])
        if vecino != f.id_indicador and score >= umbral:
            clave = tuple(sorted([f.id_indicador, vecino]))
            pares[clave] = max(score, pares.get(clave, 0))

ya = {(r.id_a, r.id_b, r.huella_a, r.huella_b) for r in spark.table(f"{GOLD}.pares_evaluados").collect()}
nuevos = [(a, b, s, huellas[a], huellas[b]) for (a, b), s in pares.items() if (a, b, huellas[a], huellas[b]) not in ya]
print(f"{len(pares)} pares con similitud >= {umbral}; {len(nuevos)} nuevos o modificados para evaluar")
spark.createDataFrame(nuevos, "id_a STRING, id_b STRING, similitud DOUBLE, huella_a STRING, huella_b STRING").createOrReplaceTempView("pares_nuevos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Juez con IA
# MAGIC `responseFormat` (con un único campo de primer nivel, `juicio`) obliga al modelo a responder con una estructura fija (relación, diferencias, justificación,
# MAGIC recomendación y ficha sugerida como oficial), lista para guardarse en la tabla.

# COMMAND ----------

def ficha_txt(p):
    campos = [("ID", "id_indicador"), ("Nombre", "nombre"), ("Área", "area_responsable"), ("Tipo", "tipo_indicador"),
              ("Definición", "definicion_funcional"), ("Meta", "meta_umbral"), ("Exclusiones", "criterios_exclusion"),
              ("Variable A", "variable_a"), ("Variable B", "variable_b"), ("Fórmula", "formula"), ("Unidad", "unidad_medida"),
              ("Frecuencia", "frecuencia_calculo"), ("Tabla de origen", "tabla_origen"), ("Data Owner", "data_owner")]
    return "concat_ws('\\n', " + ", ".join(f"concat('{e}: ', coalesce({p}.{c}, ''))" for e, c in campos) + ")"


instrucciones = (
    "Eres un experto en gobierno de datos de una institución financiera mexicana. Compara dos fichas de documentación de "
    "indicadores de negocio y clasifica su relación en exactamente una categoría: "
    "DUPLICADO = miden el mismo concepto con la misma regla de cálculo (mismas variables, exclusiones y umbrales de definición), "
    "aunque cambie el nombre, el área, la redacción o el formato de la meta; "
    "CONFLICTO = miden el mismo concepto o llevan el mismo nombre, pero difieren en algo que cambia el resultado o su lectura: "
    "regla de cálculo, criterios de exclusión, umbral de definición (por ejemplo días de atraso), población, fuente de datos, "
    "unidad o una meta sustantivamente distinta; "
    "RELACIONADO = conceptos distintos que comparten fuente o variables; DISTINTO = no tienen relación relevante. "
    "En diferencias lista solo diferencias concretas y verificables (máximo 5). "
    "En ficha_sugerida_oficial indica el ID de la ficha más completa o del área dueña natural del concepto. Responde en español.")

juicios = spark.sql(f"""
  WITH p AS (
    SELECT n.id_a, n.id_b, n.similitud, n.huella_a, n.huella_b, {ficha_txt('a')} AS ficha_a, {ficha_txt('b')} AS ficha_b
    FROM pares_nuevos n
    JOIN {SILVER}.fichas_vigentes a ON a.id_indicador = n.id_a
    JOIN {SILVER}.fichas_vigentes b ON b.id_indicador = n.id_b)
  SELECT id_a, id_b, similitud, huella_a, huella_b,
         ai_query('{modelo_llm}',
                  concat('{instrucciones}', '\\n\\nFICHA 1\\n', ficha_a, '\\n\\nFICHA 2\\n', ficha_b),
                  responseFormat => 'STRUCT<juicio:STRUCT<relacion:STRING, diferencias:ARRAY<STRING>, justificacion:STRING, recomendacion:STRING, ficha_sugerida_oficial:STRING>>'
         ) AS respuesta
  FROM p""")
juicios = (juicios.withColumn("j", F.from_json("respuesta", "relacion STRING, diferencias ARRAY<STRING>, justificacion STRING, recomendacion STRING, ficha_sugerida_oficial STRING"))
           .select("id_a", "id_b", "similitud", "huella_a", "huella_b", F.upper(F.trim("j.relacion")).alias("relacion"), "j.diferencias", "j.justificacion",
                   "j.recomendacion", "j.ficha_sugerida_oficial"))
juicios.write.mode("overwrite").saveAsTable(f"{GOLD}._juicios_tmp")
juicios = spark.table(f"{GOLD}._juicios_tmp")
display(juicios.groupBy("relacion").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Registrar hallazgos en la cola de revisión
# MAGIC `MERGE` solo inserta hallazgos nuevos: los que ya existen conservan su estado y sus decisiones.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW hallazgos_nuevos AS
  -- Duplicados y conflictos propuestos por el juez
  SELECT sha2(concat_ws('|', j.relacion, j.id_a, j.id_b, j.huella_a, j.huella_b), 256) AS id_hallazgo, j.relacion AS tipo_hallazgo,
         j.id_a AS id_ficha, a.nombre, a.area_responsable AS area,
         j.id_b AS id_ficha_relacionada, b.nombre AS nombre_relacionado, b.area_responsable AS area_relacionada,
         round(j.similitud, 4) AS similitud, j.diferencias, j.justificacion, j.recomendacion, j.ficha_sugerida_oficial,
         CASE WHEN j.relacion = 'CONFLICTO' AND (a.tipo_indicador IN ('Estratégico', 'Regulatorio') OR b.tipo_indicador IN ('Estratégico', 'Regulatorio')) THEN 'Alta'
              ELSE 'Media' END AS prioridad
  FROM {GOLD}._juicios_tmp j
  JOIN {SILVER}.fichas_vigentes a ON a.id_indicador = j.id_a
  JOIN {SILVER}.fichas_vigentes b ON b.id_indicador = j.id_b
  WHERE j.relacion IN ('DUPLICADO', 'CONFLICTO')
  UNION ALL
  -- Fichas incompletas (reglas de completitud del pipeline)
  SELECT sha2(concat_ws('|', 'INCOMPLETA', id_indicador, array_join(faltantes, ',')), 256), 'INCOMPLETA', id_indicador, nombre, area_responsable,
         NULL, NULL, NULL, NULL, faltantes,
         concat('La ficha no cumple con la documentación mínima: ', array_join(faltantes, '; '), '.'),
         'Devolver al área responsable para completar la ficha antes de certificar el indicador.', NULL,
         CASE WHEN tipo_indicador IN ('Estratégico', 'Regulatorio') THEN 'Alta' ELSE 'Media' END
  FROM (SELECT *, filter(array(
                 CASE WHEN falta_data_owner THEN 'Falta Data Owner o Data Steward' END,
                 CASE WHEN falta_origen THEN 'Falta el origen de datos (sistema, tabla y campos)' END,
                 CASE WHEN formula_ambigua THEN 'La fórmula no está expresada con variables definidas' END,
                 CASE WHEN falta_calidad THEN 'Faltan reglas de calidad (Bloque 5)' END,
                 CASE WHEN falta_dimensiones THEN 'Faltan dimensiones de análisis autorizadas (Bloque 6)' END,
                 CASE WHEN meta_sin_umbral THEN 'La meta no tiene un umbral medible' END), x -> x IS NOT NULL) AS faltantes
        FROM {SILVER}.fichas_vigentes) WHERE size(faltantes) > 0
  UNION ALL
  -- Dominio declarado distinto al sugerido por la IA
  SELECT sha2(concat_ws('|', 'DOMINIO_A_CONFIRMAR', id_indicador, dominio_sugerido), 256), 'DOMINIO_A_CONFIRMAR', id_indicador, nombre,
         area_responsable, NULL, NULL, NULL, NULL,
         array(concat('Dominio declarado: ', dominio_declarado), concat('Dominio sugerido: ', dominio_sugerido)),
         concat('El área declaró el dominio "', dominio_declarado, '", pero por su concepto el indicador corresponde a "', dominio_sugerido, '".'),
         'Confirmar con el Data Owner el dominio al que pertenece el indicador.', NULL, 'Baja'
  FROM {SILVER}.fichas_vigentes
  WHERE dominio_sugerido IS NOT NULL AND lower(trim(dominio_declarado)) <> lower(trim(dominio_sugerido))""")

spark.sql(f"""MERGE INTO {GOLD}.hallazgos_revision t USING hallazgos_nuevos s ON t.id_hallazgo = s.id_hallazgo
  WHEN NOT MATCHED THEN INSERT (id_hallazgo, tipo_hallazgo, id_ficha, nombre, area, id_ficha_relacionada, nombre_relacionado,
    area_relacionada, similitud, diferencias, justificacion, recomendacion, ficha_sugerida_oficial, prioridad, estado, fecha_deteccion)
  VALUES (s.id_hallazgo, s.tipo_hallazgo, s.id_ficha, s.nombre, s.area, s.id_ficha_relacionada, s.nombre_relacionado,
    s.area_relacionada, s.similitud, s.diferencias, s.justificacion, s.recomendacion, s.ficha_sugerida_oficial, s.prioridad,
    'PENDIENTE', current_timestamp())""")
spark.sql(f"""INSERT INTO {GOLD}.pares_evaluados (id_a, id_b, similitud, relacion, fecha_evaluacion, huella_a, huella_b)
              SELECT id_a, id_b, similitud, relacion, current_timestamp(), huella_a, huella_b FROM {GOLD}._juicios_tmp""")
spark.sql(f"DROP TABLE IF EXISTS {GOLD}._juicios_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

resumen = spark.sql(f"SELECT tipo_hallazgo, estado, count(*) AS hallazgos FROM {GOLD}.hallazgos_revision GROUP BY ALL ORDER BY ALL")
display(resumen)
if reiniciar:  # en la demo siempre hay hallazgos; en producción un lote sin hallazgos es un resultado válido
    assert spark.table(f"{GOLD}.hallazgos_revision").count() > 0, "No se generaron hallazgos"
print("Cola de revisión lista. Siguiente paso: revisión humana en la app (en la demo, 05_simular_revision).")
