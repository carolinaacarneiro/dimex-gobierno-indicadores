# Pipeline de lectura de fichas de indicadores (Lakeflow Spark Declarative Pipelines).
#
#   volumen recibidas/*.pdf
#     -> bronze.fichas_parseadas      ai_parse_document: texto y estructura de cada PDF (incremental: solo archivos nuevos)
#     -> silver.fichas_estructuradas  ai_extract: los 6 bloques de la plantilla como campos; ai_classify: dominio sugerido
#     -> silver.fichas_vigentes       última versión de cada indicador + chequeos de completitud de la documentación
#
# Documentación:
#   https://learn.microsoft.com/es-es/azure/databricks/ldp/
#   https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_parse_document
#   https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_extract
#   https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_classify
import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = spark.conf.get("fichas.catalogo")
PREFIJO = spark.conf.get("fichas.prefijo_esquemas")
RUTA_FICHAS = spark.conf.get("fichas.ruta_fichas")
RUTA_CONFIG = spark.conf.get("fichas.ruta_configuracion")
BRONZE = f"`{CATALOGO}`.`{PREFIJO}_bronze`"
SILVER = f"`{CATALOGO}`.`{PREFIJO}_silver`"


def _sql_str(texto):
    return texto.replace("\\", "\\\\").replace("'", "\\'")


# Esquema de extracción: los 6 bloques de la plantilla v1.2 (ver config/esquema_ficha.json)
with open(f"{RUTA_CONFIG}/esquema_ficha.json", encoding="utf-8") as f:
    ESQUEMA = _sql_str(json.dumps(json.load(f), ensure_ascii=False))
INSTRUCCIONES = _sql_str(
    "Ficha de documentación de un indicador de negocio de una institución financiera mexicana (plantilla de 6 bloques). "
    "Extrae los valores tal como aparecen, en español, sin resumir. Si un campo dice 'Por definir', 'No aplica' o está vacío, "
    "devuélvelo tal cual o vacío. En Tipo de Indicador devuelve la opción marcada.")

# Dominios de datos con su descripción: guían a ai_classify (ver config/dominios.json)
with open(f"{RUTA_CONFIG}/dominios.json", encoding="utf-8") as f:
    DOMINIOS = _sql_str(json.dumps(json.load(f), ensure_ascii=False))


@dp.table(
    name=f"{BRONZE}.fichas_parseadas",
    comment="Una fila por versión de ficha PDF recibida, con el resultado de ai_parse_document (VARIANT).",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect("pdf_analizado_sin_error", "cast(documento:error_status AS STRING) IS NULL")
def fichas_parseadas():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")
            .option("pathGlobFilter", "*.pdf")
            .option("cloudFiles.allowOverwrites", "true")  # una ficha reemplazada con el mismo nombre se vuelve a leer
            .load(RUTA_FICHAS)
            .select(F.col("path").alias("ruta_archivo"),
                    F.col("modificationTime").alias("fecha_modificacion"),
                    F.col("length").alias("tamano_bytes"),
                    F.expr("ai_parse_document(content, map('version', '2.0'))").alias("documento"),
                    F.current_timestamp().alias("fecha_procesamiento")))


def _v(ruta):
    """Valor escalar de la respuesta de ai_extract v2.1 ({"value": ...})."""
    return F.expr(f"nullif(trim(cast(extraccion:response.{ruta}.value AS STRING)), '')")


@dp.table(
    name=f"{SILVER}.fichas_estructuradas",
    comment="Fichas de indicadores estructuradas en los 6 bloques de la plantilla (ai_extract) con dominio sugerido (ai_classify).",
)
def fichas_estructuradas():
    texto = "concat_ws('\\n\\n', transform(cast(documento:document:elements AS ARRAY<VARIANT>), e -> cast(e:content AS STRING)))"
    df = (spark.readStream.table(f"{BRONZE}.fichas_parseadas")
          .withColumn("texto_ficha", F.expr(texto))
          .withColumn("extraccion", F.expr(
              f"ai_extract(texto_ficha, '{ESQUEMA}', options => map('version', '2.1', 'instructions', '{INSTRUCCIONES}'))")))
    df = df.select(
        "ruta_archivo", "fecha_modificacion", "fecha_procesamiento",
        F.element_at(F.split("ruta_archivo", "/"), -1).alias("archivo"),
        # Bloque 1
        _v("identidad.id_indicador").alias("id_indicador"),
        F.regexp_replace(_v("identidad.nombre"), r"\.$", "").alias("nombre"),
        _v("identidad.objetivo_estrategico").alias("objetivo_estrategico"),
        _v("identidad.definicion_funcional").alias("definicion_funcional"),
        _v("identidad.meta_umbral").alias("meta_umbral"),
        _v("identidad.pregunta_negocio").alias("pregunta_negocio"),
        _v("identidad.criterios_exclusion").alias("criterios_exclusion"),
        _v("identidad.audiencia").alias("audiencia"),
        _v("identidad.unidad_medida").alias("unidad_medida"),
        # Bloque 2
        _v("clasificacion.tipo_indicador").alias("tipo_indicador"),
        _v("clasificacion.riesgos_asociados").alias("riesgos_asociados"),
        _v("clasificacion.dependencias").alias("dependencias"),
        # Bloque 3
        _v("tecnico.variable_a").alias("variable_a"),
        _v("tecnico.variable_b").alias("variable_b"),
        _v("tecnico.frecuencia_calculo").alias("frecuencia_calculo"),
        _v("tecnico.frecuencia_publicacion").alias("frecuencia_publicacion"),
        _v("tecnico.horario_operativo").alias("horario_operativo"),
        _v("tecnico.formula").alias("formula"),
        _v("tecnico.sistema_origen").alias("sistema_origen"),
        _v("tecnico.base_datos_origen").alias("base_datos_origen"),
        _v("tecnico.esquema_origen").alias("esquema_origen"),
        _v("tecnico.tabla_origen").alias("tabla_origen"),
        F.expr("transform(cast(extraccion:response.tecnico.campos_origen AS ARRAY<VARIANT>), c -> cast(c:value AS STRING))").alias("campos_origen"),
        # Bloque 4
        _v("operativo.area_responsable").alias("area_responsable"),
        _v("operativo.dominio_datos").alias("dominio_declarado"),
        _v("operativo.data_owner").alias("data_owner"),
        _v("operativo.data_steward").alias("data_steward"),
        _v("operativo.frecuencia_revision").alias("frecuencia_revision"),
        _v("operativo.fecha_creacion").alias("fecha_creacion"),
        _v("operativo.ultima_actualizacion").alias("ultima_actualizacion"),
        F.expr("transform(cast(extraccion:response.operativo.historial_cambios AS ARRAY<VARIANT>), c -> cast(c:value AS STRING))").alias("historial_cambios"),
        # Bloque 5
        _v("calidad.criterio_completitud").alias("criterio_completitud"),
        _v("calidad.criterio_validez").alias("criterio_validez"),
        _v("calidad.regla_unicidad").alias("regla_unicidad"),
        _v("calidad.regla_oportunidad").alias("regla_oportunidad"),
        _v("calidad.protocolo_contingencia").alias("protocolo_contingencia"),
        F.expr("transform(cast(extraccion:response.calidad.glosario AS ARRAY<VARIANT>), g -> "
               "named_struct('termino', cast(g:termino.value AS STRING), 'definicion', cast(g:definicion.value AS STRING)))").alias("glosario"),
        _v("calidad.homologacion_vocabulario").alias("homologacion_vocabulario"),
        # Bloque 6
        F.expr("transform(cast(extraccion:response.dimensiones.autorizadas AS ARRAY<VARIANT>), d -> "
               "named_struct('nombre', cast(d:nombre.value AS STRING), 'campo_tecnico', cast(d:campo_tecnico.value AS STRING)))").alias("dimensiones"),
        "texto_ficha",
        F.expr("cast(extraccion:error_message AS STRING)").alias("error_extraccion"),
    )
    # Dominio sugerido por IA a partir del contenido (el área que documenta no siempre es el dueño del dominio)
    contenido = ("concat('Indicador: ', coalesce(nombre, ''), '. Definición: ', coalesce(definicion_funcional, ''), "
                 "'. Variable A: ', coalesce(variable_a, ''), '. Variable B: ', coalesce(variable_b, ''), "
                 "'. Pregunta de negocio: ', coalesce(pregunta_negocio, ''))")
    return (df.withColumn("clasificacion_ia", F.expr(
                f"ai_classify({contenido}, '{DOMINIOS}', map('version', '2.1', 'instructions', "
                f"'Clasifica el indicador en el dominio de datos al que pertenece su concepto de negocio, no en el área que lo documenta.'))"))
              .withColumn("dominio_sugerido", F.expr("cast(clasificacion_ia:response[0].value AS STRING)"))
              .drop("clasificacion_ia"))


@dp.materialized_view(
    name=f"{SILVER}.fichas_vigentes",
    comment="Última versión de cada ficha (una fila por indicador), con indicadores de completitud de la documentación.",
)
@dp.expect("tiene_id_indicador", "id_indicador IS NOT NULL")
@dp.expect("tiene_data_owner", "falta_data_owner = false")
@dp.expect("tiene_origen_de_datos", "falta_origen = false")
@dp.expect("tiene_formula_con_variables", "formula_ambigua = false")
@dp.expect("extraccion_sin_error", "error_extraccion IS NULL")
def fichas_vigentes():
    # Una fila por indicador: la versión más reciente, aunque el área la envíe con otro nombre de archivo
    ultima = Window.partitionBy(F.coalesce("id_indicador", "ruta_archivo")).orderBy(F.col("fecha_modificacion").desc(), F.col("fecha_procesamiento").desc())
    vacio = lambda c: F.coalesce(F.lower(F.col(c)).isin("", "por definir", "por definir.", "no aplica", "no aplica."), F.lit(True))  # noqa: E731
    return (spark.read.table(f"{SILVER}.fichas_estructuradas")
            .withColumn("_n", F.row_number().over(ultima)).where("_n = 1").drop("_n")
            .withColumn("falta_data_owner", vacio("data_owner") | vacio("data_steward"))
            .withColumn("falta_origen", vacio("tabla_origen"))
            # Ambigua: sin Variable A, fórmula que no usa variables, o fórmula que usa la Variable B sin definirla
            .withColumn("formula_ambigua", vacio("variable_a") | ~F.coalesce(F.col("formula"), F.lit("")).rlike("(?i)variable")
                        | (F.col("formula").rlike("(?i)variable b") & vacio("variable_b")))
            .withColumn("falta_calidad", vacio("criterio_completitud") & vacio("criterio_validez"))
            .withColumn("falta_dimensiones", F.coalesce(F.size("dimensiones") == 0, F.lit(True)))
            .withColumn("meta_sin_umbral", ~F.coalesce(F.col("meta_umbral"), F.lit("")).rlike(r"\d")
                        & ~F.lower(F.coalesce(F.col("meta_umbral"), F.lit(""))).contains("informativo")))
