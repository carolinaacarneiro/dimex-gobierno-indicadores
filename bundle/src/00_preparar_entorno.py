# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparar el entorno en Unity Catalog
# MAGIC
# MAGIC **Objetivo:** crear los esquemas y el volumen donde vive la solución de gobierno de indicadores.
# MAGIC
# MAGIC | Esquema | Para qué sirve |
# MAGIC |---|---|
# MAGIC | `<prefijo>_landing` | Volumen `fichas_indicadores`: aquí las áreas depositan las fichas PDF |
# MAGIC | `<prefijo>_fuentes` | Copia de los sistemas de origen que usan los indicadores (en la demo, datos sintéticos del CRM y de cartera) |
# MAGIC | `<prefijo>_bronze` | Resultado del análisis del PDF (`ai_parse_document`) |
# MAGIC | `<prefijo>_silver` | Fichas estructuradas en los 6 bloques de la plantilla |
# MAGIC | `<prefijo>_gold` | Catálogo certificado, glosario, dominios, hallazgos, decisiones, reglas de calidad y vistas de métricas |
# MAGIC
# MAGIC **¿Por qué así?** Separar las capas permite dar permisos distintos (las áreas solo escriben en el volumen; los
# MAGIC consumidores solo leen `gold`) y reprocesar una capa sin tocar las demás.
# MAGIC Documentación: [Unity Catalog](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/) ·
# MAGIC [Volúmenes](https://learn.microsoft.com/es-es/azure/databricks/volumes/)
# MAGIC
# MAGIC **Parámetros:** `catalogo`, `prefijo_esquemas`. **Duración:** < 1 minuto. Es idempotente: se puede ejecutar varias veces.

# COMMAND ----------

import re

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo), \
    "El catálogo y el prefijo solo pueden contener letras, números y guion bajo."

# COMMAND ----------

# MAGIC %md
# MAGIC ### Esquemas, volumen y etiquetas
# MAGIC Las etiquetas (`SET TAGS`) en los esquemas permiten encontrar y atribuir costos de todo lo que pertenece al proyecto.
# MAGIC Documentación: [Etiquetas en Unity Catalog](https://learn.microsoft.com/es-es/azure/databricks/database-objects/tags)

# COMMAND ----------

descripciones = {
    "landing": "Recepción de fichas de documentación de indicadores (PDF) enviadas por las áreas de negocio.",
    "fuentes": "Sistemas de origen de los indicadores (CRM, cartera). En la demo contiene datos sintéticos.",
    "bronze": "Resultado del análisis de los PDF con ai_parse_document.",
    "silver": "Fichas de indicadores estructuradas en los 6 bloques de la plantilla.",
    "gold": "Catálogo certificado de indicadores, glosario de negocio, dominios, revisión humana, calidad y vistas de métricas.",
}
spark.sql(f"USE CATALOG `{catalogo}`")
for capa, desc in descripciones.items():
    esquema = f"`{catalogo}`.`{prefijo}_{capa}`"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {esquema} COMMENT '{desc}'")
    spark.sql(f"ALTER SCHEMA {esquema} SET TAGS ('solucion' = 'gobierno_indicadores', 'capa' = '{capa}')")

spark.sql(f"""CREATE VOLUME IF NOT EXISTS `{catalogo}`.`{prefijo}_landing`.fichas_indicadores
              COMMENT 'Fichas PDF de indicadores. Subcarpetas: recibidas/ (entrada del pipeline) y exportaciones/ (salidas para otras herramientas).'""")
for carpeta in ["recibidas", "exportaciones"]:
    dbutils.fs.mkdirs(f"/Volumes/{catalogo}/{prefijo}_landing/fichas_indicadores/{carpeta}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

esquemas = {r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{catalogo}` LIKE '{prefijo}_*'").collect()}
faltantes = {f"{prefijo}_{c}" for c in descripciones} - esquemas
assert not faltantes, f"Faltan esquemas: {faltantes}"
display(dbutils.fs.ls(f"/Volumes/{catalogo}/{prefijo}_landing/fichas_indicadores/"))
print("Entorno listo. Siguiente paso: 01_generar_fichas_sinteticas")
