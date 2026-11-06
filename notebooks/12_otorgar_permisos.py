# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · Permisos de la solución
# MAGIC
# MAGIC **Objetivo:** que cada grupo vea solo lo que le corresponde y que la app de revisión funcione. Se ejecuta al final de
# MAGIC cada job (también después de publicar nuevas vistas de métricas), y solo otorga permisos sobre objetos que ya existen.
# MAGIC
# MAGIC | Quién | Qué recibe |
# MAGIC |---|---|
# MAGIC | Consumidores y revisores (`grupo_revisores`) | `SELECT` sobre los objetos publicados: catálogo, glosario, dominios, hallazgos, calidad, serie mensual, vistas de métricas `mv_*` y `silver.fichas_vigentes` (la usa Genie); `CAN_USE` sobre el SQL warehouse |
# MAGIC | Operación (`grupo_operacion`) | Además, `SELECT` sobre costos (`costos_solucion`), decisiones (`decisiones_revision`), avisos y el dashboard de operación |
# MAGIC | Data Stewards (`grupo_stewards`) | Además, `SELECT, MODIFY` sobre `reglas_calidad` y `definicion_vistas_de_metricas` (para aprobar vistas y reglas). Las aprobaciones de indicadores se registran solo desde la app, y `aprobadores_por_dominio` la administra el propietario del esquema (gobierno de datos) |
# MAGIC | Aprobadores (`grupo_aprobadores`) | Uso de la app de revisión (permiso `CAN_USE` en el bundle); la app escribe con su service principal |
# MAGIC | Service principal de la app | Lectura de fichas, catálogo y aprobadores por dominio; escritura solo en la cola, las decisiones y las aprobaciones |
# MAGIC
# MAGIC Siempre a grupos o service principals, nunca a usuarios. Los permisos a nivel de **catálogo** (`USE CATALOG`) los debe
# MAGIC otorgar el propietario del catálogo (ver *Guía de implementación*, sección 3.2); si el service principal que ejecuta
# MAGIC este notebook no puede, queda registrado en `gold.avisos_publicacion`.
# MAGIC Documentación: [Privilegios de Unity Catalog](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/manage-privileges/)

# COMMAND ----------

import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import WarehouseAccessControlRequest, WarehousePermissionLevel

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.text("grupo_revisores", "revisores_indicadores", "Grupo de consumidores y revisores")
dbutils.widgets.text("grupo_operacion", "operacion_datos", "Grupo de operación (costos y auditoría)")
dbutils.widgets.text("grupo_stewards", "stewards_indicadores", "Grupo de Data Stewards")
dbutils.widgets.text("nombre_app", "revision-indicadores", "Nombre de la app de revisión")
dbutils.widgets.text("warehouse_id", "", "ID del SQL warehouse")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
grupo = dbutils.widgets.get("grupo_revisores").strip()
grupo_op = dbutils.widgets.get("grupo_operacion").strip()
grupo_st = dbutils.widgets.get("grupo_stewards").strip()
nombre_app = dbutils.widgets.get("nombre_app").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
SILVER = f"`{catalogo}`.`{prefijo}_silver`"
GOLD = f"`{catalogo}`.`{prefijo}_gold`"
w = WorkspaceClient()
avisos = []


def existe(nombre):
    return spark.catalog.tableExists(f"{catalogo}.{nombre}")


def otorgar(sentencia):
    try:
        spark.sql(sentencia)
    except Exception as e:
        avisos.append(("12", "permiso", sentencia, str(e)[:300]))
        print(f"Aviso: no se pudo ejecutar '{sentencia}': {str(e)[:150]}")

# Aprobadores por dominio (opcional): si un dominio tiene filas, solo esos correos pueden decidir y aprobar en la app
spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.aprobadores_por_dominio (dominio STRING, correo STRING)
              COMMENT 'Opcional: quién puede decidir y aprobar en la app por dominio. Si un dominio no tiene filas, cualquier miembro del grupo de aprobadores puede hacerlo.'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consumidores y operación

# COMMAND ----------

vistas_mv = [r.viewName for r in spark.sql(f"SHOW VIEWS IN {GOLD} LIKE 'mv_*'").collect() if not r.viewName.endswith("_propuesta")]
publicados = ["catalogo_indicadores", "glosario_negocio", "reglas_homologacion", "dominios", "hallazgos_revision",
              "uso_vocabulario_no_homologado", "estado_publicacion_indicadores", "resultados_calidad", "serie_mensual_indicadores",
              "agentes_genie"] + vistas_mv
operacion = ["costos_solucion", "decisiones_revision", "pares_evaluados", "avisos_publicacion", "aprobaciones_indicadores",
             "reglas_calidad", "definicion_vistas_de_metricas"]

for g, objetos in [(grupo, publicados), (grupo_op, publicados + operacion), (grupo_st, publicados + operacion)]:
    if not g:
        continue
    otorgar(f"GRANT USE CATALOG ON CATALOG `{catalogo}` TO `{g}`")
    for esquema in (SILVER, GOLD):
        otorgar(f"GRANT USE SCHEMA ON SCHEMA {esquema} TO `{g}`")
    if existe(f"{prefijo}_silver.fichas_vigentes"):
        otorgar(f"GRANT SELECT ON TABLE {SILVER}.fichas_vigentes TO `{g}`")
    for o in objetos:
        if existe(f"{prefijo}_gold.{o}"):
            otorgar(f"GRANT SELECT ON TABLE {GOLD}.{o} TO `{g}`")

# Data Stewards: aprueban vistas de métricas, reglas de calidad e indicadores (guía 03, sección 4)
if grupo_st:
    for o in ("reglas_calidad", "definicion_vistas_de_metricas"):
        if existe(f"{prefijo}_gold.{o}"):
            otorgar(f"GRANT SELECT, MODIFY ON TABLE {GOLD}.{o} TO `{grupo_st}`")

# Uso del SQL warehouse (dashboard y Genie consultan con los permisos de cada usuario)
if warehouse_id:
    try:
        w.warehouses.update_permissions(warehouse_id, access_control_list=[
            WarehouseAccessControlRequest(group_name=g, permission_level=WarehousePermissionLevel.CAN_USE)
            for g in {grupo, grupo_op, grupo_st} if g])
    except Exception as e:
        avisos.append(("12", "permiso", f"CAN_USE warehouse {warehouse_id}", str(e)[:300]))
        print(f"Aviso: no se pudo dar CAN_USE sobre el warehouse: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Service principal de la app de revisión

# COMMAND ----------

try:
    sp = w.apps.get(nombre_app).service_principal_client_id
except Exception:
    sp = None
if sp:
    otorgar(f"GRANT USE CATALOG ON CATALOG `{catalogo}` TO `{sp}`")
    for esquema in (SILVER, GOLD):
        otorgar(f"GRANT USE SCHEMA ON SCHEMA {esquema} TO `{sp}`")
    if existe(f"{prefijo}_silver.fichas_vigentes"):
        otorgar(f"GRANT SELECT ON TABLE {SILVER}.fichas_vigentes TO `{sp}`")
    for t in ("catalogo_indicadores", "aprobadores_por_dominio"):
        if existe(f"{prefijo}_gold.{t}"):
            otorgar(f"GRANT SELECT ON TABLE {GOLD}.{t} TO `{sp}`")
    for t in ("hallazgos_revision", "decisiones_revision", "aprobaciones_indicadores"):
        if existe(f"{prefijo}_gold.{t}"):
            otorgar(f"GRANT SELECT, MODIFY ON TABLE {GOLD}.{t} TO `{sp}`")
else:
    avisos.append(("12", "permiso", f"app {nombre_app}", "No se encontró la app; otorgue sus permisos manualmente."))
    print(f"Aviso: no se encontró la app '{nombre_app}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Avisos y verificación

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.avisos_publicacion (notebook STRING, tipo STRING, objeto STRING, detalle STRING, fecha TIMESTAMP)
              COMMENT 'Acciones de gobierno (etiquetas, certificación, permisos) que no se pudieron aplicar; revísenla tras cada publicación.'""")
if avisos:
    (spark.createDataFrame(avisos, "notebook STRING, tipo STRING, objeto STRING, detalle STRING")
     .selectExpr("*", "current_timestamp() AS fecha").write.mode("append").saveAsTable(f"{GOLD}.avisos_publicacion"))
print(f"Permisos aplicados. Avisos: {len(avisos)} (ver gold.avisos_publicacion).")
