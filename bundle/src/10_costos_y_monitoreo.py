# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Costos y monitoreo de la solución
# MAGIC
# MAGIC **Objetivo:** que el equipo pueda operar la solución sabiendo **cuánto cuesta**, **qué se ejecutó** y **quién
# MAGIC accedió o decidió qué**.
# MAGIC
# MAGIC | Qué | Cómo | Dónde verlo |
# MAGIC |---|---|---|
# MAGIC | Costo por solución | `system.billing.usage` por job, pipeline, endpoint de AI Search y app (y por etiquetas de la política de uso serverless, si se configura) | vista `gold.costos_solucion`, dashboard de operación |
# MAGIC | Costo de las funciones de IA y AI Search | `system.billing.usage` por producto (`billing_origin_product`) | misma vista |
# MAGIC | Ejecuciones | `system.lakeflow.job_run_timeline` | consulta de abajo |
# MAGIC | Linaje | `system.access.table_lineage` y pestaña *Lineage* de Catalog Explorer | consulta de abajo |
# MAGIC | Decisiones de revisión | `gold.decisiones_revision` (usuario y fecha) | app y dashboard |
# MAGIC
# MAGIC Los datos de facturación pueden tardar algunas horas en aparecer.
# MAGIC Documentación: [Tablas del sistema de facturación](https://learn.microsoft.com/es-es/azure/databricks/admin/system-tables/billing) ·
# MAGIC [Etiquetas de uso](https://learn.microsoft.com/es-es/azure/databricks/admin/account-settings/usage-detail-tags) ·
# MAGIC [Linaje](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/data-lineage)

# COMMAND ----------

import re

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.text("endpoint_vector_search", "indicadores-vs", "Endpoint de AI Search (Vector Search)")
dbutils.widgets.text("nombre_app", "revision-indicadores", "Nombre de la app de revisión")
dbutils.widgets.text("warehouse_id_costos", "", "ID del SQL warehouse exclusivo de la solución (vacío si es compartido)")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
GOLD = f"`{catalogo}`.`{prefijo}_gold`"
endpoint_vs = re.sub(r"[^A-Za-z0-9_-]", "", dbutils.widgets.get("endpoint_vector_search"))
nombre_app = re.sub(r"[^A-Za-z0-9_-]", "", dbutils.widgets.get("nombre_app"))
warehouse_id = re.sub(r"[^A-Za-z0-9]", "", dbutils.widgets.get("warehouse_id_costos"))
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Recursos de la solución y sus etiquetas
# MAGIC Identificamos los jobs y el pipeline por la etiqueta `proyecto = gobierno_indicadores`. Para cómputo serverless, la
# MAGIC forma documentada de que las etiquetas lleguen a `system.billing.usage.custom_tags` es una política de uso serverless;
# MAGIC por eso la vista de costos también filtra por los identificadores de los jobs, pipelines, endpoint y app.

# COMMAND ----------

jobs = [(str(j.job_id), j.settings.name, (j.settings.tags or {}).get("proyecto")) for j in w.jobs.list()
        if (j.settings.tags or {}).get("proyecto") == "gobierno_indicadores"]
pipelines = [(p.pipeline_id, p.name) for p in w.pipelines.list_pipelines() if "Gobierno de indicadores" in (p.name or "")]
display(spark.createDataFrame(jobs, "job_id STRING, nombre STRING, proyecto STRING"))
assert jobs, "No se encontraron jobs con la etiqueta proyecto = gobierno_indicadores"
ids_jobs = ", ".join(f"'{j[0]}'" for j in jobs)
ids_pipelines = ", ".join(f"'{p[0]}'" for p in pipelines) or "''"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Vista de costos de la solución
# MAGIC Combina varios criterios: uso con la etiqueta del proyecto (jobs y cualquier recurso con política de uso serverless),
# MAGIC uso de los pipelines de la solución, del endpoint de AI Search, de la app y (opcional) del SQL warehouse. Si el
# MAGIC warehouse es compartido con otras soluciones, deje `warehouse_id_costos` vacío: su costo no es atribuible solo a esta. El costo estimado usa el precio de lista (`list_prices`); el costo
# MAGIC real depende del contrato.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE VIEW {GOLD}.costos_solucion
  COMMENT 'Consumo (DBU) y costo estimado a precio de lista de la solución de gobierno de indicadores.'
  AS SELECT u.usage_date, u.billing_origin_product AS producto, u.sku_name,
            coalesce(u.usage_metadata.job_id, u.usage_metadata.dlt_pipeline_id, u.usage_metadata.endpoint_name) AS recurso,
            u.custom_tags['ambiente'] AS ambiente,
            sum(u.usage_quantity) AS dbus,
            sum(u.usage_quantity * p.pricing.effective_list.default) AS costo_lista_usd
     FROM system.billing.usage u
     LEFT JOIN system.billing.list_prices p
       ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
      AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time) AND p.currency_code = 'USD'
     WHERE u.usage_date >= date_sub(current_date(), 90)
       AND (u.custom_tags['proyecto'] = 'gobierno_indicadores'
            OR u.usage_metadata.job_id IN ({ids_jobs})
            OR u.usage_metadata.dlt_pipeline_id IN ({ids_pipelines})
            OR u.usage_metadata.endpoint_name = '{endpoint_vs}'
            OR u.usage_metadata.app_name = '{nombre_app}'
            {f"OR u.usage_metadata.warehouse_id = '{warehouse_id}'" if warehouse_id else ""})
     GROUP BY ALL""")
display(spark.sql(f"SELECT producto, round(sum(dbus), 2) AS dbus, round(sum(costo_lista_usd), 2) AS costo_lista_usd FROM {GOLD}.costos_solucion GROUP BY ALL ORDER BY 3 DESC"))
display(spark.sql(f"SELECT usage_date, round(sum(costo_lista_usd), 2) AS costo_lista_usd FROM {GOLD}.costos_solucion GROUP BY ALL ORDER BY 1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Historial de ejecuciones

# COMMAND ----------

display(spark.sql(f"""SELECT job_id, run_id, result_state, period_start_time, period_end_time,
                             timestampdiff(SECOND, period_start_time, period_end_time) AS segundos
                      FROM system.lakeflow.job_run_timeline
                      WHERE job_id IN ({ids_jobs}) AND period_start_time >= current_date() - INTERVAL 30 DAYS
                      ORDER BY period_start_time DESC LIMIT 50"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Linaje: de la ficha al catálogo
# MAGIC Unity Catalog registra automáticamente qué tabla alimenta a cuál.

# COMMAND ----------

display(spark.sql(f"""SELECT DISTINCT source_table_full_name, target_table_full_name, entity_type
                      FROM system.access.table_lineage
                      WHERE event_date >= current_date() - INTERVAL 7 DAYS
                        AND (target_table_full_name LIKE '{catalogo}.{prefijo}_%' OR source_table_full_name LIKE '{catalogo}.{prefijo}_%')
                      ORDER BY target_table_full_name"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Auditoría de la revisión humana

# COMMAND ----------

display(spark.sql(f"""SELECT revisor, decision, count(*) AS decisiones, max(fecha_decision) AS ultima
                      FROM {GOLD}.decisiones_revision GROUP BY ALL ORDER BY decisiones DESC"""))
print("Monitoreo listo. Siguiente paso: 11_validaciones")
