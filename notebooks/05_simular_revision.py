# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Simular la revisión humana (solo demo)
# MAGIC
# MAGIC **Objetivo:** que la demo pueda ejecutarse de punta a punta sin intervención, registrando decisiones como lo haría
# MAGIC un revisor en la **app de revisión**. En producción **este notebook no se usa**: las decisiones las toman los Data
# MAGIC Owners y Data Stewards en la app, y quedan con su usuario.
# MAGIC
# MAGIC Criterio del revisor simulado (usa la respuesta esperada de la muestra sintética):
# MAGIC
# MAGIC | Hallazgo | Decisión simulada |
# MAGIC |---|---|
# MAGIC | Duplicado o conflicto plantado | Unificar / definir oficial la ficha original del concepto |
# MAGIC | Par que la IA marcó pero no es duplicado ni conflicto | Mantener ambas |
# MAGIC | Ficha incompleta | Devolver al área |
# MAGIC | Dominio a confirmar | Mantener el dominio declarado por el área |
# MAGIC
# MAGIC Se dejan **pendientes a propósito** dos conflictos (Tasa de Aprobación y Tasa de Retención) y el dominio del
# MAGIC Índice de Morosidad (¿Cartera y Cobranza o Riesgo de Crédito?) para resolverlos en vivo en la app durante la presentación.

# COMMAND ----------

import re

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
GOLD = f"`{catalogo}`.`{prefijo}_gold`"
RESPUESTA_ESPERADA = f"`{catalogo}`.`{prefijo}_landing`.respuesta_esperada_fichas_sinteticas"

PENDIENTES_PARA_LA_DEMO = [("IND-011", "IND-203"), ("IND-032", "IND-206")]
DOMINIO_PENDIENTE_PARA_LA_DEMO = ["IND-016"]
pendientes_sql = " OR ".join(
    f"(least(h.id_ficha, h.id_ficha_relacionada) = '{a}' AND greatest(h.id_ficha, h.id_ficha_relacionada) = '{b}')"
    for a, b in PENDIENTES_PARA_LA_DEMO)

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TEMP VIEW decisiones_simuladas AS
  WITH g AS (SELECT least(id_ficha, id_ficha_relacionada) AS a, greatest(id_ficha, id_ficha_relacionada) AS b
             FROM {RESPUESTA_ESPERADA} WHERE id_ficha_relacionada IS NOT NULL),
  h AS (SELECT * FROM {GOLD}.hallazgos_revision h WHERE estado = 'PENDIENTE' AND NOT ({pendientes_sql})
             AND NOT (h.tipo_hallazgo = 'DOMINIO_A_CONFIRMAR' AND h.id_ficha IN ({', '.join(repr(x) for x in DOMINIO_PENDIENTE_PARA_LA_DEMO)})))
  SELECT uuid() AS id_decision, h.id_hallazgo,
         CASE WHEN h.tipo_hallazgo IN ('DUPLICADO', 'CONFLICTO') AND g.a IS NOT NULL
                THEN CASE WHEN h.tipo_hallazgo = 'DUPLICADO' THEN 'UNIFICAR' ELSE 'DEFINIR_OFICIAL' END
              WHEN h.tipo_hallazgo IN ('DUPLICADO', 'CONFLICTO') THEN 'MANTENER_AMBAS'
              WHEN h.tipo_hallazgo = 'INCOMPLETA' THEN 'DEVOLVER_AL_AREA'
              ELSE 'MANTENER_DOMINIO_DECLARADO' END AS decision,
         -- La ficha original del concepto (ID menor a IND-100 en la muestra) queda como oficial
         CASE WHEN g.a IS NOT NULL THEN least(h.id_ficha, h.id_ficha_relacionada) END AS id_ficha_oficial,
         CASE WHEN h.tipo_hallazgo = 'INCOMPLETA' THEN concat('Completar: ', array_join(h.diferencias, '; '))
              ELSE 'Decisión registrada por la simulación de la demo.' END AS comentario,
         'revisor.simulado@demo' AS revisor, current_timestamp() AS fecha_decision
  FROM h LEFT JOIN g ON g.a = least(h.id_ficha, h.id_ficha_relacionada) AND g.b = greatest(h.id_ficha, h.id_ficha_relacionada)""")

spark.sql(f"INSERT INTO {GOLD}.decisiones_revision SELECT * FROM decisiones_simuladas")
spark.sql(f"""UPDATE {GOLD}.hallazgos_revision SET estado = 'RESUELTO'
              WHERE id_hallazgo IN (SELECT id_hallazgo FROM {GOLD}.decisiones_revision)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

display(spark.sql(f"""SELECT h.tipo_hallazgo, d.decision, count(*) AS cantidad
                      FROM {GOLD}.decisiones_revision d JOIN {GOLD}.hallazgos_revision h USING (id_hallazgo) GROUP BY ALL ORDER BY ALL"""))
pend = spark.sql(f"SELECT count(*) FROM {GOLD}.hallazgos_revision WHERE estado = 'PENDIENTE'").first()[0]
print(f"Hallazgos pendientes para la demostración en vivo: {pend}. Siguiente paso: 06_publicar_catalogo_y_glosario")
