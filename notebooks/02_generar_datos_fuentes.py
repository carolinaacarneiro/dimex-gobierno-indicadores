# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Generar los datos de los sistemas de origen (sintéticos)
# MAGIC
# MAGIC **Objetivo:** contar con las tablas físicas que las fichas citan como *Origen de Datos*, para poder convertir los
# MAGIC indicadores aprobados en **vistas de métricas** y ejecutar sus **reglas de calidad** sobre datos reales.
# MAGIC
# MAGIC | Tabla (esquema `<prefijo>_fuentes`) | Representa | Indicadores que la usan |
# MAGIC |---|---|---|
# MAGIC | `tbl_citas_master` | `DB_CRM_PROD.dbo.Tbl_Citas_Master` (CRM) | IND-001, IND-002, IND-007 |
# MAGIC | `cat_sucursales` | Estructura de sucursales (DataShare) | dimensión Sucursal Destino |
# MAGIC | `tbl_cartera_mensual` | `DB_CORE_CREDITO.dbo.Tbl_Cartera_Mensual` (cierre de cartera) | IND-016, IND-043 |
# MAGIC
# MAGIC Los datos incluyen a propósito los problemas que describen las reglas del Bloque 5 de la ficha IND-001: estatus
# MAGIC nulos (~1.5%), valores fuera del catálogo, duplicados por reintentos de carga y registros de asistencia que llegan
# MAGIC con más de 36 horas de desfase.
# MAGIC
# MAGIC **En producción** estas tablas se reemplazan por la ingesta real (por ejemplo, Lakeflow Connect para SQL Server o
# MAGIC Lakehouse Federation); ver la *Guía de implementación en su entorno*.
# MAGIC
# MAGIC **Parámetro `simular_falla_calidad`:** `si` agrega una carga del día con 6% de estatus nulos para demostrar en vivo
# MAGIC el protocolo de contingencia. **Duración:** 1–2 minutos.

# COMMAND ----------

import re

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.dropdown("simular_falla_calidad", "no", ["si", "no"], "Agregar carga con estatus nulos")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
simular_falla = dbutils.widgets.get("simular_falla_calidad") == "si"
FU = f"`{catalogo}`.`{prefijo}_fuentes`"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catálogo de sucursales

# COMMAND ----------

regiones = {"Centro": ["CDMX Norte", "CDMX Sur", "Estado de México"], "Occidente": ["Jalisco", "Michoacán"],
            "Norte": ["Nuevo León", "Coahuila", "Chihuahua"], "Sureste": ["Veracruz", "Yucatán", "Tabasco"]}
sucursales = []
n = 1
for region, zonas in regiones.items():
    for zona in zonas:
        for k in range(1, 5):
            sucursales.append((f"SUC{n:03d}", f"Sucursal {zona} {k}", zona, region))
            n += 1
spark.createDataFrame(sucursales, "Id_Sucursal STRING, Nom_Sucursal STRING, Cod_Zona STRING, Cod_Region STRING") \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FU}.cat_sucursales")
spark.sql(f"COMMENT ON TABLE {FU}.cat_sucursales IS 'Estructura de sucursales (fuente: DataShare). Una fila por sucursal.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Citas de Telemarketing (`tbl_citas_master`)
# MAGIC Generación determinística con funciones de Spark (la misma ejecución produce los mismos datos). La tasa de asistencia
# MAGIC varía por canal, turno y región, y cae en el último mes, para que los análisis y la alerta del indicador cuenten algo.

# COMMAND ----------

campanas = ["Crédito Nómina Plus", "Renovación Anticipada", "Adelanto de Nómina", "Consolidación de Deuda", "Crédito Pensionados"]
canales = ["Redes Sociales", "SMS", "Llamada Saliente", "Sitio Web", "Referido", "WhatsApp"]
efecto_canal = {"Redes Sociales": -0.07, "SMS": -0.04, "Llamada Saliente": 0.0, "Sitio Web": 0.02, "Referido": 0.06, "WhatsApp": 0.01}
n_suc = len(sucursales)


def u(col_semilla, sal):
    """Número pseudoaleatorio reproducible en [0, 1) a partir de una columna y una 'sal'."""
    return (F.abs(F.xxhash64(col_semilla, F.lit(sal))) % 1_000_000) / 1_000_000


def citas(df_ids, fecha_carga_fija=None, pct_nulos=0.015, pct_nulos_recientes=0.008):
    canal_arr = F.array(*[F.lit(c) for c in canales])
    efecto = F.create_map(*[x for k, v in efecto_canal.items() for x in (F.lit(k), F.lit(v))])
    df = (df_ids
          .withColumn("dia", (u("id", "dia") * 365).cast("int"))
          .withColumn("Fec_Registro", F.expr("timestampadd(SECOND, cast(id % 32400 as int) + 28800, date_sub(current_date(), 366 - dia))"))
          .withColumn("Fec_Cita", F.expr("timestampadd(HOUR, cast(24 + (id % 7) * 24 + (id % 9) as int), Fec_Registro)"))
          .withColumn("Cod_Turno", F.when(F.hour("Fec_Cita") < 14, "Matutino").otherwise("Vespertino"))
          .withColumn("Id_Cita", F.format_string("CT%08d", F.col("id")))
          .withColumn("Id_Cliente", F.format_string("CL%06d", (F.abs(F.xxhash64("id", F.lit("cli"))) % 90000).cast("int")))
          .withColumn("Id_Agente", F.format_string("AG%03d", (F.col("id") % 60 + 1).cast("int")))
          .withColumn("Nom_Campana", F.element_at(F.array(*[F.lit(c) for c in campanas]), (u("id", "camp") * len(campanas)).cast("int") + 1))
          .withColumn("Desc_Canal_Origen", F.element_at(canal_arr, (u("id", "canal") * len(canales)).cast("int") + 1))
          .withColumn("Id_Sucursal_Destino", F.format_string("SUC%03d", (u("id", "suc") * n_suc).cast("int") + 1))
          .withColumn("Cod_Tipificacion", F.when(u("id", "tip") < 0.08, "Reagendado").otherwise("Primera_Cita"))
          .withColumn("p_asistir", F.lit(0.86) + efecto[F.col("Desc_Canal_Origen")]
                      + F.when(F.col("Cod_Turno") == "Vespertino", -0.03).otherwise(0.02)
                      + F.when(F.col("dia") > 335, -0.05).otherwise(0.0)
                      + F.when(F.col("Id_Sucursal_Destino").isin("SUC017", "SUC031", "SUC038"), -0.12).otherwise(0.0))
          .withColumn("r", u("id", "estatus"))
          .withColumn("Cod_Estatus",
                      F.when(F.col("Fec_Cita") > F.current_timestamp(), "Agendada")
                      .when(F.col("r") < 0.11, "Cancelada")
                      .when(u("id", "asis") < F.col("p_asistir"), "Asistió")
                      .otherwise("No_Asistió"))
          .withColumn("Fec_Cancelacion", F.when(F.col("Cod_Estatus") == "Cancelada",
                                                F.expr("timestampadd(HOUR, -cast(CASE WHEN id % 96 = 23 THEN 30 ELSE 1 + id % 96 END as int), Fec_Cita)")))
          .withColumn("Fec_Registro_Asistencia", F.when(F.col("Cod_Estatus") == "Asistió",
                                                        F.expr("timestampadd(HOUR, cast(case when id % 50 = 0 then 40 else id % 20 end as int), Fec_Cita)")))
          # Problemas de calidad descritos en el Bloque 5: estatus nulos y valores fuera del catálogo
          .withColumn("Cod_Estatus", F.when(u("id", "nulo") < F.when(F.col("dia") > 335, F.lit(pct_nulos_recientes)).otherwise(F.lit(pct_nulos)), F.lit(None))
                      .when(u("id", "inval") < 0.003, F.lit("ASISTIO"))
                      .otherwise(F.col("Cod_Estatus")))
          .withColumn("_fecha_carga", F.lit(fecha_carga_fija) if fecha_carga_fija is not None
                      else F.expr("timestampadd(HOUR, 26, cast(to_date(Fec_Registro) as timestamp))")))
    return df.select("Id_Cita", "Id_Cliente", "Id_Agente", "Fec_Registro", "Fec_Cita", "Cod_Estatus", "Cod_Tipificacion",
                     "Fec_Cancelacion", "Fec_Registro_Asistencia", "Nom_Campana", "Desc_Canal_Origen",
                     "Id_Sucursal_Destino", "Cod_Turno", "_fecha_carga")


tabla_citas = f"{FU}.tbl_citas_master"
if not simular_falla:
    base = citas(spark.range(1, 120_001))
    # Duplicados por reintentos de carga del CRM (0.5% de los registros)
    reintentos = base.where(F.abs(F.xxhash64("Id_Cita")) % 200 == 0)
    base.unionByName(reintentos).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla_citas)
else:
    # Carga de hoy con 6% de estatus nulos: dispara el protocolo de contingencia de IND-001
    siguiente = spark.table(tabla_citas).agg(F.max(F.regexp_extract("Id_Cita", r"(\d+)", 1).cast("long"))).first()[0] + 1
    nueva = (citas(spark.range(siguiente, siguiente + 400), pct_nulos=0.06, pct_nulos_recientes=0.06)
             .withColumn("Fec_Registro", F.expr("timestampadd(HOUR, -cast(4 + substr(Id_Cita, -1) as int), current_timestamp())"))
             .withColumn("Fec_Cita", F.expr("timestampadd(DAY, 2, Fec_Registro)"))
             .withColumn("Cod_Estatus", F.when(F.col("Cod_Estatus").isNull(), None).otherwise(F.lit("Agendada")))
             .withColumn("Fec_Cancelacion", F.lit(None).cast("timestamp"))
             .withColumn("Fec_Registro_Asistencia", F.lit(None).cast("timestamp"))
             .withColumn("_fecha_carga", F.current_timestamp()))
    nueva.write.mode("append").saveAsTable(tabla_citas)
    print("Se agregó una carga del día con 6% de estatus nulos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diccionario de datos en Unity Catalog
# MAGIC Los comentarios de tabla y columna son metadatos de Unity Catalog: los ven Catalog Explorer, Genie y los usuarios de
# MAGIC SQL. Documentación: [COMMENT ON](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-comment)

# COMMAND ----------

comentarios_citas = {
    "Id_Cita": "Folio único de la cita generado por el CRM.",
    "Id_Cliente": "Identificador del cliente o Contacto Origen.",
    "Id_Agente": "Agente de Telemarketing que agendó la cita.",
    "Fec_Registro": "Fecha y hora en que se registró la cita en el CRM.",
    "Fec_Cita": "Fecha y hora compromiso de la visita a sucursal.",
    "Cod_Estatus": "Estatus de la cita. Catálogo oficial: Agendada, Asistió, Cancelada, No_Asistió.",
    "Cod_Tipificacion": "Tipificación del registro: Primera_Cita o Reagendado.",
    "Fec_Cancelacion": "Fecha y hora en que el cliente canceló la cita (solo citas canceladas).",
    "Fec_Registro_Asistencia": "Fecha y hora en que la sucursal registró la asistencia en AppCte/ISI.",
    "Nom_Campana": "Campaña comercial (producto) que originó la cita.",
    "Desc_Canal_Origen": "Canal de entrada del prospecto.",
    "Id_Sucursal_Destino": "Sucursal a la que se agendó la visita.",
    "Cod_Turno": "Turno operativo de la cita: Matutino o Vespertino.",
    "_fecha_carga": "Fecha y hora de la carga en la plataforma de datos (cierre nocturno).",
}
if not simular_falla:
    spark.sql(f"COMMENT ON TABLE {tabla_citas} IS 'Citas agendadas por Telemarketing (fuente: CRM, DB_CRM_PROD.dbo.Tbl_Citas_Master).'")
    for col, txt in comentarios_citas.items():
        spark.sql(f"COMMENT ON COLUMN {tabla_citas}.`{col}` IS '{txt}'")
    spark.sql(f"ALTER TABLE {tabla_citas} ALTER COLUMN Id_Cliente SET TAGS ('datos_personales' = 'identificador_cliente')")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cierre mensual de cartera (`tbl_cartera_mensual`)

# COMMAND ----------

if not simular_falla:
    productos = ["Crédito Nómina", "Crédito Pensionados", "Adelanto de Nómina"]
    sectores = ["Gobierno Federal", "Gobierno Estatal", "Salud", "Educación", "Seguridad Pública"]
    regiones_l = list(regiones)
    creditos = (spark.range(1, 20_001).withColumnRenamed("id", "cid")
                .withColumn("Id_Credito", F.format_string("CR%07d", F.col("cid")))
                .withColumn("Id_Cliente", F.format_string("CL%06d", (F.abs(F.xxhash64("cid", F.lit("cc"))) % 90000).cast("int")))
                .withColumn("Cod_Producto", F.element_at(F.array(*[F.lit(p) for p in productos]), (u("cid", "prod") * 3).cast("int") + 1))
                .withColumn("conv", (u("cid", "conv") ** 2 * 50).cast("int") + 1)
                .withColumn("Id_Convenio", F.format_string("CONV%03d", F.col("conv")))
                .withColumn("Desc_Sector", F.element_at(F.array(*[F.lit(s) for s in sectores]), (F.col("conv") % 5) + 1))
                .withColumn("Cod_Region", F.element_at(F.array(*[F.lit(r) for r in regiones_l]), (u("cid", "reg") * 4).cast("int") + 1))
                .withColumn("monto", 15000 + u("cid", "monto") * 135000)
                .withColumn("riesgo", u("cid", "riesgo") + F.when(F.col("Desc_Sector") == "Gobierno Estatal", 0.04).otherwise(0.0)))
    cortes = spark.sql("SELECT last_day(add_months(current_date(), -m)) AS Fec_Corte, m FROM (SELECT explode(sequence(1, 12)) AS m)")
    cartera = (creditos.crossJoin(cortes)
               .withColumn("Sdo_Insoluto", F.round(F.col("monto") * (0.35 + 0.05 * F.col("m")), 2))
               .withColumn("Dias_Atraso",
                           F.when(F.col("riesgo") > 0.962 - (12 - F.col("m")) * 0.0008, (95 + F.col("cid") % 200))
                           .when(F.col("riesgo") > 0.93, (31 + F.col("cid") % 59))
                           .when(F.col("riesgo") > 0.85, (1 + F.col("cid") % 29)).otherwise(F.lit(0)).cast("int"))
               .withColumn("Bucket_Atraso", F.when(F.col("Dias_Atraso") == 0, "Al corriente")
                           .when(F.col("Dias_Atraso") <= 30, "1-30").when(F.col("Dias_Atraso") <= 60, "31-60")
                           .when(F.col("Dias_Atraso") <= 90, "61-90").otherwise("Más de 90"))
               .withColumn("Ind_Castigado", (F.col("Dias_Atraso") > 270))
               .withColumn("Ind_Reestructurado_6m", (u("cid", "reest") < 0.02))
               .select("Id_Credito", "Id_Cliente", "Fec_Corte", "Cod_Producto", "Id_Convenio", "Desc_Sector", "Cod_Region",
                       "Sdo_Insoluto", "Dias_Atraso", "Bucket_Atraso", "Ind_Castigado", "Ind_Reestructurado_6m"))
    tabla_cartera = f"{FU}.tbl_cartera_mensual"
    cartera.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla_cartera)
    spark.sql(f"COMMENT ON TABLE {tabla_cartera} IS 'Cierre mensual de cartera de crédito (fuente: Core de Crédito, DB_CORE_CREDITO.dbo.Tbl_Cartera_Mensual). Una fila por crédito y corte.'")
    for col, txt in {
        "Id_Credito": "Identificador del crédito.", "Id_Cliente": "Identificador del cliente.",
        "Fec_Corte": "Fecha de cierre de mes del corte.", "Cod_Producto": "Producto de crédito.",
        "Id_Convenio": "Convenio de nómina por el que se descuenta el crédito.", "Desc_Sector": "Sector de la dependencia del convenio.",
        "Cod_Region": "Región comercial.", "Sdo_Insoluto": "Saldo insoluto del crédito al corte (MXN).",
        "Dias_Atraso": "Días de atraso del pago más antiguo exigible.", "Bucket_Atraso": "Rango de días de atraso.",
        "Ind_Castigado": "Verdadero si el crédito fue castigado.", "Ind_Reestructurado_6m": "Verdadero si el crédito se reestructuró en los últimos 6 meses.",
    }.items():
        spark.sql(f"COMMENT ON COLUMN {tabla_cartera}.`{col}` IS '{txt}'")
    spark.sql(f"ALTER TABLE {tabla_cartera} ALTER COLUMN Id_Cliente SET TAGS ('datos_personales' = 'identificador_cliente')")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

display(spark.sql(f"""
  SELECT count(*) AS registros, count(DISTINCT Id_Cita) AS citas_unicas,
         round(100 * avg(CASE WHEN Cod_Estatus IS NULL THEN 1 ELSE 0 END), 2) AS pct_estatus_nulo,
         round(100 * count_if(Cod_Estatus = 'Asistió') / count_if(Cod_Estatus IN ('Asistió', 'No_Asistió', 'Cancelada')), 1) AS pct_asistencia_aprox
  FROM {tabla_citas}"""))
assert spark.table(tabla_citas).count() > 100_000
if not simular_falla:
    assert spark.table(f"{FU}.tbl_cartera_mensual").count() == 240_000
print("Fuentes listas. Siguiente paso: pipeline de lectura de fichas.")
