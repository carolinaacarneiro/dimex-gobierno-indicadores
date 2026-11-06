# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · De la ficha aprobada a la vista de métricas
# MAGIC
# MAGIC **Objetivo:** que cada indicador **certificado** quede implementado una sola vez, en Unity Catalog, como una
# MAGIC **vista de métricas** (*metric view*): la fórmula del Bloque 3, los criterios de exclusión y de validez como filtros,
# MAGIC y las dimensiones autorizadas del Bloque 6. Dashboards, Genie, alertas y SQL usan esa misma definición.
# MAGIC
# MAGIC **Cómo lo hacemos:**
# MAGIC 1. `config/mapeo_fuentes.json` indica dónde está en Unity Catalog cada tabla que citan las fichas.
# MAGIC 2. Para cada tabla, `ai_query` **propone** las medidas y dimensiones a partir de las fichas certificadas y del
# MAGIC    diccionario de la tabla (columnas y comentarios), con una respuesta estructurada.
# MAGIC 3. **Cada expresión se ejecuta** contra la tabla antes de usarla; las que fallan se descartan y quedan registradas.
# MAGIC 4. Se crea la vista con `CREATE VIEW ... WITH METRICS LANGUAGE YAML` (versión 1.1: nombres visibles, formato y sinónimos).
# MAGIC 5. Cada versión queda en `gold.definicion_vistas_de_metricas` (historial con estado y aprobador); el detalle de la última generación, en `gold.vistas_de_metricas_generadas`.
# MAGIC
# MAGIC La IA acelera la traducción de la ficha a código; **el Data Steward aprueba** el resultado. Por eso el notebook
# MAGIC tiene tres modos (parámetro `modo`):
# MAGIC
# MAGIC | Modo | Qué hace | Dónde se usa |
# MAGIC |---|---|---|
# MAGIC | `proponer` | Genera la propuesta, crea `<vista>_propuesta` **sin certificar** y la guarda como `PROPUESTA` en `gold.definicion_vistas_de_metricas` (historial) | Producción (por defecto) |
# MAGIC | `publicar_aprobadas` | No llama a la IA: publica, con el YAML exacto aprobado, cada vista cuya última versión `APROBADA` cambió, y la certifica | Producción, después de la aprobación |
# MAGIC | `generar` | Propone y publica en un solo paso, con aprobación automática | Solo la demo |
# MAGIC
# MAGIC Para aprobar, el Steward revisa el YAML de la propuesta (y la vista `<vista>_propuesta`) y ejecuta:
# MAGIC `UPDATE <catalogo>.<prefijo>_gold.definicion_vistas_de_metricas SET estado = 'APROBADA', aprobada_por = current_user(),
# MAGIC fecha_aprobacion = current_timestamp() WHERE vista = '<vista>' AND version = <version>`. Recomendamos además versionar
# MAGIC el YAML aprobado en el repositorio. El notebook `11_validaciones` compara IND-001 contra un cálculo de referencia
# MAGIC escrito a mano.
# MAGIC
# MAGIC Documentación: [Vistas de métricas](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/) ·
# MAGIC [Referencia YAML](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/yaml-reference)

# COMMAND ----------

import json
import os
import re
import time
import unicodedata

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.text("modelo_llm", "databricks-claude-sonnet-4-6", "Modelo")
dbutils.widgets.text("tag_dominio", "dominio_datos", "Clave de la etiqueta de dominio")
dbutils.widgets.dropdown("modo", "proponer", ["proponer", "publicar_aprobadas", "generar"], "Modo")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
modelo_llm = dbutils.widgets.get("modelo_llm").strip()
tag_dominio = dbutils.widgets.get("tag_dominio").strip()
modo = dbutils.widgets.get("modo")
GOLD = f"`{catalogo}`.`{prefijo}_gold`"
FU = f"`{catalogo}`.`{prefijo}_fuentes`"

raiz = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
with open(f"/Workspace{raiz}/config/mapeo_fuentes.json", encoding="utf-8") as f:
    MAPEO = {k: v for k, v in json.load(f).items() if not k.startswith("_")}


def nombre_tabla(t):
    return t if "." in t else f"{FU}.`{t}`"


def slug(texto):
    t = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "_", t).strip("_")


def sql_str(t):
    return (t or "").replace("\\", "\\\\").replace("'", "\\'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Indicadores certificados por tabla de origen

# COMMAND ----------

certificados = spark.sql(f"""SELECT * FROM {GOLD}.catalogo_indicadores WHERE estado_certificacion = 'CERTIFICADO'""").collect()
por_tabla = {}
for c in certificados:
    clave = next((k for k in MAPEO if k.lower() == (c.tabla_origen_documentada or "").lower()), None)
    if clave:
        por_tabla.setdefault(clave, []).append(c)
for k, v in por_tabla.items():
    print(k, "->", [c.id_indicador for c in v])
if modo != "publicar_aprobadas":
    assert por_tabla, "Ningún indicador certificado tiene su tabla de origen mapeada en config/mapeo_fuentes.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Propuesta de medidas y dimensiones con IA

# COMMAND ----------

REGLAS = """Eres un ingeniero de analítica que implementa indicadores de negocio como vistas de métricas de Databricks (Unity Catalog).
Recibes el diccionario de una tabla (y de tablas unidas con su alias) y las fichas de los indicadores certificados que la usan.
Devuelve medidas y dimensiones siguiendo estas reglas:
1. Una medida por indicador. expr es UNA expresión de agregación de Databricks SQL sobre columnas de la tabla (sin SELECT ni FROM).
   Usa los nombres de columna exactos del diccionario. Para columnas de tablas unidas usa alias.columna.
2. Respeta la fórmula de la ficha. Si es un porcentaje (x 100) multiplica por 100. Para cocientes usa try_divide(numerador, denominador).
3. Aplica en el numerador Y en el denominador los criterios de exclusión de negocio y las reglas de validez del Bloque 5
   (por ejemplo, registros con valores fuera del catálogo oficial se descartan; registros reagendados se excluyen).
   Excluye EXACTAMENTE lo que dice la ficha y nada más: si la ficha excluye solo un subconjunto de un estatus (por
   ejemplo, "cancelaciones con más de 24 horas de anticipación"), los demás registros de ese estatus (las cancelaciones
   con 24 horas o menos) SÍ permanecen en el denominador. El denominador ("total de registros") incluye todos los
   estatus válidos del catálogo oficial salvo las exclusiones explícitas. Usa la misma condición de exclusión en
   numerador y denominador.
   Expresa los filtros con COUNT(DISTINCT CASE WHEN condición THEN columna END) o SUM(CASE WHEN condición THEN valor END).
4. Si la regla de unicidad indica una llave, cuenta registros únicos con COUNT(DISTINCT llave).
5. Una diferencia de horas entre dos timestamps se calcula con timestampdiff(HOUR, inicio, fin).
6. nombre en snake_case sin acentos (por ejemplo tasa_conversion_cita_efectiva); nombre_visible legible en español;
   formato: 'porcentaje', 'numero' o 'moneda'.
7. Dimensiones: solo las autorizadas en el Bloque 6 de las fichas, sin repetir, más 'mes' (DATE_TRUNC('MONTH', columna_fecha)) y 'fecha' (to_date(columna_fecha)).
   Si una dimensión autorizada tiene un catálogo unido con descripción (por ejemplo nombre, zona o región de sucursal), agrega esas columnas como dimensiones.
8. Comentarios en español, breves, basados en la definición funcional."""

FORMATO = ("STRUCT<propuesta:STRUCT<medidas:ARRAY<STRUCT<id_indicador:STRING, nombre:STRING, nombre_visible:STRING, expr:STRING, comentario:STRING, formato:STRING>>, "
           "dimensiones:ARRAY<STRUCT<nombre:STRING, nombre_visible:STRING, expr:STRING, comentario:STRING>>>>")


def diccionario(tabla, alias):
    filas = spark.sql(f"DESCRIBE TABLE {nombre_tabla(tabla)}").collect()
    return "\n".join(f"- {alias}.{r.col_name} ({r.data_type}): {r.comment or ''}" for r in filas
                     if r.col_name and not r.col_name.startswith("#"))


def ficha(c):
    dims = "; ".join(f"{d.nombre} -> {d.campo_tecnico}" for d in (c.dimensiones or []))
    return (f"ID: {c.id_indicador}\nNombre: {c.nombre}\nDefinición: {c.definicion_funcional}\nVariable A: {c.variable_a}\n"
            f"Variable B: {c.variable_b}\nFórmula: {c.formula}\nUnidad: {c.unidad_medida}\nCriterios de exclusión: {c.criterios_exclusion}\n"
            f"Completitud: {c.criterio_completitud}\nValidez: {c.criterio_validez}\nUnicidad: {c.regla_unicidad}\nDimensiones autorizadas: {dims}")


propuestas = {}
for clave, fichas in (por_tabla.items() if modo != "publicar_aprobadas" else []):
    m = MAPEO[clave]
    contexto = f"Tabla principal (alias source): {clave}\n{diccionario(m['tabla'], 'source')}\nColumna de fecha principal: source.{m['columna_fecha']}\n"
    for j in m.get("joins", []):
        contexto += f"\nTabla unida (alias {j['nombre']}, unión {j['on']}):\n{diccionario(j['tabla'], j['nombre'])}\n"
    instruccion = REGLAS + "\n\n" + contexto + "\n\nFICHAS:\n\n" + "\n\n".join(ficha(c) for c in fichas)
    r = spark.sql(f"SELECT ai_query('{modelo_llm}', '{sql_str(instruccion)}', responseFormat => '{FORMATO}') AS r").first().r
    r = json.loads(r)
    propuestas[clave] = r.get("propuesta", r)  # la respuesta trae directamente el objeto interno
    print(clave, len(propuestas[clave]["medidas"]), "medidas,", len(propuestas[clave]["dimensiones"]), "dimensiones")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Validar cada expresión ejecutándola y registrar la versión (historial)

# COMMAND ----------

def q(s):
    return json.dumps(s, ensure_ascii=False)  # una cadena JSON es una cadena YAML válida


# Migración: la versión anterior de la tabla no tenía historial (columnas version, estado, publicada...)
if spark.catalog.tableExists(f"{catalogo}.{prefijo}_gold.definicion_vistas_de_metricas") and \
        "publicada" not in spark.table(f"{GOLD}.definicion_vistas_de_metricas").columns:
    spark.sql(f"DROP TABLE {GOLD}.definicion_vistas_de_metricas")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.definicion_vistas_de_metricas (
  vista STRING, version BIGINT COMMENT 'Versión (marca de tiempo de la generación).', yaml STRING COMMENT 'Definición YAML completa.',
  medidas ARRAY<STRUCT<id_indicador: STRING, nombre: STRING, expr: STRING>>, dimensiones ARRAY<STRUCT<nombre: STRING, expr: STRING>>,
  estado STRING COMMENT 'PROPUESTA, APROBADA o RECHAZADA.', fecha TIMESTAMP, aprobada_por STRING, fecha_aprobacion TIMESTAMP,
  publicada BOOLEAN COMMENT 'Verdadero cuando la versión aprobada ya se publicó como vista oficial.')
COMMENT 'Historial de definiciones de vistas de métricas: propuestas de la IA y versiones aprobadas por el Data Steward.'""")

# Las expresiones propuestas por la IA se rechazan si contienen subconsultas, sentencias o comentarios, antes de ejecutarlas
PROHIBIDAS = re.compile(r"(;|--|/\*|\b(select|from|join|insert|update|delete|merge|drop|alter|create|grant|revoke|call|exec|union|with)\b)", re.I)


def expresion_segura(expr):
    return bool(expr) and not PROHIBIDAS.search(re.sub(r"'[^']*'", "", expr))


def nombre_valido(nombre):
    return bool(re.fullmatch(r"[a-z][a-z0-9_]{0,62}", nombre or ""))


registro = []
for clave, prop in propuestas.items():
    m = MAPEO[clave]
    fuente = nombre_tabla(m["tabla"])
    desde = f"{fuente} AS source " + " ".join(f"LEFT JOIN {nombre_tabla(j['tabla'])} AS {j['nombre']} ON {j['on']}" for j in m.get("joins", []))
    fichas = {c.id_indicador: c for c in por_tabla[clave]}

    medidas, dimensiones = [], []
    for d in prop["dimensiones"]:
        if not (expresion_segura(d.get("expr")) and nombre_valido(d.get("nombre"))):
            registro.append((m["vista"], None, d["nombre"], "dimension", d.get("expr"), "DESCARTADA", "Expresión o nombre rechazados por la validación de seguridad"))
            continue
        try:
            spark.sql(f"SELECT {d['expr']} FROM {desde} LIMIT 1").collect()
            dimensiones.append(d)
        except Exception as e:
            registro.append((m["vista"], None, d["nombre"], "dimension", d["expr"], "DESCARTADA", str(e)[:300]))
    for med in prop["medidas"]:
        if not (expresion_segura(med.get("expr")) and nombre_valido(med.get("nombre"))):
            registro.append((m["vista"], med.get("id_indicador"), med["nombre"], "medida", med.get("expr"), "DESCARTADA", "Expresión o nombre rechazados por la validación de seguridad"))
            continue
        try:
            spark.sql(f"SELECT {med['expr']} FROM {desde}").collect()
            medidas.append(med)
        except Exception as e:
            registro.append((m["vista"], med.get("id_indicador"), med["nombre"], "medida", med["expr"], "DESCARTADA", str(e)[:300]))

    yaml = ["version: 1.1", f"comment: {q('Indicadores certificados sobre ' + clave + '. Generada a partir de las fichas aprobadas.')}",
            f"source: {fuente.replace('`', '')}"]
    if m.get("joins"):
        yaml.append("joins:")
        for j in m["joins"]:
            yaml += [f"  - name: {j['nombre']}", f"    source: {nombre_tabla(j['tabla']).replace('`', '')}", f"    'on': {q(j['on'])}"]
    yaml.append("dimensions:")
    for d in dimensiones:
        yaml += [f"  - name: {d['nombre']}", f"    expr: {q(d['expr'])}", f"    display_name: {q(d['nombre_visible'])}",
                 f"    comment: {q(d['comentario'])}"]
    yaml.append("measures:")
    for med in medidas:
        c = fichas.get(med.get("id_indicador"))
        sinonimos = [s for s in ([c.nombre] + list(c.sinonimos or [])) if s][:10] if c else []
        comentario = f"{med['id_indicador']} · {med['comentario']}" + (f" Meta: {c.meta_umbral}" if c and c.meta_umbral else "")
        yaml += [f"  - name: {med['nombre']}", f"    expr: {q(med['expr'])}", f"    display_name: {q(med['nombre_visible'])}",
                 f"    comment: {q(comentario)}"]
        if sinonimos:
            yaml.append(f"    synonyms: {q(sinonimos)}")
        if med.get("formato") == "porcentaje":
            yaml += ["    format:", "      type: number", "      decimal_places:", "        type: exact", "        places: 2"]
    texto_yaml = "\n".join(yaml)

    # La propuesta se crea como vista separada (sin certificar) para que el Steward la consulte antes de aprobarla
    destino = m["vista"] if modo == "generar" else f"{m['vista']}_propuesta"
    spark.sql(f"CREATE OR REPLACE VIEW {GOLD}.{destino} WITH METRICS LANGUAGE YAML AS $$\n{texto_yaml}\n$$")
    estado = "APROBADA" if modo == "generar" else "PROPUESTA"
    for med in medidas:
        registro.append((m["vista"], med.get("id_indicador"), med["nombre"], "medida", med["expr"], "CREADA" if modo == "generar" else "PROPUESTA", None))
    for d in dimensiones:
        registro.append((m["vista"], None, d["nombre"], "dimension", d["expr"], "CREADA" if modo == "generar" else "PROPUESTA", None))
    (spark.createDataFrame([(m["vista"], int(time.time() * 1000), texto_yaml,
                             [(x.get("id_indicador"), x["nombre"], x["expr"]) for x in medidas], [(x["nombre"], x["expr"]) for x in dimensiones],
                             estado, None, "aprobación automática (demo)" if modo == "generar" else None, None, False)],
                           "vista STRING, version BIGINT, yaml STRING, medidas ARRAY<STRUCT<id_indicador: STRING, nombre: STRING, expr: STRING>>, "
                           "dimensiones ARRAY<STRUCT<nombre: STRING, expr: STRING>>, estado STRING, fecha TIMESTAMP, aprobada_por STRING, "
                           "fecha_aprobacion TIMESTAMP, publicada BOOLEAN")
     .withColumn("fecha", F.current_timestamp())
     .withColumn("fecha_aprobacion", F.when(F.lit(modo == "generar"), F.current_timestamp()))
     .write.mode("append").saveAsTable(f"{GOLD}.definicion_vistas_de_metricas"))
    print(f"{destino}: {len(medidas)} medidas, {len(dimensiones)} dimensiones ({estado})")
    print(texto_yaml)

if registro:
    (spark.createDataFrame(registro, "vista STRING, id_indicador STRING, nombre STRING, tipo STRING, expr STRING, estado STRING, error STRING")
     .write.mode("overwrite").saveAsTable(f"{GOLD}.vistas_de_metricas_generadas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Publicar las versiones aprobadas
# MAGIC Se publica, con el YAML exacto aprobado, la última versión `APROBADA` de cada vista que todavía no se publicó; se
# MAGIC certifica y se enlaza en el catálogo. Las vistas oficiales cambian solo cuando hay una nueva versión aprobada.

# COMMAND ----------

avisos = []  # se guardan en gold.avisos_publicacion


def certificar(vista):
    try:
        spark.sql(f"SET TAG ON TABLE {vista} `system.certification_status` = `certified`")
    except Exception as e:  # requiere ASSIGN sobre la etiqueta de sistema
        avisos.append(("07", "certificacion", vista.replace("`", ""), str(e)[:300]))
        print(f"Aviso: no se pudo certificar {vista}: {str(e)[:150]}")


spark.sql(f"""CREATE OR REPLACE VIEW {GOLD}.vistas_de_metricas_vigentes
  COMMENT 'Medidas y dimensiones de la última versión aprobada de cada vista de métricas.'
  AS WITH ultima AS (SELECT * FROM {GOLD}.definicion_vistas_de_metricas WHERE estado = 'APROBADA'
                     QUALIFY row_number() OVER (PARTITION BY vista ORDER BY version DESC) = 1)
  SELECT vista, version, 'medida' AS tipo, m.id_indicador, m.nombre, m.expr FROM ultima LATERAL VIEW explode(medidas) e AS m
  UNION ALL
  SELECT vista, version, 'dimension', NULL, d.nombre, d.expr FROM ultima LATERAL VIEW explode(dimensiones) e AS d""")

pendientes = spark.sql(f"""SELECT * FROM {GOLD}.definicion_vistas_de_metricas WHERE estado = 'APROBADA'
                           QUALIFY row_number() OVER (PARTITION BY vista ORDER BY version DESC) = 1""").where("NOT publicada").collect()
for d in pendientes:
    vista = f"{GOLD}.{d.vista}"
    spark.sql(f"CREATE OR REPLACE VIEW {vista} WITH METRICS LANGUAGE YAML AS $$\n{d.yaml}\n$$")
    ids = [x.id_indicador for x in d.medidas if x.id_indicador]
    doms = [r.dominio for r in spark.table(f"{GOLD}.catalogo_indicadores").where(F.col("id_indicador").isin(ids)).collect() if r.dominio]
    try:
        spark.sql(f"ALTER VIEW {vista} SET TAGS ('{tag_dominio}' = '{slug(max(set(doms), key=doms.count)) if doms else 'gobierno_de_datos'}', 'solucion' = 'gobierno_indicadores')")
    except Exception as e:
        avisos.append(("07", "etiqueta", vista.replace("`", ""), str(e)[:300]))
        print(f"Aviso: no se pudo etiquetar {vista}: {str(e)[:150]}")
    certificar(vista)
    if ids:
        spark.sql(f"UPDATE {GOLD}.catalogo_indicadores SET vista_de_metricas = '{catalogo}.{prefijo}_gold.{d.vista}' "
                  f"WHERE id_indicador IN ({', '.join(repr(i) for i in ids)})")
    spark.sql(f"UPDATE {GOLD}.definicion_vistas_de_metricas SET publicada = true WHERE vista = '{d.vista}' AND version = {d.version}")
    print(f"Publicada {vista} (versión {d.version})")

# El catálogo se regenera en cada publicación: se vuelve a enlazar cada indicador con su vista vigente
for r in spark.table(f"{GOLD}.vistas_de_metricas_vigentes").where("tipo = 'medida' AND id_indicador IS NOT NULL").collect():
    spark.sql(f"UPDATE {GOLD}.catalogo_indicadores SET vista_de_metricas = '{catalogo}.{prefijo}_gold.{r.vista}' WHERE id_indicador = '{r.id_indicador}'")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {GOLD}.avisos_publicacion (notebook STRING, tipo STRING, objeto STRING, detalle STRING, fecha TIMESTAMP)
              COMMENT 'Acciones de gobierno (etiquetas, certificación, permisos) que no se pudieron aplicar; revísenla tras cada publicación.'""")
if avisos:
    spark.createDataFrame(avisos, "notebook STRING, tipo STRING, objeto STRING, detalle STRING") \
        .selectExpr("*", "current_timestamp() AS fecha").write.mode("append").saveAsTable(f"{GOLD}.avisos_publicacion")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Serie mensual estable para dashboards y alertas
# MAGIC Una vista común con columnas fijas (`id_indicador`, `mes`, `valor`) sobre las vistas de métricas vigentes: el
# MAGIC dashboard y la alerta del indicador no dependen de los nombres de las medidas.

# COMMAND ----------

vigentes = spark.table(f"{GOLD}.vistas_de_metricas_vigentes").where("tipo = 'medida' AND id_indicador IS NOT NULL").collect()
partes = [f"SELECT '{r.id_indicador}' AS id_indicador, '{r.nombre}' AS medida, mes, CAST(MEASURE({r.nombre}) AS DOUBLE) AS valor "
          f"FROM {GOLD}.{r.vista} GROUP BY mes" for r in vigentes]
if partes:
    spark.sql(f"""CREATE OR REPLACE VIEW {GOLD}.serie_mensual_indicadores
      COMMENT 'Valor mensual de cada indicador certificado, calculado con su vista de métricas vigente.'
      AS SELECT s.*, c.nombre, c.meta_umbral, c.unidad_medida FROM ({' UNION ALL '.join(partes)}) s
      JOIN {GOLD}.catalogo_indicadores c USING (id_indicador)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación: consultar las vistas vigentes con `MEASURE()`

# COMMAND ----------

display(spark.table(f"{GOLD}.definicion_vistas_de_metricas").select("vista", "version", "estado", "aprobada_por", "publicada", "fecha").orderBy(F.desc("version")))
for v in sorted({r.vista for r in vigentes}):
    medidas = [r.nombre for r in vigentes if r.vista == v]
    display(spark.sql(f"SELECT mes, {', '.join(f'MEASURE({x}) AS {x}' for x in medidas)} FROM {GOLD}.{v} GROUP BY ALL ORDER BY mes DESC LIMIT 6"))
if modo == "proponer":
    print("Propuestas registradas. Siguiente paso: aprobación del Data Steward y ejecución con modo = publicar_aprobadas.")
else:
    assert vigentes, "No hay vistas de métricas aprobadas y publicadas"
    print("Vistas de métricas vigentes publicadas. Siguiente paso: 08_reglas_de_calidad")
