# Databricks notebook source
# MAGIC %md
# MAGIC # 09 · Agentes de Genie: conversar con el catálogo y con los indicadores
# MAGIC
# MAGIC Creamos (o actualizamos) dos **agentes de Genie** (*Genie Agents*), para preguntar en español sin escribir SQL:
# MAGIC
# MAGIC | Agente | Para quién | Responde preguntas como |
# MAGIC |---|---|---|
# MAGIC | **Catálogo de indicadores** | Gobierno de Datos, áreas de negocio | ¿Qué indicadores existen de cobranza? ¿Quién es el Data Owner de X? ¿Qué fichas están duplicadas? ¿Qué significa "Contacto Origen"? |
# MAGIC | **Indicadores de negocio** | Gerencias, comités | ¿Cuál fue la tasa de conversión de cita efectiva por canal el último mes? ¿Cómo evolucionó la morosidad? |
# MAGIC
# MAGIC El primero cubre el **paso 1** del proyecto (entender qué indicadores existen, a partir de las fichas ya leídas y
# MAGIC estructuradas); el segundo consume las **vistas de métricas**, de modo que todas las respuestas usan la definición
# MAGIC certificada. Cada respuesta muestra el SQL que la generó: es auditable.
# MAGIC
# MAGIC **Adaptación a su entorno:** las consultas de ejemplo y preguntas de referencia del agente *Catálogo* usan
# MAGIC indicadores de la demo (IND-001, IMOR, "Contacto Origen"): reemplácenlas en `ejemplos_catalogo` y `bench_catalogo`
# MAGIC con preguntas reales. Con `sobrescribir_genie = no` (producción), el agente se crea una vez y los ajustes que hagan
# MAGIC después en la interfaz de Genie se conservan.
# MAGIC
# MAGIC Buenas prácticas aplicadas: pocas tablas bien comentadas, instrucciones de negocio explícitas, consultas de ejemplo
# MAGIC y **preguntas de referencia** (*benchmarks*) para medir la precisión antes de publicar.
# MAGIC Documentación: [Genie Agents](https://learn.microsoft.com/es-es/azure/databricks/genie-agents/set-up) ·
# MAGIC [Buenas prácticas](https://learn.microsoft.com/es-es/azure/databricks/genie-agents/best-practices)

# COMMAND ----------

import json
import re

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.text("warehouse_id", "", "ID del SQL warehouse")
dbutils.widgets.text("grupo_revisores", "revisores_indicadores", "Grupo con acceso a los agentes")
dbutils.widgets.dropdown("sobrescribir_genie", "no", ["si", "no"], "Reescribir la configuración de agentes existentes")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
grupo = dbutils.widgets.get("grupo_revisores").strip()
sobrescribir = dbutils.widgets.get("sobrescribir_genie") == "si"
assert warehouse_id, "Indique el ID del SQL warehouse (widget warehouse_id)."
G = f"{catalogo}.{prefijo}_gold"
S = f"{catalogo}.{prefijo}_silver"
w = WorkspaceClient()
carpeta = f"/Workspace/Users/{w.current_user.me().user_name}"


def _id(n):
    return f"{n:032x}"


def espacio(tablas, vistas, instrucciones, ejemplos, benchmarks, preguntas):
    return {
        "version": 2,
        "config": {"sample_questions": [{"id": _id(i + 1), "question": [q]} for i, q in enumerate(preguntas)]},
        "data_sources": {"tables": [{"identifier": t} for t in sorted(tablas)],
                         **({"metric_views": [{"identifier": v} for v in sorted(vistas)]} if vistas else {})},
        "instructions": {
            "text_instructions": [{"id": _id(100), "content": [instrucciones]}],
            "example_question_sqls": [{"id": _id(200 + i), "question": [q], "sql": [s]} for i, (q, s) in enumerate(ejemplos)],
        },
        "benchmarks": {"questions": [{"id": _id(300 + i), "question": [q], "answer": [{"format": "SQL", "content": [s]}]}
                                     for i, (q, s) in enumerate(benchmarks)]},
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agente 1 · Catálogo de indicadores

# COMMAND ----------

instr_catalogo = """Contexto: catálogo de indicadores de negocio de una institución financiera mexicana. Cada indicador viene de una ficha de documentación (plantilla de 6 bloques) que las áreas envían; las fichas se leyeron con IA, se revisaron y se publicaron.
Reglas:
1. "Indicador oficial", "vigente" o "certificado" = estado_certificacion = 'CERTIFICADO' en catalogo_indicadores. Si preguntan por un indicador que no está certificado, indique su estado y motivo_estado.
2. Para buscar un indicador por concepto, busque con ILIKE en nombre, definicion_funcional y en los sinonimos (array_join(sinonimos, ' ')); los sinónimos son nombres de fichas duplicadas que se unificaron.
3. Duplicados, conflictos, fichas incompletas y dominios a confirmar están en hallazgos_revision (estado PENDIENTE o RESUELTO).
4. Términos de negocio y homologación de vocabulario: glosario_negocio (tipo_entrada TERMINO, HOMOLOGACION o INDICADOR). Fichas que aún usan términos reemplazados: uso_vocabulario_no_homologado.
5. Responsables: data_owner (visto bueno funcional) y data_steward (implementación). Son puestos, no personas.
6. Calidad: estado_publicacion_indicadores indica si un indicador está en pausa por el protocolo de contingencia; resultados_calidad tiene el detalle por regla.
7. Para el texto completo de una ficha use fichas_vigentes.texto_ficha.
8. Responda en español."""

ejemplos_catalogo = [
    ("¿Qué indicadores certificados existen en el dominio Cartera y Cobranza?",
     f"SELECT id_indicador, nombre, tipo_indicador, data_owner, meta_umbral FROM {G}.catalogo_indicadores\n"
     f"WHERE estado_certificacion = 'CERTIFICADO' AND dominio = 'Cartera y Cobranza' ORDER BY id_indicador"),
    ("¿Qué indicadores miden la morosidad de la cartera?",
     f"SELECT id_indicador, nombre, estado_certificacion, motivo_estado, sinonimos FROM {G}.catalogo_indicadores\n"
     f"WHERE nombre ILIKE '%moros%' OR definicion_funcional ILIKE '%vencid%' OR definicion_funcional ILIKE '%atraso%' OR array_join(sinonimos, ' ') ILIKE '%vencid%'\n"
     f"ORDER BY estado_certificacion, id_indicador"),
    ("¿Quién es el Data Owner de la Tasa de Conversión de Cita Efectiva?",
     f"SELECT id_indicador, nombre, data_owner, data_steward, area_responsable FROM {G}.catalogo_indicadores\n"
     f"WHERE nombre ILIKE '%conversión de cita efectiva%'"),
    ("¿Qué conflictos entre fichas siguen pendientes de revisión?",
     f"SELECT id_ficha, nombre, area, id_ficha_relacionada, nombre_relacionado, area_relacionada, justificacion, prioridad\n"
     f"FROM {G}.hallazgos_revision WHERE tipo_hallazgo = 'CONFLICTO' AND estado = 'PENDIENTE' ORDER BY prioridad"),
    ("¿Qué significa Contacto Origen?",
     f"SELECT termino, tipo_entrada, definicion, sinonimos, fichas_fuente FROM {G}.glosario_negocio WHERE termino ILIKE '%contacto origen%'"),
    ("¿Qué indicadores usan la tabla Tbl_Citas_Master?",
     f"SELECT id_indicador, nombre, estado_certificacion, vista_de_metricas FROM {G}.catalogo_indicadores\n"
     f"WHERE tabla_origen ILIKE 'Tbl_Citas_Master' ORDER BY id_indicador"),
    ("¿Cuántas fichas hay por dominio y estado?",
     f"SELECT dominio, estado_certificacion, count(*) AS fichas FROM {G}.catalogo_indicadores GROUP BY ALL ORDER BY dominio, estado_certificacion"),
    ("¿Qué fichas se devolvieron a las áreas y qué les falta?",
     f"SELECT id_indicador, nombre, area_responsable, motivo_estado FROM {G}.catalogo_indicadores WHERE estado_certificacion = 'DEVUELTO_AL_AREA'"),
    ("¿Qué fichas todavía usan vocabulario no homologado?",
     f"SELECT id_indicador, nombre, area_responsable, termino_encontrado, termino_oficial FROM {G}.uso_vocabulario_no_homologado ORDER BY id_indicador"),
]
bench_catalogo = [
    ("¿Cuántos indicadores están certificados?", f"SELECT count(*) AS certificados FROM {G}.catalogo_indicadores WHERE estado_certificacion = 'CERTIFICADO'"),
    ("¿Qué fichas se unificaron en el Índice de Morosidad (IMOR)?",
     f"SELECT id_indicador, nombre, fichas_unificadas, sinonimos FROM {G}.catalogo_indicadores WHERE nombre ILIKE '%Índice de Morosidad%' AND estado_certificacion = 'CERTIFICADO'"),
    ("¿Cuántos hallazgos de revisión hay pendientes por tipo?",
     f"SELECT tipo_hallazgo, count(*) AS pendientes FROM {G}.hallazgos_revision WHERE estado = 'PENDIENTE' GROUP BY tipo_hallazgo ORDER BY tipo_hallazgo"),
    ("¿Qué indicadores regulatorios están certificados?",
     f"SELECT id_indicador, nombre, dominio FROM {G}.catalogo_indicadores WHERE estado_certificacion = 'CERTIFICADO' AND tipo_indicador = 'Regulatorio' ORDER BY id_indicador"),
]
espacio_catalogo = espacio(
    [f"{G}.{t}" for t in ["catalogo_indicadores", "glosario_negocio", "dominios", "hallazgos_revision", "uso_vocabulario_no_homologado",
                          "estado_publicacion_indicadores", "resultados_calidad"]] + [f"{S}.fichas_vigentes"],
    [], instr_catalogo, ejemplos_catalogo, bench_catalogo,
    ["¿Qué indicadores existen de cobranza?", "¿Qué fichas están duplicadas?", "¿Quién es el Data Owner del IMOR?",
     "¿Qué significa Contacto Origen?"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agente 2 · Indicadores de negocio (sobre las vistas de métricas)
# MAGIC Las consultas de ejemplo se construyen con las medidas y dimensiones de las vistas de métricas vigentes (aprobadas).

# COMMAND ----------

gen = spark.table(f"{G}.vistas_de_metricas_vigentes").collect()  # solo versiones aprobadas y publicadas
vistas = sorted({f"{G}.{r.vista}" for r in gen})
medidas = [r for r in gen if r.tipo == "medida"]
dims = {}
for r in gen:
    if r.tipo == "dimension":
        dims.setdefault(r.vista, []).append(r)
catalogo_ind = {r.id_indicador: r for r in spark.table(f"{G}.catalogo_indicadores").collect()}


def dim_con(vista, patron):
    return next((d.nombre for d in dims.get(vista, []) if re.search(patron, d.expr, re.I) or re.search(patron, d.nombre, re.I)), None)


ejemplos_neg, bench_neg = [], []
for m in medidas:
    c = catalogo_ind.get(m.id_indicador)
    nombre = c.nombre if c else m.nombre
    v = f"{G}.{m.vista}"
    ejemplos_neg.append((f"¿Cómo evolucionó {nombre} por mes?",
                         f"SELECT mes, MEASURE({m.nombre}) AS {m.nombre} FROM {v} GROUP BY ALL ORDER BY mes"))
    bench_neg.append((f"¿Cuál fue {nombre} en el último mes completo?",
                      f"SELECT MEASURE({m.nombre}) AS {m.nombre} FROM {v} WHERE mes = date_trunc('MONTH', add_months(current_date(), -1))"))
    for patron, texto in [("canal", "canal de origen"), ("region|Cod_Region", "región"), ("turno", "turno"), ("convenio", "convenio")]:
        d = dim_con(m.vista, patron)
        if d:
            ejemplos_neg.append((f"¿Cuál fue {nombre} por {texto} en el último mes completo?",
                                 f"SELECT {d}, MEASURE({m.nombre}) AS {m.nombre} FROM {v}\n"
                                 f"WHERE mes = date_trunc('MONTH', add_months(current_date(), -1)) GROUP BY ALL ORDER BY {m.nombre}"))
            break

instr_negocio = f"""Contexto: indicadores de negocio certificados de una institución financiera mexicana, implementados como vistas de métricas de Unity Catalog.
Reglas:
1. Use siempre las vistas de métricas con la función MEASURE() para calcular los indicadores; no recalcule las fórmulas a mano.
2. "Último mes" significa el último mes completo: mes = date_trunc('MONTH', add_months(current_date(), -1)).
3. La meta de cada indicador está en {G}.catalogo_indicadores.meta_umbral; compárela cuando pregunten si se cumple.
4. Si un indicador está en pausa por calidad ({G}.estado_publicacion_indicadores.publicar = false), adviértalo en la respuesta.
5. Los porcentajes ya están multiplicados por 100.
6. Responda en español."""
espacio_negocio = espacio([f"{G}.catalogo_indicadores", f"{G}.estado_publicacion_indicadores"], vistas, instr_negocio,
                          ejemplos_neg, bench_neg,
                          [q for q, _ in ejemplos_neg[:4]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crear o actualizar los agentes
# MAGIC La API de Genie conserva el nombre técnico *spaces* en sus rutas.

# COMMAND ----------

existentes, token = [], None
while True:
    pagina = w.api_client.do("GET", "/api/2.0/genie/spaces", query={"page_token": token} if token else None)
    existentes += pagina.get("spaces", [])
    token = pagina.get("next_page_token")
    if not token:
        break

# Cada agente se identifica por el space_id guardado en gold.agentes_genie (así, dos entornos en el mismo workspace no
# se pisan). Con sobrescribir_genie = no, un agente existente no se modifica: se conservan las instrucciones, ejemplos y
# preguntas de referencia que el equipo ajuste en la interfaz de Genie.
guardados = {}
if spark.catalog.tableExists(f"{G}.agentes_genie"):
    guardados = {r.agente: r.space_id for r in spark.table(f"{G}.agentes_genie").collect()}
ids_existentes = {s.get("space_id") for s in existentes}
agentes = {}
avisos = []
for titulo, desc, esp in [
    ("Catálogo de indicadores", "Preguntas en español sobre los indicadores documentados: qué existe, estado, responsables, duplicados, conflictos y glosario.", espacio_catalogo),
    ("Indicadores de negocio", "Preguntas en español sobre los indicadores certificados, calculados con las vistas de métricas de Unity Catalog.", espacio_negocio),
]:
    sid = guardados.get(titulo) if guardados.get(titulo) in ids_existentes else None
    cuerpo = {"warehouse_id": warehouse_id, "serialized_space": json.dumps(esp, ensure_ascii=False), "title": titulo, "description": desc}
    if sid and sobrescribir:
        w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{sid}", body=cuerpo)
    elif not sid:
        sid = w.api_client.do("POST", "/api/2.0/genie/spaces", body={**cuerpo, "parent_path": carpeta}).get("space_id")
    else:
        print(f"{titulo}: ya existe; se conserva su configuración (sobrescribir_genie = no).")
    agentes[titulo] = sid
    print(f"{titulo}: {w.config.host}/genie/rooms/{agentes[titulo]}")
    try:  # acceso para el grupo de consumidores
        w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{agentes[titulo]}",
                        body={"access_control_list": [{"group_name": grupo, "permission_level": "CAN_RUN"}]})
    except Exception as e:
        avisos.append(("09", "permiso", f"Genie {titulo} CAN_RUN {grupo}", str(e)[:300]))
        print(f"Aviso: no se pudo dar acceso al grupo {grupo}: {str(e)[:150]}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {G}.avisos_publicacion (notebook STRING, tipo STRING, objeto STRING, detalle STRING, fecha TIMESTAMP)
              COMMENT 'Acciones de gobierno (etiquetas, certificación, permisos) que no se pudieron aplicar; revísenla tras cada publicación.'""")
if avisos:
    spark.createDataFrame(avisos, "notebook STRING, tipo STRING, objeto STRING, detalle STRING") \
        .selectExpr("*", "current_timestamp() AS fecha").write.mode("append").saveAsTable(f"{G}.avisos_publicacion")

spark.createDataFrame([(t, i) for t, i in agentes.items()], "agente STRING, space_id STRING") \
    .write.mode("overwrite").saveAsTable(f"{G}.agentes_genie")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación de las consultas de ejemplo y de referencia
# MAGIC Cada consulta SQL de los agentes se ejecuta aquí: si alguna falla, el notebook termina con error.

# COMMAND ----------

errores = []
for q, s in ejemplos_catalogo + bench_catalogo + ejemplos_neg + bench_neg:
    try:
        spark.sql(s).collect()
    except Exception as e:
        errores.append(f"{q}: {str(e)[:200]}")
assert not errores, "Consultas con error:\n" + "\n".join(errores)
print(f"{len(ejemplos_catalogo + bench_catalogo + ejemplos_neg + bench_neg)} consultas verificadas. Siguiente paso: 10_costos_y_monitoreo")
