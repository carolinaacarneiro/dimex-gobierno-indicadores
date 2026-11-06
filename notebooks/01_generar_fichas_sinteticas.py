# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generar fichas sintéticas de indicadores (PDF)
# MAGIC
# MAGIC **Objetivo:** simular lo que hoy envían las áreas de negocio: fichas PDF con la plantilla de documentación de
# MAGIC indicadores (versión 1.2, 6 bloques), y dejarlas en el volumen `fichas_indicadores/recibidas/`.
# MAGIC
# MAGIC **¿Qué contiene la muestra?** 62 fichas de 9 dominios (Oportunidades, Originación, Cartera y Cobranza, Riesgo de
# MAGIC Crédito, Finanzas, Clientes y Experiencia, Red de Sucursales, Cumplimiento Regulatorio, Capital Humano). Para que la
# MAGIC demo tenga casos reales que resolver, incluye a propósito:
# MAGIC
# MAGIC | Caso | Cantidad | Ejemplo |
# MAGIC |---|---|---|
# MAGIC | Duplicados (mismo concepto, otra área u otro nombre) | 8 | *Tasa de Asistencia a Citas en Sucursal* (Red Comercial) ≈ IND-001 |
# MAGIC | Conflictos (mismo concepto con otra regla, meta o fuente) | 6 | *Índice de Morosidad (IMOR)* con 60 días (Cobranza) vs. 90 días (Riesgos) |
# MAGIC | Fichas incompletas | 4 | sin Data Owner, sin origen de datos, fórmula ambigua |
# MAGIC
# MAGIC La respuesta esperada  se guarda en `<prefijo>_landing.respuesta_esperada_fichas_sinteticas` solo para medir
# MAGIC qué tan bien la solución detecta estos casos. **En producción este notebook no se usa:** las fichas reales llegan
# MAGIC al volumen (carga manual, Azure Storage, SharePoint) y el resto del flujo es idéntico.
# MAGIC
# MAGIC La ficha IND-001 es el ejemplo original de la plantilla (`datos_ejemplo/Ejemplo_Dummy.pdf`), que se copia tal cual.
# MAGIC
# MAGIC **Parámetros:** `reemplazar` (si = borra y regenera todas las fichas), `solo_ficha_nueva` (si = solo envía la ficha
# MAGIC nueva de la demostración en vivo). **Duración:** 1–2 minutos.

# COMMAND ----------

# MAGIC %pip install reportlab==4.4.10 --quiet
# MAGIC %restart_python

# COMMAND ----------

import os
import re
import shutil
import sys
import tempfile

dbutils.widgets.text("catalogo", "gobierno_datos", "Catálogo")
dbutils.widgets.text("prefijo_esquemas", "indicadores", "Prefijo de los esquemas")
dbutils.widgets.dropdown("reemplazar", "si", ["si", "no"], "Borrar y regenerar todas las fichas")
dbutils.widgets.dropdown("solo_ficha_nueva", "no", ["si", "no"], "Enviar solo la ficha nueva (demo en vivo)")
catalogo = dbutils.widgets.get("catalogo").strip()
prefijo = dbutils.widgets.get("prefijo_esquemas").strip()
assert re.fullmatch(r"[A-Za-z0-9_]+", catalogo) and re.fullmatch(r"[A-Za-z0-9_]+", prefijo)
reemplazar = dbutils.widgets.get("reemplazar") == "si"
solo_nueva = dbutils.widgets.get("solo_ficha_nueva") == "si"

# El código auxiliar vive junto a este notebook (carpetas config/ y herramientas/)
raiz = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
sys.path.insert(0, f"/Workspace{raiz}")
from config.catalogo_fichas import DOMINIOS, INDICADORES, RESPONSABLES, respuesta_esperada  # noqa: E402
from herramientas.plantilla_pdf import completar, generar_pdf  # noqa: E402

destino = f"/Volumes/{catalogo}/{prefijo}_landing/fichas_indicadores/recibidas"
original = f"/Workspace{raiz}/datos_ejemplo/Ejemplo_Dummy.pdf"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ficha nueva para la demostración en vivo
# MAGIC Cobranza documenta otra vez la morosidad, con otro nombre y otro umbral: la solución debe encontrarla sola.

# COMMAND ----------

FICHA_NUEVA = {
    "id": "IND-109", "nombre": "Porcentaje de Cartera en Mora", "area": "Cobranza", "dominio": "Cartera y Cobranza",
    "conflicto_con": "IND-016", "tipo": "Operativo", "unidad": "Porcentaje (%)",
    "definicion": "Saldo de los créditos con más de 90 días sin pago entre el saldo total de la cartera administrada, calculado al cierre de cada semana e incluyendo créditos castigados en el año.",
    "meta": "Menor o igual al 5%.",
    "pregunta": "¿Qué porcentaje de la cartera está en mora esta semana?",
    "exclusiones": "Ninguna.",
    "var_a": "Saldo insoluto con Dias_Atraso > 90, incluidos castigos del año.", "var_b": "Saldo insoluto total.",
    "formula": "(Variable A / Variable B) x 100",
    "fuente": ("Core de Crédito", "DB_CORE_CREDITO", "dbo", "Tbl_Cartera_Mensual"), "campos": "Id_Credito, Sdo_Insoluto, Dias_Atraso",
    "dimensiones": [("Convenio de Nómina", "Id_Convenio"), ("Región", "Cod_Region")],
}


def nombre_archivo(ind):
    limpio = re.sub(r"[^A-Za-z0-9]+", "_", ind["nombre"].encode("ascii", "ignore").decode()).strip("_")
    return f"{ind['id']}_{limpio}.pdf"


def escribir(ind, tmp):
    local = os.path.join(tmp, nombre_archivo(ind))
    generar_pdf(completar(ind, DOMINIOS, RESPONSABLES), local)
    shutil.copy(local, f"{destino}/{nombre_archivo(ind)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generar y copiar las fichas al volumen
# MAGIC Los PDF se generan en un directorio temporal y se copian al volumen de Unity Catalog con rutas `/Volumes/...`.
# MAGIC Documentación: [Trabajar con archivos en volúmenes](https://learn.microsoft.com/es-es/azure/databricks/volumes/volume-files)

# COMMAND ----------

with tempfile.TemporaryDirectory() as tmp:
    if solo_nueva:
        escribir(FICHA_NUEVA, tmp)
        print(f"Ficha nueva enviada: {nombre_archivo(FICHA_NUEVA)}")
    else:
        if reemplazar:
            dbutils.fs.rm(destino, recurse=True)
            dbutils.fs.mkdirs(destino)
        for ind in INDICADORES:
            if ind["id"] == "IND-001" and os.path.exists(original):
                shutil.copy(original, f"{destino}/{nombre_archivo(ind)}")  # ficha original de la plantilla
            else:
                escribir(ind, tmp)
        print(f"{len(INDICADORES)} fichas en {destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Respuesta esperada (solo para medir la detección en la demo)

# COMMAND ----------

filas = respuesta_esperada() + [(FICHA_NUEVA["id"], FICHA_NUEVA["conflicto_con"], "CONFLICTO")]
(spark.createDataFrame(filas, "id_ficha STRING, id_ficha_relacionada STRING, tipo_esperado STRING")
 .write.mode("overwrite").saveAsTable(f"`{catalogo}`.`{prefijo}_landing`.respuesta_esperada_fichas_sinteticas"))
spark.sql(f"COMMENT ON TABLE `{catalogo}`.`{prefijo}_landing`.respuesta_esperada_fichas_sinteticas IS "
          "'Solo demo: relaciones plantadas a propósito en las fichas sintéticas (duplicados, conflictos e incompletas) para medir la detección.'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificación

# COMMAND ----------

archivos = [f for f in dbutils.fs.ls(destino) if f.name.endswith(".pdf")]
esperadas = 1 if solo_nueva else len(INDICADORES)
assert len(archivos) >= esperadas, f"Se esperaban al menos {esperadas} PDF y hay {len(archivos)}"
print(f"{len(archivos)} fichas PDF en el volumen. Siguiente paso: pipeline de lectura de fichas.")
