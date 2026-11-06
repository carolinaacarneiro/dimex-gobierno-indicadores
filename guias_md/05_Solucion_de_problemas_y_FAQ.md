# Solución de problemas y preguntas frecuentes

Reunimos aquí los problemas que encontramos al construir y ejecutar la solución, y los que son previsibles al llevarla
a su entorno. Cada uno tiene su causa, cómo resolverlo y la documentación de referencia.

## 1. Problemas encontrados durante la construcción

| Síntoma | Causa | Solución | Referencia |
|---|---|---|---|
| `UC_TAG_POLICY_VALUE_NOT_ALLOWED` al ejecutar `SET TAGS` | La cuenta tiene una **etiqueta gobernada** con la misma clave (por ejemplo `proyecto` o `ambiente`) y el valor no está entre los permitidos | Usar claves propias para los objetos de Unity Catalog (la solución usa `solucion`, `capa`, `datos_personales`) o pedir al administrador que agregue el valor permitido. Revisar antes con `databricks tag-policies list-tag-policies` | [Etiquetas gobernadas](https://learn.microsoft.com/es-es/azure/databricks/admin/governed-tags/) |
| `ai_extract` devuelve `Could not parse the provided schema: Field 'type' must be of type 'object'` | En la versión 2.x el esquema es un objeto JSON que asocia **cada nombre de campo con su especificación**; no se envuelve en `{"type": "object", "properties": ...}` | Usar el formato de `config/esquema_ficha.json`: campos de primer nivel con `type`, `properties`, `items` o `labels` | [ai_extract](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_extract) |
| El índice de AI Search (antes Vector Search) se queda en *"pending endpoint provisioning"* durante mucho tiempo | El primer índice de un endpoint recién creado puede quedar esperando el aprovisionamiento | Esperar a que el endpoint esté `ONLINE` antes de crear el índice; si el índice no avanza, eliminarlo y volver a crearlo (el notebook 04 lo crea si no existe) | [Crear endpoints e índices](https://learn.microsoft.com/es-es/azure/databricks/ai-search/create-ai-search) |
| Con el umbral de similitud inicial (0.80) se escapaban duplicados | Las fichas duplicadas reales tienen redacción distinta; en la muestra los pares plantados obtuvieron entre 0.65 y 0.90 | Calibrar `umbral_similitud` con una muestra conocida (sección 5 de la guía de implementación). La demo usa 0.60: el juez de IA descarta los pares que no son duplicados ni conflictos | — |
| `AI_FUNCTION_UNSUPPORTED_RESPONSE_FORMAT.DDL_STRING` en `ai_query` | Con `responseFormat` en formato DDL solo se admite **un campo de primer nivel y de tipo STRUCT** | Envolver la estructura en un campo: `STRUCT<juicio:STRUCT<relacion:STRING, ...>>`. El modelo devuelve directamente el objeto interno (`{"relacion": ...}`) | [ai_query](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_query) |
| `Structured output is not supported for this model` | No todos los modelos admiten salidas estructuradas | Usar un modelo que las admita (la demo usa `databricks-claude-sonnet-4-6`); probarlo con una consulta corta antes de cambiar `modelo_llm` | [Salidas estructuradas](https://learn.microsoft.com/es-es/azure/databricks/machine-learning/model-serving/structured-outputs) |
| La regla "fórmula ambigua" marcaba indicadores de suma | Indicadores como *Monto Colocado Mensual* no tienen Variable B ("No aplica") | La regla considera ambigua la fórmula solo si falta la Variable A, si no usa variables o si usa la Variable B sin definirla | — |

## 2. Problemas previsibles en su entorno

| Síntoma | Causa | Solución | Referencia |
|---|---|---|---|
| `AI_FUNCTION_...` o "endpoint not found" al llamar `ai_query` | El modelo indicado en `modelo_llm` no existe en la región o el usuario no tiene permiso `CAN QUERY` | Listar los endpoints disponibles (`databricks serving-endpoints list`) y cambiar la variable; otorgar permiso al service principal | [Modelos admitidos](https://learn.microsoft.com/es-es/azure/databricks/machine-learning/model-serving/foundation-model-overview) |
| Las funciones de IA no están disponibles | Región sin soporte o funciones deshabilitadas | Revisar la disponibilidad por región y el procesamiento entre geografías | [Disponibilidad por región](https://learn.microsoft.com/es-es/azure/databricks/resources/feature-region-support) · [Geografías](https://learn.microsoft.com/es-es/azure/databricks/resources/databricks-geos) |
| `PERMISSION_DENIED` en la app de revisión | El service principal de la app no tiene permisos sobre las tablas | Ejecutar el notebook `12_otorgar_permisos` (o el job `configuracion_inicial`) y revisar `gold.avisos_publicacion`; el `USE CATALOG` lo otorga el propietario del catálogo (guía 03, sección 3.2) | [Recursos de la app](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-apps/sql-warehouse) |
| La app no muestra el usuario | Los encabezados de identidad no llegan (por ejemplo, al probar la app localmente) | Probar desde la URL de la app en el workspace | [Databricks Apps](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-apps/) |
| `CREATE VIEW ... WITH METRICS` falla | Runtime o warehouse sin soporte de vistas de métricas YAML 1.1, o una expresión inválida | Usar un SQL warehouse serverless actualizado; revisar `gold.vistas_de_metricas_generadas` (columna `error`) | [Vistas de métricas](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/) |
| Una medida generada no coincide con el reporte vigente | El modelo interpretó distinto una exclusión. Nos pasó durante la construcción: en una generación, la IA excluyó del denominador **todas** las citas canceladas cuando la ficha solo excluye las canceladas con más de 24 horas de anticipación (diferencia de hasta 2.6 puntos). La validación contra el cálculo de referencia lo detectó | No aprobar esa versión: corregir la expresión en el YAML propuesto (o mejorar la redacción de la ficha) y aprobar la versión corregida. Por eso, en producción, ninguna vista de métricas se publica sin la aprobación del Data Steward | [Referencia YAML](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/yaml-reference) |
| El SQL warehouse está detenido y la app tarda en responder | Arranque del warehouse | Los warehouses serverless arrancan en segundos; para demos, enciéndanlo unos minutos antes | [SQL warehouses](https://learn.microsoft.com/es-es/azure/databricks/compute/sql-warehouse/) |
| `%pip install reportlab` falla en `01_generar_fichas_sinteticas` | El cómputo serverless no tiene salida a PyPI (políticas de red del workspace) | Solo afecta a la demo. Instalen la librería desde un *wheel* en un volumen (`%pip install /Volumes/.../reportlab-*.whl`) o configuren un entorno base del workspace | [Bibliotecas en serverless](https://learn.microsoft.com/es-es/azure/databricks/compute/serverless/dependencies) |
| El pipeline no lee fichas nuevas | Archivo con extensión distinta a `.pdf` o fuera de `recibidas/` | Ajustar `pathGlobFilter` o la ruta en la configuración del pipeline | [Auto Loader](https://learn.microsoft.com/es-es/azure/databricks/ingestion/cloud-object-storage/auto-loader/) |
| Cambió la plantilla (nueva versión) | Campos nuevos o renombrados | Editar `config/esquema_ficha.json` y la selección de columnas en `fichas.py`; ejecutar el pipeline con *full refresh* | [Full refresh](https://learn.microsoft.com/es-es/azure/databricks/ldp/updates) |
| La vista de costos no muestra datos | Los registros de facturación tardan hasta 12 horas, o falta acceso a `system.billing` | Esperar; pedir al administrador acceso de lectura a las tablas del sistema | [Facturación](https://learn.microsoft.com/es-es/azure/databricks/admin/system-tables/billing) |
| Genie responde con otro cálculo | Falta contexto o la pregunta no se parece a los ejemplos | Agregar instrucciones y consultas de ejemplo en la interfaz de Genie (se conservan con `sobrescribir_genie = no`) y medir con las preguntas de referencia | [Buenas prácticas de Genie](https://learn.microsoft.com/es-es/azure/databricks/genie-agents/best-practices) |

## 3. Preguntas frecuentes

**¿Un indicador sin hallazgos se certifica solo?**
No. Queda en estado `PENDIENTE_APROBACION` hasta que su Data Owner lo aprueba en la pestaña *Aprobar indicadores* de la
app. Si la ficha cambia después, la aprobación deja de valer.

**¿La IA decide qué indicador es el oficial?**
No. La IA propone (duplicado, conflicto, diferencias, ficha sugerida); la decisión la toma el responsable en la app y
queda registrada con su usuario.

**¿Qué pasa con una ficha retirada?**
Queda en el catálogo con estado `RETIRADO` y el ID de la ficha oficial en `reemplazada_por`; su nombre pasa a ser
sinónimo del indicador oficial (y Genie lo reconoce).

**¿Puedo seguir usando mis herramientas de BI actuales?**
Sí. Las vistas de métricas se consultan desde el SQL warehouse con SQL estándar y `MEASURE()`; la idea es que los
tableros consuman la definición certificada en lugar de recalcularla.

**¿Qué pasa si un área actualiza su ficha?**
Al dejar la nueva versión en el volumen (con el mismo nombre de archivo o con otro, siempre que conserve el ID del
indicador), el pipeline la procesa y `fichas_vigentes` toma la versión más reciente. Si cambió su contenido, sus pares
se vuelven a evaluar, los hallazgos nuevos quedan pendientes, el indicador vuelve a revisión en la siguiente
publicación y sus reglas de calidad se vuelven a extraer.

**¿Cuántas fichas soporta?**
El diseño es incremental y la búsqueda semántica evita comparar todas contra todas; cientos o miles de fichas no
cambian la arquitectura. El costo crece con el número de fichas nuevas, no con el total.

**¿Los documentos salen de Databricks?**
Las fichas se guardan en un volumen de Unity Catalog y se procesan con funciones de IA y modelos servidos por
Databricks. Dónde se procesan depende de la región: si se habilita el
[procesamiento entre geografías](https://learn.microsoft.com/es-es/azure/databricks/resources/databricks-geos) (necesario en
algunas regiones) o se usa un workspace en otra región, el procesamiento ocurre fuera de la región del workspace.
Revísenlo con Cumplimiento antes de decidir (guía 03, sección 3.1). Ver
[Foundation Model APIs](https://learn.microsoft.com/es-es/azure/databricks/machine-learning/foundation-model-apis/).

**¿Qué funcionalidades están en versión preliminar?**
El modelo de embeddings multilingüe usado en la demo, las políticas de uso serverless y *Pages* están en versión
preliminar o Beta; ninguna es indispensable: el modelo de embeddings es una variable del bundle y *Pages* es opcional.
