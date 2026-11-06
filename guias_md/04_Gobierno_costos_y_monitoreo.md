# Gobierno, costos y monitoreo

Una solución de gobierno de datos también tiene que ser gobernable: en este documento les mostramos cómo saber cuánto
cuesta, cómo se vigila sola y dónde ver qué pasó. Lo que viene implementado en el bundle y lo que se configura en la
consola (porque requiere permisos de administrador) se indica en cada sección.

## 1. Estándar de etiquetas

Usamos dos familias de etiquetas, porque cumplen funciones distintas:

**a) Etiquetas de cómputo** (jobs y pipelines): describen el recurso y, según el tipo de cómputo, llegan a `system.billing.usage.custom_tags`. Para el cómputo **serverless**, la forma documentada de garantizar que las etiquetas lleguen a la facturación es una **política de uso serverless** (sección 2); por eso la vista de costos también identifica el uso por job, pipeline, endpoint y app.

| Clave | Valor en la demo | Para qué sirve | Dónde se define |
|---|---|---|---|
| `proyecto` | `gobierno_indicadores` | Costo por solución | `presets.tags` del destino en `databricks.yml` |
| `ambiente` | `demo` / `dev` / `prod` | Separar entornos | `presets.tags` |
| `area_negocio` | `gobierno_de_datos` | Asignación de costos (*chargeback*) | `presets.tags` |
| `responsable` | `<equipo_datos>` | Dueño técnico | `presets.tags` |
| `centro_costos` | `<centro_de_costos>` | Mapeo con Finanzas | `presets.tags` (destino `cliente`) |

**b) Etiquetas de Unity Catalog** (esquemas, tablas, vistas y columnas): describen y gobiernan los datos.

| Clave | Ejemplo | Dónde se aplica |
|---|---|---|
| `dominio_datos` | `oportunidades`, `cartera_y_cobranza`, `gobierno_de_datos` | Tablas de origen, catálogo, glosario y vistas de métricas |
| `responsable_negocio` / `responsable_tecnico` | `gerente_de_operaciones_telemarketing` | Tablas de origen (tomado del Bloque 4 de las fichas) |
| `datos_personales` | `identificador_cliente` | Columnas `Id_Cliente` de las tablas sintéticas de la demo (en su entorno, etiqueten o clasifiquen las columnas personales de las tablas reales) |
| `solucion`, `capa` | `gobierno_indicadores`, `gold` | Esquemas y tablas de la solución |
| `system.certification_status` | `certified` | Catálogo, glosario y vistas de métricas oficiales |

> **Antes de elegir las claves, revisen las etiquetas gobernadas existentes en su cuenta.** Una etiqueta gobernada
> solo acepta los valores permitidos: si su cuenta ya tiene, por ejemplo, una política para `proyecto` o `ambiente`
> con otros valores, `SET TAGS` sobre objetos de Unity Catalog con esa clave fallará. Para listarlas:
> `databricks tag-policies list-tag-policies`. Durante la construcción de esta demo encontramos exactamente este caso y
> por eso las etiquetas de Unity Catalog de la solución usan claves propias (`solucion`, `capa`, `datos_personales`).
> Documentación: [Etiquetas gobernadas](https://learn.microsoft.com/es-es/azure/databricks/admin/governed-tags/) ·
> [Etiquetas en Unity Catalog](https://learn.microsoft.com/es-es/azure/databricks/database-objects/tags) ·
> [Etiquetas de uso](https://learn.microsoft.com/es-es/azure/databricks/admin/account-settings/usage-detail-tags)

### Evolución recomendada: Dominios de Unity Catalog (no implementado en el bundle)

En la demo, `dominio_datos` es una etiqueta libre que clasifica cada activo. Para organizar el catálogo por dominios
en la página *Discover* de Unity Catalog, les recomendamos estos pasos, que requieren permisos de administrador o
curador y por eso no se automatizan:

1. **Crear una etiqueta gobernada por dominio.** En Unity Catalog, cada Dominio se basa en su propia etiqueta
   gobernada (por ejemplo, el dominio "Oportunidades" sobre la etiqueta `oportunidades`); los subdominios usan claves
   con el formato `dominio/subdominio`. Usen como claves los valores de `gold.dominios.valor_etiqueta`. Se crean en
   *Catalog* → *Governed tags* o con `databricks tag-policies create-tag-policy`. Ver
   [etiquetas gobernadas](https://learn.microsoft.com/es-es/azure/databricks/admin/governed-tags/).
2. **Crear los Dominios** en *Discover* sobre esas etiquetas y asignar los activos (aplicando la etiqueta del dominio a
   tablas, vistas de métricas, dashboards o agentes de Genie; si se etiqueta un catálogo o esquema, lo heredan sus
   objetos). Requiere el permiso de curador (`MANAGE DISCOVER`). Ver
   [Dominios y subdominios](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/domains).
3. Opcionalmente, publicar el glosario de cada dominio en **Pages** (versión Beta) usando la exportación Markdown que
   genera el notebook `06` en `exportaciones/pages/`. Ver [Pages](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/pages).

`dominio_datos` puede mantenerse como etiqueta de clasificación. Antes de crear etiquetas gobernadas, verifiquen con
su administrador que no existan políticas con las mismas claves.

## 2. Política de uso serverless y presupuestos

- **Política de uso serverless** (*serverless usage policy*, antes *budget policy*; **versión preliminar pública**):
  un administrador del workspace crea la política (*Settings* → *Compute* → *Serverless usage policies*) con las
  etiquetas del proyecto y la asigna a los usuarios o service principals que ejecutan la solución. Así, también el uso
  serverless de notebooks interactivos queda etiquetado. Tengan en cuenta que los pipelines disparados por un job no
  heredan la política del job: asígnenla también al pipeline. **No se configura en el bundle de la demo.**
  Ver [políticas de uso serverless](https://learn.microsoft.com/es-es/azure/databricks/admin/usage/budget-policies).
- **Presupuestos de cuenta** (consola de la cuenta → *Usage* → *Budgets*): envían un correo cuando el gasto del filtro
  (por ejemplo, `proyecto = gobierno_indicadores`) supera un monto mensual. Requieren un administrador de la cuenta.
  Ver [presupuestos](https://learn.microsoft.com/es-es/azure/databricks/admin/account-settings/budgets).

## 3. Cómo ver el costo

El notebook `10_costos_y_monitoreo` crea la vista `gold.costos_solucion`, valorizada a precio de lista, que reúne el
uso con la etiqueta `proyecto`, el de los jobs y pipelines de la solución, el del endpoint de AI Search
(`usage_metadata.endpoint_name`) y el de la app (`usage_metadata.app_name`). El SQL warehouse se incluye solo si es
exclusivo de la solución (variable del bundle `warehouse_id_costos`); si es compartido, su costo no es atribuible:

```sql
SELECT producto, round(sum(dbus), 2) AS dbus, round(sum(costo_lista_usd), 2) AS costo_lista_usd
FROM <mi_catalogo>.indicadores_gold.costos_solucion
WHERE usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL ORDER BY costo_lista_usd DESC;
```

Cómo leerla:

| `producto` (`billing_origin_product`) | Qué representa en esta solución |
|---|---|
| `JOBS` | Notebooks de los jobs (generación, detección, publicación, calidad) |
| `DLT` | Pipeline de lectura de fichas (Lakeflow pipelines) |
| `AI_FUNCTIONS` | `ai_parse_document`, `ai_extract`, `ai_classify` y `ai_query` |
| `MODEL_SERVING` | Llamadas a los modelos servidos (incluidos los embeddings) |
| `VECTOR_SEARCH` | Endpoint e índice de búsqueda semántica (AI Search, antes Vector Search) |
| `SQL` | Warehouse de la app, dashboard, alertas y Genie (solo si el warehouse es exclusivo) |
| `APPS` | Cómputo de la app de revisión |

El dashboard **Gobierno de indicadores · Operación** (página *Costos*, compartido solo con `grupo_operacion`) muestra la evolución diaria por producto. Los datos de facturación pueden tardar
algunas horas en aparecer. El costo a precio de lista es una referencia: el costo real depende de su contrato.
Ver [tablas del sistema de facturación](https://learn.microsoft.com/es-es/azure/databricks/admin/system-tables/billing).

## 4. Alertas configuradas

Las alertas se despliegan **pausadas** (`estado_alertas = PAUSED`, valor por defecto), porque las tablas que
consultan no existen hasta la primera publicación. Actívenlas con `UNPAUSED` y vuelvan a desplegar; para probarlas
antes, usen *Ejecutar ahora* en *SQL* → *Alertas*.

| Alerta | Qué vigila | Umbral (variable del bundle) | Frecuencia |
|---|---|---|---|
| Protocolo de contingencia activado | Indicadores marcados en pausa porque falló una regla de contingencia (por ejemplo, estatus nulos > 2% en IND-001) | `> 0` | Cada hora |
| Fuente sin carga reciente | Horas desde la última carga de `tabla_frescura` (columna `columna_frescura`; en la demo, `tbl_citas_master._fecha_carga`). En su entorno apúntenla a la tabla real | `> horas_maximas_sin_carga` (72) | Diaria 10:00 |
| IND-001 por debajo de la meta | Tasa de Conversión de Cita Efectiva del último mes | `< meta_indicador_alerta_kpi` (85) | Mensual, día 2 |
| Hallazgos de prioridad alta sin resolver | Conflictos y fichas incompletas críticas sin decisión | `> 0` después de `dias_maximos_revision` días | Semanal |
| Costo diario por encima del umbral | Costo de ayer de la solución | `> umbral_costo_diario_usd` | Diaria 10:00 |

Todas notifican a `correo_alertas`, que debe ser el correo de un **usuario del workspace**. Para notificar a una lista
de distribución, Teams o un webhook, un administrador crea un **destino de notificación** y en
`resources/alertas.yml` se reemplaza `user_email` por `destination_id`. Para ajustar un umbral cambien la variable y
vuelvan a desplegar el bundle. Ver [destinos de notificación](https://learn.microsoft.com/es-es/azure/databricks/admin/workspace-settings/notification-destinations).

**Qué significa "en pausa":** el indicador queda con `publicar = false` en `gold.estado_publicacion_indicadores` y la
alerta notifica al Steward, que aplica el protocolo (por ejemplo, pausar la actualización del tablero). Las vistas de
métricas siguen respondiendo; si quieren bloquear automáticamente el consumo, filtren los tableros o la vista por ese
estado. Las reglas se evalúan en cada carga de las fuentes (job `calidad_por_carga`) y en cada publicación.
Ver [alertas de Databricks SQL](https://learn.microsoft.com/es-es/azure/databricks/sql/user/alerts/).

Además, los jobs notifican por correo cuando fallan (`email_notifications.on_failure`) y el job de procesamiento tiene
una regla de salud si dura más de una hora (`health`). Ver
[notificaciones de jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/notifications).

## 5. Calidad de datos y monitoreo operativo

| Qué | Dónde |
|---|---|
| Completitud de la documentación de las fichas | Expectativas del pipeline (`tiene_data_owner`, `tiene_origen_de_datos`, `tiene_formula_con_variables`…), visibles en la página del pipeline y en su *event log*. Ver [expectativas](https://learn.microsoft.com/es-es/azure/databricks/ldp/expectations). |
| Reglas de calidad de cada indicador (Bloque 5) | `gold.reglas_calidad`, `gold.resultados_calidad` (historial) y `gold.estado_publicacion_indicadores` |
| Historial de ejecuciones | `system.lakeflow.job_run_timeline` (consulta en el notebook 10) |
| Linaje | Pestaña *Lineage* de Catalog Explorer y `system.access.table_lineage` |
| Auditoría de accesos | `system.access.audit` |
| Auditoría de decisiones y aprobaciones | `gold.decisiones_revision`, `gold.aprobaciones_indicadores` y la página *Auditoría* del dashboard de operación |
| Acciones de gobierno no aplicadas | `gold.avisos_publicacion` (etiquetas, certificación o permisos que no se pudieron aplicar) y la página *Auditoría* del dashboard de operación |

## 6. Cómo estimar el costo en producción

El costo de esta solución lo determinan pocas variables. Les recomendamos medirlo en su entorno con un lote real y
proyectar:

| Variable | Impacto |
|---|---|
| Número de fichas nuevas o modificadas por periodo | Análisis de PDF, extracción y clasificación (una vez por versión de ficha) |
| Páginas por ficha | El costo de las funciones de IA depende del volumen procesado; ver [costos de las funciones de IA](https://learn.microsoft.com/es-es/azure/databricks/large-language-models/ai-functions) |
| Pares similares por ficha nueva | Llamadas al juez (`ai_query`); el umbral de similitud las controla |
| Tamaño del modelo del juez | Un modelo más pequeño reduce el costo por par; validen la precisión con la muestra |
| AI Search (antes Vector Search) | El endpoint consume mientras existe, aunque no se consulte; para cargas esporádicas consideren crearlo solo durante la revisión. Ver [AI Search](https://learn.microsoft.com/es-es/azure/databricks/ai-search/ai-search) |
| Frecuencia de publicación y de evaluación de calidad | Ejecuciones de los jobs y del warehouse |
| Usuarios de la app, Genie y dashboard | Uso del SQL warehouse (serverless se apaga solo cuando no hay uso) |

En la demo, el procesamiento completo de 62 fichas desde cero se registra en `gold.costos_solucion`; úsenlo como
referencia por ficha para proyectar los ~600 indicadores.
