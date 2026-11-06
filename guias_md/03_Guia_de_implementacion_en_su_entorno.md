# Guía de implementación en su entorno

Esta guía les acompaña a llevar la solución de la demo a su propio workspace de Azure Databricks, con sus fichas reales
y sus tablas de origen. Seguimos el mismo orden en que recomendamos hacerlo: primero el entorno, luego las fichas,
después las fuentes y, al final, el paso a producción.

## 1. Diferencias entre la demo y producción

| Elemento | En la demo | En producción |
|---|---|---|
| Fichas | 62 PDF sintéticos generados por `01_generar_fichas_sinteticas` | Sus fichas reales en el volumen `recibidas/` (carga manual, Azure Storage o SharePoint) |
| Tablas de origen | `tbl_citas_master`, `cat_sucursales` y `tbl_cartera_mensual` sintéticas | Sus tablas reales (CRM, cartera…) ingeridas o federadas en Unity Catalog |
| Revisión | `05_simular_revision` registra decisiones automáticas | Los Data Owners y Stewards deciden en la app; **no se ejecuta el notebook 05** |
| Respuesta esperada | `respuesta_esperada_fichas_sinteticas` mide la detección | Se reemplaza por una muestra revisada manualmente (sección 5) |
| Jobs de demostración | `demo_completa`, `demo_llega_ficha_nueva`, `demo_falla_de_calidad` | Solo existen en el destino `demo`; en producción se usan `actualizacion_fichas` y `publicacion_catalogo` |
| Destino del bundle | `demo` (modo desarrollo) | `cliente` (modo producción, ejecutado por un service principal) |

## 2. Mapeo de datos: de la demo a sus fuentes

| Dato de la demo | Su fuente real | Cómo conectarla |
|---|---|---|
| Fichas PDF en `recibidas/` | Carpeta compartida o sitio de SharePoint donde las áreas dejan las fichas | Copia programada al volumen, [conector de SharePoint de Lakeflow Connect](https://learn.microsoft.com/es-es/azure/databricks/ingestion/lakeflow-connect/sharepoint) o un volumen externo sobre Azure Storage ([ubicaciones externas](https://learn.microsoft.com/es-es/azure/databricks/connect/unity-catalog/cloud-storage/)) |
| Fichas en Word, imagen u otro formato | Igual que arriba | `ai_parse_document` también admite otros formatos de documento e imagen; revisen la [lista de formatos admitidos](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_parse_document) y ajusten `pathGlobFilter` en `src/pipeline/transformaciones/fichas.py` |
| Fichas capturadas en formularios o Excel | Exportación del formulario | No hace falta el paso de PDF: carguen el Excel/CSV en una tabla y mapeen sus columnas a `silver.fichas_estructuradas` |
| `tbl_citas_master` | `DB_CRM_PROD.dbo.Tbl_Citas_Master` (SQL Server) | [Lakeflow Connect para SQL Server](https://learn.microsoft.com/es-es/azure/databricks/ingestion/lakeflow-connect/sql-server-overview) (ingesta incremental, **recomendada**) o [Lakehouse Federation](https://learn.microsoft.com/es-es/azure/databricks/query-federation/) (consulta sin copiar; ver nota abajo) |
| `cat_sucursales` | Estructura de sucursales (DataShare) | Igual que el anterior, según el sistema |
| `tbl_cartera_mensual` | `DB_CORE_CREDITO.dbo.Tbl_Cartera_Mensual` | Igual que el anterior |
| Sus tablas ya migradas a Databricks | Esquemas existentes en Unity Catalog | Basta con apuntar `config/mapeo_fuentes.json` a `catalogo.esquema.tabla` |

> **Nota sobre Lakehouse Federation:** la evaluación de calidad en cada carga (job `calidad_por_carga`) usa un
> [desencadenador por actualización de tablas](https://learn.microsoft.com/es-es/azure/databricks/jobs/trigger-table-update),
> que no admite tablas federadas y vigila hasta 10 tablas por job (25 con *file events* habilitados). Si federan las fuentes, programen `calidad_por_carga`
> por horario en lugar del desencadenador y definan en `mapeo_fuentes.json` una `columna_carga` que identifique la
> última carga (las tablas federadas suelen no tenerla). Por eso recomendamos ingerir las fuentes con Lakeflow Connect.

**`config/mapeo_fuentes.json`** es el puente entre la ficha y Unity Catalog. La clave es `BASE.ESQUEMA.TABLA` tal como
aparece en el Bloque 3 de la ficha; el valor indica la tabla real, el nombre de la vista de métricas, la columna de
fecha, la columna que identifica la última carga y las tablas de catálogo para dimensiones:

```json
"DB_CRM_PROD.dbo.Tbl_Citas_Master": {
  "tabla": "crm_prod.bronze.tbl_citas_master",
  "vista": "mv_citas_telemarketing",
  "columna_fecha": "Fec_Cita",
  "columna_carga": "_fecha_carga",
  "joins": [{"nombre": "sucursal", "tabla": "datos_maestros.silver.cat_sucursales",
             "on": "source.Id_Sucursal_Destino = sucursal.Id_Sucursal"}]
}
```

Les recomendamos agregar a sus tablas **comentarios de columna** (como hace `02_generar_datos_fuentes`): el generador de
vistas de métricas, las reglas de calidad y Genie los usan para entender cada campo.

## 3. Configuración paso a paso

### 3.1 Requisitos

| Requisito | Detalle |
|---|---|
| Workspace con Unity Catalog | Y cómputo serverless habilitado para notebooks, jobs y pipelines |
| **Región compatible** | Ver la tabla de regiones de abajo: **revísenla antes de cualquier otro paso** |
| SQL warehouse serverless | Para la app, el dashboard, las alertas y Genie |
| Funciones de IA y Foundation Model APIs (pago por token) | `ai_parse_document`, `ai_extract`/`ai_classify` v2 y `ai_query` |
| AI Search (antes Vector Search) | Permiso para crear endpoints |
| Databricks Apps | Habilitado en el workspace |
| Genie | La opción **"Funciones de IA impulsadas por socios"** habilitada por un administrador (viene desactivada por defecto en workspaces con perfil de seguridad de cumplimiento) y, para quienes usen los agentes, el derecho de *consumer access* o de *Databricks SQL* en el workspace. El service principal que ejecuta los jobs (crea los agentes en el notebook 09) también necesita el derecho de Databricks SQL. Ver [funciones de IA impulsadas por socios](https://learn.microsoft.com/es-es/azure/databricks/databricks-ai/partner-powered) y [configurar Genie Agents](https://learn.microsoft.com/es-es/azure/databricks/genie-agents/set-up) |
| Modelo de embeddings | La demo usa `databricks-qwen3-embedding-0-6b` (multilingüe, **versión preliminar pública**). Antes de producción, validen su disponibilidad o elijan otro endpoint de embeddings multilingüe disponible en su región (variable `modelo_embeddings`) |
| Databricks CLI | Versión con soporte de bundles, apps, dashboards y alertas (la demo se validó con la versión 1.21.0). Ver [instalación](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/cli/install) |
| Permisos de administrador (opcional) | Para crear etiquetas gobernadas y Dominios en *Discover* |

**Disponibilidad por región (Azure).** Según la [tabla oficial de disponibilidad por región](https://learn.microsoft.com/es-es/azure/databricks/resources/feature-region-support)
(consultada en octubre de 2026; verifiquen la versión vigente):

| Región | AI Search | Databricks Apps | `ai_parse_document` v2, `ai_extract`/`ai_classify` v2 | Foundation Model APIs pago por token | ¿Sirve para esta solución? |
|---|---|---|---|---|---|
| `centralus`, `eastus`, `eastus2`, `southcentralus`, `westus`, `westus2`, `westus3`, `northcentralus` | ✓ | ✓ | ✓ | ✓ | Sí |
| `brazilsouth` | ✓ | ✓ | ✓ con procesamiento entre geografías | ✓ con procesamiento entre geografías | Sí, habilitando el [procesamiento entre geografías](https://learn.microsoft.com/es-es/azure/databricks/resources/databricks-geos) |
| `mexicocentral` | — | — | — | — | **No.** Estas capacidades no están disponibles en esa región; ver la nota de residencia de datos |

> **Residencia de datos.** Usar un workspace en otra región (por ejemplo, de EE. UU.) o habilitar el procesamiento
> entre geografías implica que los documentos o los datos se procesen fuera de México. Antes de decidirlo, revísenlo
> con sus áreas de Cumplimiento y Jurídico frente a sus obligaciones regulatorias. Alternativas a evaluar: mantener
> los datos operativos en su región y procesar en otra región solo las fichas de documentación (que describen
> indicadores, no contienen datos de clientes), compartiendo entre workspaces con
> [Delta Sharing](https://learn.microsoft.com/es-es/azure/databricks/delta-sharing/) o
> [Lakehouse Federation](https://learn.microsoft.com/es-es/azure/databricks/query-federation/).

Revisen también los modelos disponibles por región en [modelos de Foundation hospedados en Databricks](https://learn.microsoft.com/es-es/azure/databricks/unity-gateway/model-region-availability).

### 3.2 Catálogo, esquemas y permisos

1. Elijan el catálogo (por ejemplo `gobierno_datos`) y el prefijo de esquemas (por defecto `indicadores`).
   El notebook `00_preparar_entorno` crea los esquemas y el volumen.
2. Creen los grupos en la cuenta y asignen permisos mínimos:

```sql
-- Áreas que envían fichas: solo escriben en el volumen
GRANT USE CATALOG ON CATALOG <mi_catalogo> TO `areas_negocio`;
GRANT USE SCHEMA ON SCHEMA <mi_catalogo>.indicadores_landing TO `areas_negocio`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME <mi_catalogo>.indicadores_landing.fichas_indicadores TO `areas_negocio`;

-- Consumidores y revisores: leen los objetos publicados (el notebook 12 otorga estos permisos tabla por tabla)
GRANT USE CATALOG ON CATALOG <mi_catalogo> TO `revisores_indicadores`;
GRANT USE SCHEMA ON SCHEMA <mi_catalogo>.indicadores_gold TO `revisores_indicadores`;
GRANT USE SCHEMA ON SCHEMA <mi_catalogo>.indicadores_silver TO `revisores_indicadores`;
GRANT SELECT ON TABLE <mi_catalogo>.indicadores_silver.fichas_vigentes TO `revisores_indicadores`;   -- la usa el agente de Genie
GRANT SELECT ON TABLE <mi_catalogo>.indicadores_gold.catalogo_indicadores TO `revisores_indicadores`;
-- ... y glosario_negocio, reglas_homologacion, dominios, hallazgos_revision, uso_vocabulario_no_homologado,
--     estado_publicacion_indicadores, resultados_calidad, serie_mensual_indicadores y las vistas mv_*
-- La vista costos_solucion y las tablas de auditoría (decisiones_revision, pares_evaluados) no se otorgan a consumidores.
```

   El notebook `12_otorgar_permisos` (última tarea de los jobs `configuracion_inicial`, `actualizacion_fichas`, `publicacion_catalogo` y `publicar_vistas_aprobadas`) otorga los `SELECT` tabla por tabla sobre los objetos
   que existen en ese momento, incluidas las vistas `mv_*` nuevas, y los permisos del service principal de la app
   (lectura de fichas y catálogo; escritura solo en la cola, las decisiones y las aprobaciones). Lo que no pueda
   aplicar queda en `gold.avisos_publicacion`. Ver [privilegios de Unity Catalog](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/manage-privileges/).

   **Permisos a nivel de catálogo (los otorga el propietario del catálogo, una vez).** El service principal de los
   jobs no puede otorgar `USE CATALOG` si no es propietario del catálogo ni tiene `MANAGE` sobre él:

```sql
GRANT USE CATALOG ON CATALOG <mi_catalogo> TO `revisores_indicadores`;
GRANT USE CATALOG ON CATALOG <mi_catalogo> TO `operacion_datos`;
GRANT USE CATALOG ON CATALOG <mi_catalogo> TO `<application_id_del_service_principal_de_la_app>`;  -- ver Compute → Apps → revision-indicadores
```

   **Grupos (todos de la cuenta, no locales del workspace):**

   | Variable | Rol | Qué recibe |
   |---|---|---|
   | `grupo_revisores` | Consumidores | `SELECT` sobre lo publicado, dashboard principal y agentes de Genie |
   | `grupo_aprobadores` | Data Owners y Stewards que deciden en la app | `CAN_USE` sobre la app de revisión |
   | `grupo_stewards` | Data Stewards | Además, `SELECT, MODIFY` sobre `reglas_calidad` y `definicion_vistas_de_metricas` (los otorga el notebook 12) y `CAN_MANAGE_RUN` sobre los jobs `publicacion_catalogo` y `publicar_vistas_aprobadas` (definido en el bundle). `aprobadores_por_dominio` la administra el propietario del esquema (gobierno de datos), y las aprobaciones de indicadores se registran solo desde la app |
   | `grupo_operacion` | Operación y finanzas | Además, costos, auditoría y avisos (dashboard de operación) |

   **Control por dominio (recomendado):** carguen en `gold.aprobadores_por_dominio` los correos que pueden decidir y
   aprobar en cada dominio; la app rechaza decisiones de otros usuarios en ese dominio. Si un dominio no tiene filas,
   cualquier miembro de `grupo_aprobadores` puede decidir en él:

```sql
INSERT INTO <mi_catalogo>.indicadores_gold.aprobadores_por_dominio VALUES
  ('Oportunidades', 'gerente.telemarketing@su-empresa.com'),
  ('Cartera y Cobranza', 'gerente.cobranza@su-empresa.com');
```

   La app escribe con su service principal; la persona que decide queda en la columna `revisor` de
   `gold.decisiones_revision` y en `gold.aprobaciones_indicadores`.

3. **Service principal que ejecuta los jobs** (`run_as` del destino `cliente`). Necesita, como mínimo:

```sql
-- Catálogo de la solución: crear y administrar los esquemas <prefijo>_*
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG <mi_catalogo> TO `<sp_gobierno_indicadores>`;
-- Si los esquemas ya existen, el service principal debe ser su propietario o tener MANAGE sobre ellos
-- (el notebook 12 otorga permisos a otros grupos):  GRANT MANAGE ON SCHEMA <mi_catalogo>.indicadores_gold TO `<sp...>`;

-- Tablas de origen reales (una línea por cada tabla de mapeo_fuentes.json), con permiso para etiquetarlas
GRANT USE CATALOG ON CATALOG <catalogo_fuentes> TO `<sp_gobierno_indicadores>`;
GRANT USE SCHEMA ON SCHEMA <catalogo_fuentes>.<esquema_fuentes> TO `<sp_gobierno_indicadores>`;
GRANT SELECT, APPLY TAG ON TABLE <catalogo_fuentes>.<esquema_fuentes>.<tabla_origen> TO `<sp_gobierno_indicadores>`;

-- Tablas del sistema (notebook 10: costos, ejecuciones y linaje); las otorga un administrador del metastore
GRANT USE CATALOG ON CATALOG system TO `<sp_gobierno_indicadores>`;
GRANT USE SCHEMA ON SCHEMA system.billing TO `<sp_gobierno_indicadores>`;
GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `<sp_gobierno_indicadores>`;
GRANT USE SCHEMA ON SCHEMA system.access TO `<sp_gobierno_indicadores>`;
GRANT SELECT ON TABLE system.billing.usage TO `<sp_gobierno_indicadores>`;
GRANT SELECT ON TABLE system.billing.list_prices TO `<sp_gobierno_indicadores>`;
GRANT SELECT ON TABLE system.lakeflow.job_run_timeline TO `<sp_gobierno_indicadores>`;
GRANT SELECT ON TABLE system.access.table_lineage TO `<sp_gobierno_indicadores>`;
```

   Además: `ASSIGN` sobre la etiqueta gobernada de sistema `system.certification_status` (para certificar tablas y
   vistas), `CAN QUERY` sobre los endpoints `modelo_llm` y `modelo_embeddings`, `CAN USE` sobre el SQL warehouse y
   permiso para crear endpoints de AI Search. Ver [etiquetas](https://learn.microsoft.com/es-es/azure/databricks/database-objects/tags) y
   [tablas del sistema](https://learn.microsoft.com/es-es/azure/databricks/admin/system-tables/billing).

### 3.3 Variables del bundle (destino `cliente`)

En `databricks.yml` descomenten el destino `cliente` y completen:

| Variable | Qué poner |
|---|---|
| `workspace.host` | URL de su workspace (`https://adb-<id>.<n>.azuredatabricks.net`) |
| `run_as.service_principal_name` | *Application ID* del service principal que ejecuta los jobs |
| `catalogo`, `prefijo_esquemas` | Su catálogo y prefijo |
| `warehouse_id` | ID de su SQL warehouse serverless |
| `modelo_llm` | Endpoint de chat disponible en su región (juez, vistas de métricas, reglas de calidad) |
| `modelo_embeddings` | Endpoint de embeddings multilingüe disponible en su región |
| `endpoint_vector_search` | Nombre del endpoint (se crea si no existe) |
| `tag_dominio` | Clave de la etiqueta de dominio (por defecto `dominio_datos`) |
| `grupo_revisores`, `grupo_aprobadores`, `grupo_stewards` | Grupos de la cuenta de consumidores, de quienes usan la app y de Data Stewards (sección 3.2) |
| `estado_alertas` | `PAUSED` al desplegar; `UNPAUSED` después de la primera publicación |
| `grupo_operacion` | Grupo que ve costos, auditoría y avisos (dashboard de operación) |
| `aprobar_indicadores_automaticamente` | Déjenlo en `no` (por defecto): los indicadores sin hallazgos se certifican solo cuando el Data Owner los aprueba en la app. `si` solo en la demo |
| `id_indicador_alerta_kpi` | ID del indicador que vigila la alerta de meta (en la demo, `IND-001`) |
| `correo_alertas` | Correo de un **usuario del workspace** para fallas de jobs y alertas SQL (para listas de distribución, configuren un destino de notificación; ver guía 04) |
| `tablas_origen_calidad` | Tablas de origen reales (hasta 10, o 25 con *file events*; no federadas) que disparan la evaluación de calidad |
| `tabla_frescura`, `columna_frescura`, `horas_maximas_sin_carga` | Tabla de origen real, su columna de fecha de carga y las horas sin carga que disparan la alerta de frescura |
| `modo_vistas_de_metricas`, `aprobar_reglas_automaticamente` | Déjenlos en `proponer` y `no` (valores por defecto): la IA propone y el Data Steward aprueba. `generar` y `si` solo se usan en la demo |
| `warehouse_id_costos` | ID del SQL warehouse si es exclusivo de la solución (para sumarlo a la vista de costos); vacío si es compartido |
| `meta_indicador_alerta_kpi`, `dias_maximos_revision`, `umbral_costo_diario_usd` | Umbrales de las alertas (meta del indicador de `id_indicador_alerta_kpi`) |

### 3.4 Configuración de la solución

| Archivo | Qué ajustar |
|---|---|
| `src/config/dominios.json` | Sus dominios de datos oficiales y su descripción (guían a `ai_classify` y definen los valores de la etiqueta de dominio) |
| `src/config/esquema_ficha.json` | Campos a extraer; si la plantilla cambia de versión, agreguen o renombren campos aquí y en `fichas.py` |
| `src/config/mapeo_fuentes.json` | Ubicación real de las tablas de origen (sección 2) |

### 3.5 Desplegar

```bash
databricks bundle validate -t cliente
databricks bundle deploy -t cliente
```

## 4. Primera carga, cargas incrementales y reprocesos

En el destino `cliente` existen estos jobs (los jobs `demo_*` solo existen en el destino `demo`):

| Job | Qué hace | Cuándo |
|---|---|---|
| `configuracion_inicial` | `00_preparar_entorno` → `12`: esquemas, volumen, etiquetas y permisos iniciales | Una vez por entorno |
| `actualizacion_fichas` | Pipeline *Lectura de fichas* → `04` (duplicados, conflictos e incompletas) → `12` | Al llegar fichas al volumen (*file arrival trigger*) |
| `publicacion_catalogo` | `06` → `07` (modo `proponer`) y `08` → `09` → `10` → `12`: catálogo, glosario, **propuestas** de vistas de métricas, reglas de calidad, agentes de Genie, vista de costos y permisos | De lunes a viernes a las 20:00 |
| `publicar_vistas_aprobadas` | `07` (modo `publicar_aprobadas`) → `09` → `12`: publica y certifica, con el YAML exacto aprobado, las vistas de métricas aprobadas por el Data Steward | Después de cada aprobación |
| `calidad_por_carga` | `08`: evalúa las reglas de calidad | Al actualizarse las tablas de `tablas_origen_calidad` (*table update trigger*) |

Todos se despliegan **pausados**. El orden recomendado:

```bash
databricks bundle run -t cliente revision_indicadores       # 0. inicia la app de revisión (una vez por despliegue)
# 0b. el propietario del catálogo otorga los USE CATALOG de la sección 3.2 (grupos y service principal de la app)
databricks bundle run -t cliente configuracion_inicial      # 1. una sola vez
# 2. copien las fichas reales a /Volumes/<mi_catalogo>/indicadores_landing/fichas_indicadores/recibidas/
databricks bundle run -t cliente actualizacion_fichas       # 3. lectura, detección y permisos de la app sobre la cola
# 4. los responsables resuelven la cola en la app de revisión (pestaña Cola de revisión)
databricks bundle run -t cliente publicacion_catalogo       # 5. publicación + propuestas de vistas y reglas
# 6. aprobaciones: Data Owners (pestaña Aprobar indicadores de la app) y Data Steward (vistas y reglas, ver abajo)
databricks bundle run -t cliente publicacion_catalogo       # 7. certifica los indicadores aprobados
databricks bundle run -t cliente publicar_vistas_aprobadas  # 8. vistas de métricas oficiales y certificadas
# 9. activen las alertas: estado_alertas = UNPAUSED y vuelvan a desplegar (antes, las tablas gold no existen)
```

**Aprobación de los Data Owners (paso 6).** Que la IA no encuentre duplicados, conflictos ni faltantes no basta para
certificar: un indicador sin hallazgos queda en estado **PENDIENTE_APROBACION** hasta que su Data Owner lo aprueba en
la pestaña *Aprobar indicadores* de la app (las aprobaciones se registran solo desde la app, con la identidad del usuario). Si después cambia la
ficha, la aprobación deja de valer y el indicador vuelve a pendiente.

**Aprobación del Data Steward (paso 6).** Las propuestas de la IA no se publican solas:

```sql
-- Vistas de métricas: revisar el YAML y la vista <vista>_propuesta, y aprobar una versión
SELECT vista, version, yaml FROM <mi_catalogo>.indicadores_gold.definicion_vistas_de_metricas WHERE estado = 'PROPUESTA';
UPDATE <mi_catalogo>.indicadores_gold.definicion_vistas_de_metricas
SET estado = 'APROBADA', aprobada_por = current_user(), fecha_aprobacion = current_timestamp()
WHERE vista = 'mv_citas_telemarketing' AND version = <version>;

-- Reglas de calidad: revisar (en especial filtro y condicion_valida) y aprobar
SELECT * FROM <mi_catalogo>.indicadores_gold.reglas_calidad WHERE NOT coalesce(aprobada, false);
UPDATE <mi_catalogo>.indicadores_gold.reglas_calidad SET aprobada = true, aprobada_por = current_user() WHERE id_regla = '<id_regla>';
```

**Quién ejecuta cada job:** el service principal de `run_as` en la programación; los Data Stewards (`grupo_stewards`)
pueden lanzar `publicacion_catalogo` y `publicar_vistas_aprobadas` (`CAN_MANAGE_RUN`), y `grupo_operacion` ve todos los
jobs y el pipeline (`CAN_VIEW`).

**Agentes de Genie:** se crean una vez. Con `sobrescribir_genie = no` (por defecto) los ajustes que hagan en la interfaz
de Genie (instrucciones, ejemplos, preguntas de referencia) se conservan; las consultas de ejemplo iniciales usan
indicadores de la demo y conviene reemplazarlas por preguntas reales (en la interfaz o en `ejemplos_catalogo` y
`bench_catalogo` del notebook `09`, en cuyo caso ejecuten una vez con `sobrescribir_genie = si`).

Solo las reglas aprobadas se evalúan, y las vistas oficiales cambian solo cuando se aprueba una nueva versión. El
historial queda en `definicion_vistas_de_metricas`.

Después activen los *triggers* y la programación desde la interfaz de Jobs o cambiando `pause_status` a `UNPAUSED`.

- **Cargas incrementales:** cada ficha nueva se procesa sola. Si un área **reemplaza** su ficha (mismo nombre de archivo
  o nombre nuevo con el mismo ID de indicador), el pipeline toma la versión más reciente; si cambió su contenido, sus
  pares se vuelven a evaluar, aparecen hallazgos nuevos en estado PENDIENTE y el indicador vuelve a **EN_REVISION**
  en la siguiente publicación; sus reglas de calidad se vuelven a extraer.
- **Reprocesos:** para releer todas las fichas (por ejemplo, tras cambiar `esquema_ficha.json`), ejecuten el pipeline con
  *full refresh*. Los hallazgos y decisiones existentes se conservan.

## 5. Validación con datos reales

Antes de abrir la solución a las áreas, les sugerimos validar con una **muestra revisada a mano** (30 a 50 fichas que
incluyan casos conocidos de duplicados y conflictos):

| Qué validar | Cómo | Criterio sugerido |
|---|---|---|
| Lectura | Todas las fichas de la muestra en `silver.fichas_vigentes`, sin `error_extraccion` | 100% |
| Extracción | Comparar 10 campos clave contra el PDF en 20 fichas | ≥ 95% de campos correctos |
| Detección | Cargar la muestra conocida en una tabla con el mismo formato que `respuesta_esperada_fichas_sinteticas` y reutilizar las validaciones 3 y 4 de `11_validaciones` | ≥ 90% de pares conocidos detectados |
| Umbral de similitud | Revisar los pares con similitud cercana a `umbral_similitud`; subirlo si hay muchos falsos positivos, bajarlo si se escapan duplicados | Ajuste acordado con Gobierno de Datos |
| Vista de métricas | Para 3 a 5 indicadores, comparar el valor de la vista contra el reporte vigente del mismo mes (como hace la validación 7 con IND-001) | Diferencia explicada o nula |
| Genie | Ejecutar las preguntas de referencia (*benchmarks*) de cada agente y agregar preguntas reales de las áreas | ≥ 90% de respuestas correctas antes de publicar |

## 6. Paso a producción

- **Entornos:** un destino del bundle por entorno (`dev`, `qa`, `prod`), cada uno con su catálogo o prefijo.
- **CI/CD:** desplieguen desde su pipeline (Azure DevOps o GitHub Actions) autenticados como **el mismo service principal
  configurado en `run_as`** (con apps, dashboards y alertas en el bundle, la identidad que despliega debe ser la misma de
  `run_as`). Recomendamos la [federación de identidades de carga de trabajo (OIDC)](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/auth/provider-github)
  en lugar de un secreto de larga duración; el ejemplo usa OAuth M2M con secreto como alternativa. Ejemplo mínimo (GitHub Actions):

  ```yaml
  jobs:
    deploy:
      runs-on: ubuntu-latest
      env:
        DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
        DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
        DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
      steps:
        - uses: actions/checkout@v4
        - uses: databricks/setup-cli@<version_o_sha_fijado>   # fijen una versión: evita cambios no revisados en el pipeline
        - run: databricks bundle validate -t cliente && databricks bundle deploy -t cliente
  ```

  Ver [CI/CD en Azure Databricks](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/ci-cd/) y
  [autenticación OAuth M2M](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/auth/oauth-m2m).
- **Vistas de métricas en el repositorio:** además del historial en `gold.definicion_vistas_de_metricas`, les
  recomendamos copiar al repositorio el YAML de cada versión aprobada.
- **Programación:** activar `actualizacion_fichas` (llegada de archivos), `calidad_por_carga` (actualización de tablas) y `publicacion_catalogo` (de lunes a viernes); `publicar_vistas_aprobadas` se ejecuta después de cada aprobación.
- **SLA sugeridos:** ficha nueva en la cola de revisión en menos de 1 hora; hallazgos de prioridad alta resueltos en
  `dias_maximos_revision` días; catálogo publicado cada día hábil.
- **Consumo desde BI:** las vistas de métricas se consultan desde el SQL warehouse; revisen la
  [integración de vistas de métricas con herramientas de BI](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/bi-tools)
  para conectar sus tableros actuales a la definición certificada.

## 7. Lista de verificación final

- [ ] Destino `cliente` completo y `bundle validate` sin errores
- [ ] Grupos creados y permisos otorgados (áreas, revisores, consumidores, service principal de la app)
- [ ] Service principal de ejecución con los permisos de la sección 3.2
- [ ] Grupos de la cuenta creados (`grupo_revisores`, `grupo_aprobadores`, `grupo_stewards`, `grupo_operacion`) y `gold.aprobadores_por_dominio` cargada
- [ ] Alertas activadas (`estado_alertas = UNPAUSED`) después de la primera publicación
- [ ] Etiquetas gobernadas existentes revisadas; clave de dominio sin conflicto
- [ ] Modelos y AI Search disponibles en la región del workspace
- [ ] Acceso a PyPI desde serverless o alternativa para `reportlab` (solo necesario para generar las fichas sintéticas de la demo)
- [ ] `dominios.json` con los dominios oficiales aprobados
- [ ] `mapeo_fuentes.json` apuntando a las tablas reales, con comentarios de columna
- [ ] Muestra de validación procesada y criterios de la sección 5 cumplidos
- [ ] Revisores capacitados en la app de revisión
- [ ] Región del workspace compatible (sección 3.1) y Genie habilitado (funciones de IA impulsadas por socios)
- [ ] YAML de las vistas de métricas y reglas de `gold.reglas_calidad` revisados y **aprobados** por los Data Stewards; `publicar_vistas_aprobadas` ejecutado
- [ ] Alerta de frescura apuntando a la tabla real (`tabla_frescura`, `columna_frescura`)
- [ ] Indicadores sin hallazgos aprobados por sus Data Owners (pestaña *Aprobar indicadores*)
- [ ] Columnas con datos personales de las tablas reales etiquetadas o clasificadas (en la demo solo se etiquetan las sintéticas), con máscaras de columna donde aplique
- [ ] `gold.avisos_publicacion` vacío o revisado tras la primera publicación
- [ ] Prueba de humo en su workspace de Azure (la demo se validó en otra nube): `demo_completa` en un catálogo de pruebas con `11_validaciones` en OK
- [ ] Preguntas de referencia de Genie actualizadas con preguntas reales
- [ ] Alertas con correos (usuarios del workspace o destinos de notificación) y umbrales reales
- [ ] Política de uso serverless y presupuesto de cuenta configurados
- [ ] `configuracion_inicial` ejecutado; jobs `actualizacion_fichas`, `calidad_por_carga` y `publicacion_catalogo` activados
