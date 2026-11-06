# Referencias

Documentación oficial de Azure Databricks utilizada en la solución, agrupada por tema.

## Unity Catalog y gobierno

- [Unity Catalog](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/): modelo de gobierno, catálogos, esquemas y permisos.
- [Volúmenes](https://learn.microsoft.com/es-es/azure/databricks/volumes/): almacenamiento gobernado de archivos (las fichas PDF).
- [Archivos en volúmenes](https://learn.microsoft.com/es-es/azure/databricks/volumes/volume-files): lectura y escritura con rutas `/Volumes/...`.
- [Privilegios de Unity Catalog](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/manage-privileges/): `GRANT` a grupos y service principals.
- [Etiquetas](https://learn.microsoft.com/es-es/azure/databricks/database-objects/tags): `SET TAGS` y etiquetas de columna.
- [Certificar y marcar como obsoletos](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/certify-deprecate-data): `system.certification_status`.
- [Etiquetas gobernadas](https://learn.microsoft.com/es-es/azure/databricks/admin/governed-tags/): valores permitidos por clave.
- [Dominios y subdominios](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/domains): organización de activos por área de negocio.
- [Pages (Beta)](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/pages): definiciones de negocio por dominio.
- [COMMENT ON](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-comment): comentarios de tablas y columnas.
- [Filtros de filas y máscaras de columna](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/filters-and-masks/): protección de datos personales.
- [Linaje](https://learn.microsoft.com/es-es/azure/databricks/data-governance/unity-catalog/data-lineage): linaje automático de tablas y columnas.
- [Tabla de auditoría](https://learn.microsoft.com/es-es/azure/databricks/admin/system-tables/audit-logs): `system.access.audit`.

## Capa semántica

- [Vistas de métricas](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/): definición, consulta con `MEASURE()` y consumo.
- [Vistas de métricas en herramientas de BI](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/bi-tools).
- [Referencia YAML de vistas de métricas](https://learn.microsoft.com/es-es/azure/databricks/uc-semantics/metric-views/yaml-reference): versión 1.1, `display_name`, `format`, `synonyms`, `joins`.

## Funciones de IA y modelos

- [ai_parse_document](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_parse_document): análisis de PDF y otros documentos.
- [ai_extract](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_extract): extracción con esquema JSON (versión 2.1).
- [ai_classify](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_classify): clasificación con etiquetas descritas.
- [ai_query](https://learn.microsoft.com/es-es/azure/databricks/sql/language-manual/functions/ai_query): llamadas a modelos con `responseFormat`.
- [Salidas estructuradas](https://learn.microsoft.com/es-es/azure/databricks/machine-learning/model-serving/structured-outputs): respuestas con estructura fija.
- [Foundation Model APIs](https://learn.microsoft.com/es-es/azure/databricks/machine-learning/foundation-model-apis/): modelos servidos por Databricks.
- [Modelos admitidos](https://learn.microsoft.com/es-es/azure/databricks/machine-learning/model-serving/foundation-model-overview): catálogo de modelos.
- [Disponibilidad por región](https://learn.microsoft.com/es-es/azure/databricks/resources/feature-region-support) · [Geografías de Databricks](https://learn.microsoft.com/es-es/azure/databricks/resources/databricks-geos).

## Búsqueda semántica

- [AI Search (antes Vector Search)](https://learn.microsoft.com/es-es/azure/databricks/ai-search/ai-search): índices Delta Sync con embeddings administrados.
- [Crear endpoints e índices](https://learn.microsoft.com/es-es/azure/databricks/ai-search/create-ai-search).

## Ingesta y procesamiento

- [Lakeflow Spark Declarative Pipelines](https://learn.microsoft.com/es-es/azure/databricks/ldp/): tablas de streaming y vistas materializadas.
- [Expectativas](https://learn.microsoft.com/es-es/azure/databricks/ldp/expectations): reglas de calidad en el pipeline.
- [Actualizaciones y full refresh](https://learn.microsoft.com/es-es/azure/databricks/ldp/updates).
- [Auto Loader](https://learn.microsoft.com/es-es/azure/databricks/ingestion/cloud-object-storage/auto-loader/): lectura incremental de archivos.
- [Lakeflow Connect para SQL Server](https://learn.microsoft.com/es-es/azure/databricks/ingestion/lakeflow-connect/sql-server-overview) · [SharePoint](https://learn.microsoft.com/es-es/azure/databricks/ingestion/lakeflow-connect/sharepoint).
- [Lakehouse Federation](https://learn.microsoft.com/es-es/azure/databricks/query-federation/): consulta de fuentes externas sin copiar.
- [Ubicaciones externas](https://learn.microsoft.com/es-es/azure/databricks/connect/unity-catalog/cloud-storage/).

## Orquestación, despliegue y cómputo

- [Lakeflow Jobs](https://learn.microsoft.com/es-es/azure/databricks/jobs/) · [Desencadenadores por actualización de tablas](https://learn.microsoft.com/es-es/azure/databricks/jobs/trigger-table-update) · [Desencadenadores por llegada de archivos](https://learn.microsoft.com/es-es/azure/databricks/jobs/file-arrival-triggers) · [Notificaciones](https://learn.microsoft.com/es-es/azure/databricks/jobs/notifications).
- [Bundles: Declarative Automation Bundles (antes Databricks Asset Bundles)](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/bundles/) · [CI/CD con bundles](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/ci-cd/) · [Recursos de bundles](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/bundles/resources).
- [Databricks CLI](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/cli/install).
- [Cómputo serverless](https://learn.microsoft.com/es-es/azure/databricks/compute/serverless/) · [SQL warehouses](https://learn.microsoft.com/es-es/azure/databricks/compute/sql-warehouse/).

## Aplicaciones y consumo

- [Databricks Apps](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-apps/) · [SQL warehouse como recurso de la app](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/databricks-apps/sql-warehouse).
- [Genie Agents](https://learn.microsoft.com/es-es/azure/databricks/genie-agents/set-up) · [Buenas prácticas](https://learn.microsoft.com/es-es/azure/databricks/genie-agents/best-practices).
- [Dashboards AI/BI](https://learn.microsoft.com/es-es/azure/databricks/dashboards/).
- [Alertas de Databricks SQL](https://learn.microsoft.com/es-es/azure/databricks/sql/user/alerts/) · [Destinos de notificación](https://learn.microsoft.com/es-es/azure/databricks/admin/workspace-settings/notification-destinations).

## Costos y monitoreo

- [Tablas del sistema de facturación](https://learn.microsoft.com/es-es/azure/databricks/admin/system-tables/billing): `system.billing.usage` y `list_prices`.
- [Etiquetas de uso](https://learn.microsoft.com/es-es/azure/databricks/admin/account-settings/usage-detail-tags).
- [Políticas de uso serverless](https://learn.microsoft.com/es-es/azure/databricks/admin/usage/budget-policies) (versión preliminar pública).
- [Presupuestos](https://learn.microsoft.com/es-es/azure/databricks/admin/account-settings/budgets).
