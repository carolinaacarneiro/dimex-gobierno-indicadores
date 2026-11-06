# Gobierno de indicadores de negocio — DIMEX

Hoy cada área documenta sus indicadores en una ficha (plantilla de 6 bloques), y con cerca de 600 fichas es imposible
ver qué indicadores se repiten, cuáles se contradicen y cuál es la definición oficial. Esta solución lee las fichas con
IA dentro de Azure Databricks, detecta duplicados, conflictos y fichas incompletas, los pone en manos de los
responsables para decidir, y publica el resultado en Unity Catalog como **catálogo certificado**, **glosario de negocio**,
**vistas de métricas**, **reglas de calidad ejecutables** y **agentes de Genie** para preguntar en español.

## Contenido del paquete

| Archivo o carpeta | Para qué sirve | Cuándo leerlo |
|---|---|---|
| `LEAME.md` / `guias_pdf/LEAME.pdf` (este archivo) | Qué hay en el paquete y por dónde empezar | Primero |
| `guias_pdf/` y `guias_md/` | Las guías en PDF y Markdown (mismo contenido) | Según la tabla de abajo |
| `01_Guia_de_la_demo` | Reproducir la demo paso a paso y entender cada etapa | Para ver la solución funcionando |
| `02_Arquitectura_y_decisiones` | Arquitectura, decisiones de diseño, buenas prácticas, seguridad | Antes de presentarla a arquitectura y seguridad |
| `03_Guia_de_implementacion_en_su_entorno` | Llevarla a su workspace con sus fichas y tablas reales | Al planear la implementación |
| `04_Gobierno_costos_y_monitoreo` | Etiquetas, costos, alertas, calidad y auditoría | Al operar la solución |
| `05_Solucion_de_problemas_y_FAQ` | Errores conocidos y preguntas frecuentes | Cuando algo no funcione |
| `06_Referencias` | Documentación oficial usada | De consulta |
| `bundle/` | Todo el código como *bundle* (Declarative Automation Bundles, antes Databricks Asset Bundles): `databricks.yml`, `resources/`, `src/`, `app/`, `dashboards/` | Para desplegar |
| `notebooks/` | Los notebooks en formato `.ipynb` y su fuente `.py` (idénticos a los de `bundle/src/`), para leerlos sin desplegar | De consulta |

## Orden recomendado de lectura

1. Este archivo → 2. `01_Guia_de_la_demo` → 3. `02_Arquitectura_y_decisiones` → 4. `03_Guia_de_implementacion_en_su_entorno`
→ 5. `04_Gobierno_costos_y_monitoreo` (y `05` cuando lo necesiten).

## Requisitos previos

> **Importante, región de Azure:** las funciones de IA, AI Search, Databricks Apps y los modelos de pago por token no
> están disponibles en todas las regiones (por ejemplo, hoy no lo están en `mexicocentral`). Antes de desplegar,
> revisen la tabla de regiones y la nota de residencia de datos de la guía `03` (sección 3.1).

- Workspace de Azure Databricks con **Unity Catalog** y **cómputo serverless** (notebooks, jobs y pipelines).
- Un **SQL warehouse serverless**.
- **Funciones de IA**, **Foundation Model APIs**, **AI Search** (antes Vector Search) y **Databricks Apps** disponibles en la región.
- Permisos para crear esquemas en un catálogo, crear jobs, pipelines, apps y endpoints de AI Search.
- **Databricks CLI** reciente con soporte de bundles ([instalación](https://learn.microsoft.com/es-es/azure/databricks/dev-tools/cli/install)).

## Inicio rápido

**Demo con datos sintéticos** (destino `demo`). En `bundle/databricks.yml`, dentro del destino `demo`, reemplacen los
ocho valores marcados con `<...>`: `workspace.host` (se edita en el archivo), `catalogo`, `warehouse_id`,
`correo_alertas` (el correo de un usuario del workspace) y los cuatro grupos. Los cuatro grupos deben ser **grupos de la cuenta** (no grupos locales del workspace); para una demo pueden ser el
mismo grupo: `grupo_revisores` (consumidores), `grupo_aprobadores` (usan la app de revisión), `grupo_stewards`
(aprueban vistas y reglas) y `grupo_operacion` (costos y auditoría).
Todos, salvo el host, también pueden pasarse con `--var` (usen las mismas variables en `validate`, `deploy` y `run`):

```bash
cd bundle
databricks bundle validate -t demo
databricks bundle deploy -t demo
databricks bundle run -t demo demo_completa          # crea todo desde cero
databricks bundle run -t demo revision_indicadores   # inicia la app de revisión
```

La ejecución completa desde cero tarda entre 15 y 40 minutos (la primera vez se crea el endpoint de AI Search). Al
terminar, el notebook `11_validaciones` verifica 10 criterios y deja el resultado en `gold.resultado_validaciones`.

**Su entorno con datos reales** (destino `cliente`): descomenten el destino `cliente` al final de `databricks.yml`,
completen sus valores y sigan la `03_Guia_de_implementacion_en_su_entorno` (permisos, mapeo de fuentes y orden de los
jobs).

La demo se construyó y validó de punta a punta en cómputo serverless de Databricks sobre otra nube; el bundle no usa
servicios específicos de la nube. Antes de llevarla a producción en Azure, revisen la región (guía 03, sección 3.1) y
ejecuten una prueba de humo en su workspace (destino `demo` en un catálogo de pruebas, con `11_validaciones` en OK).
