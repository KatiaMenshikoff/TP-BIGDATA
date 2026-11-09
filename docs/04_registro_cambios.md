# Registro de cambios

La versión 1.1 surge de una revisión interna del equipo; la 2.0 incorpora el feedback del docente sobre la Entrega 1.

## Versión 1.1 (05/10)

Revisión interna del equipo. No cambia el alcance ni las decisiones de arquitectura.

### Correcciones técnicas

| Ubicación | Cambio | Motivo |
| --- | --- | --- |
| Documento de diseño, Sección 10.4 (tabla Q2) | Se agrega `as_of_date` a la partición. La partición se borra y se vuelve a escribir en cada corrida. | En Cassandra las columnas de la clave no se actualizan. Un cambio de costo generaría filas duplicadas. |
| Documento de diseño, Sección 8.3 (punto 3) | Se aclara que cada partición de Silver se reconstruye con todos los eventos de Bronze de esa fecha. | El texto no decía con qué datos se reescribía la partición. Leído literalmente, se perderían los eventos anteriores. |
| Documento de diseño, Sección 7.1 (promoción de Silver a Gold) | Gold se recalcula en cada micro-batch y el dato no es definitivo hasta superar el watermark. | Con un watermark de 62 días y 60 días de datos, ninguna fecha se cerraría y Gold no se publicaría. |

### Ajustes de documentación

| Ubicación | Cambio |
| --- | --- |
| Documento de diseño, Sección 1.2 | El usuario "Ingeniería de datos (autora)" pasa a "Ingeniería de datos". |
| Documento de diseño, Sección 7.3 | "prefijo fijo" pasa a "nombres fijos". |
| Documento de diseño, Sección 11.1 | Los roles se reparten entre los cuatro integrantes. El rol de streaming y serving se divide en dos. |
| Documento de diseño, Sección 11.3 | El repositorio figura como público. |
| README | Se lista a los integrantes del equipo. |
| Documento de diseño, encabezado y portada | La versión pasa a 1.1 y la portada lista a los cuatro integrantes. |
| Documento de diseño, Sección 8.3 (punto 3) | Se completa la última oración ("…por sí solo."). |
| Documento de diseño, Sección 11.2 | Las horas se aclaran como totales del equipo (unas 3–4 h por persona por semana); se quita la referencia a "una sola persona". |
| Plan de correcciones | Se corrige el apellido "Josephsohn". |

### Pendiente (resuelto en la versión 2.0)

Escuchar y corregir las correcciones del docente.
## Versión 2.0 (16/11) · Entrega 2

Incorpora el feedback de la Entrega 1 (plan en `docs/03_plan_correcciones.md`) y pasa del diseño al pipeline ejecutable.

### Respuesta al feedback del docente

| Feedback | Cambio | Ubicación |
| --- | --- | --- |
| Explicitar más los trade-offs frente a las alternativas | Tabla de trade-offs (ganaríamos / perderíamos / señal de reversión) en cada decisión; resumen de las 5 decisiones principales | `DECISIONS.md` v2.0; documento E2, Sección 12 |
| Conservar la trazabilidad en la E2 | Linaje por fila y por corrida, balance Landing vs Bronze, Gold comparado contra el MapReduce de la E1, matriz requisito → evidencia | Documento E2, Secciones 10 y 13 |

### Nuevo

| Artefacto | Contenido |
| --- | --- |
| `src/cpa/` | Pipeline: `pipeline.py` (7 etapas), `transforms.py` (lógica compartida batch/streaming), `schemas.py`, `serving.py`, `common.py` (config, run_log), `evidence.py` |
| `infra/` | `docker-compose.yml` (Cassandra 5) y `cql/` (esquema y consultas) |
| `scripts/` | `run_pipeline.sh`, `reset.sh`, `demo_idempotencia.sh`, `demo_late_data.sh` |
| `tests/` | 17 tests de transformaciones y 1 de integración del stream (24 en total con los de la E1) |
| `evidence/e2/` | Inventario del lake, esquemas, quarantine, streaming, late data, idempotencia, anomalías, serving, run_log |
| `docs/` | `05_documento_entrega2.md` (+ PDF), `06_backlog_final.md`, `diagramas/arquitectura_v2` y `flujo_streaming_v2` |

### Decisiones

- Cerradas con evidencia: D-05 (watermark), D-06 (FX), D-08 (anomalías) y las abiertas A1–A5.
- Nuevas: D-11 (Cassandra en Docker / AstraDB por `.env`), D-12 (driver Python), D-13 (dos queries de streaming), D-14 (compactación en Silver), D-15 (parseo tolerante con ANSI).

### Cambios respecto del diseño v1

Ver documento E2, Sección 1.2. El documento de diseño v1.1 se conserva sin cambios como línea de base.
