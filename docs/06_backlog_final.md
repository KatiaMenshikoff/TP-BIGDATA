# Backlog final priorizado (Entrega 2 → MVP final del 07/12/2026)

Consigna, Secciones 6.2 y 6.6: el trabajo restante se ordena en **obligatorio / deseable / fuera de alcance**. Lo deseable sólo se encara con lo obligatorio cerrado; nada de lo deseable puede poner en riesgo la integración end-to-end.

## Obligatorio (requisitos de la evaluación final)

| ID | Ítem | Requisito (consigna) | Responsable | Esfuerzo (h) | Criterio de terminado |
|---|---|---|---|---|---|
| B-01 | Correr el pipeline completo contra **AstraDB** real y guardar capturas | 6.4 / 7.6 | Francisco Gomes | 3 | `evidence/final/serving/` con las 5 consultas en AstraDB |
| B-02 | Gold `revenue_by_org_month` desde `silver/fact_billing` + tabla Q4 `((org_id), month)` | 7.3, 7.4 Q4 | Julián A. Szarfmeser | 4 | CQL + captura; total USD = suma de `fact_billing` |
| B-03 | Silver `fact_tickets` (Q06 CSAT fuera de rango) + Gold `tickets_by_org_date` + tabla Q3 `((severity), date)` | 7.3, 7.4 Q3 | Julián A. Szarfmeser | 5 | CQL + captura; 40 CSAT a nulo con flag |
| B-04 | Gold `genai_tokens_by_org_date` + tabla Q5 `((org_id), date)` | 7.3, 7.4 Q5 | Iván Josephsohn | 3 | CQL + captura; tokens = 3.117 eventos con valor |
| B-05 | Tabla Cassandra para `cost_anomaly_mart` (consulta de anomalías por org) | 7.2.6 | Francisco Gomes | 2 | CQL + captura |
| B-06 | Notebook de Colab que ejecute todo el pipeline (Drive como lake) y probar el Quickstart en Colab | 8.2 | Iván Josephsohn | 3 | Notebook ejecutado con salidas |
| B-07 | Diccionario de datos completo de Silver y Gold | 7.5 | Katia Menshikoff | 3 | `docs/` con todas las columnas, tipos y linaje |
| B-08 | Presentación ejecutiva, video y guion de defensa con participación de los 4 | 7.5 | Todo el equipo | 10 | Artefactos en el canal de entrega |
| B-09 | Validar el Quickstart en un entorno limpio (otra máquina) y medir O7 | 8.2 | Katia Menshikoff | 2 | Tiempo medido en el README |

## Deseable

| ID | Ítem | Valor | Riesgo |
|---|---|---|---|
| D-01 | Delta Lake para Silver (`MERGE` por `event_id`, `OPTIMIZE`) | Escala la deduplicación sin estado de 62 días (D-05) | Dependencia nueva atada a Spark 4 |
| D-02 | Reinyección automática de `quarantine/late_events` a Bronze | Cierra el ciclo de late data sin intervención | Bajo (hoy hay 0 tardíos con 62 días) |
| D-03 | IsolationForest como segundo detector y comparación con MAD | Detecta anomalías multivariadas | Sin etiquetas, difícil de evaluar |
| D-04 | Compactación de Bronze (D-14) | Menos archivos chicos | Reescribe la capa de reproceso |
| D-05 | Dashboard (Grafana/Superset) sobre Cassandra | Consumo visual | Tiempo de configuración |
| D-06 | Trigger `processingTime="1 minute"` con llegada simulada de archivos para medir O1 en continuo | Evidencia de frescura más realista | Bajo |

## Fuera de alcance

| Ítem | Motivo |
|---|---|
| Orquestación con Airflow | El orquestador propio (`cpa.pipeline`) con etapas idempotentes alcanza para el MVP |
| Kafka como fuente del stream | La consigna define el directorio JSONL como fuente |
| Despliegue en un cluster (Dataproc/EMR) | El volumen de la muestra corre en `local[*]`; el código no cambia |
| Control de acceso por usuario en Cassandra (roles RBAC) | Se documenta el modelo de acceso; implementarlo exige un cluster con autenticación |
