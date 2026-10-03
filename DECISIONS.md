# Registro de decisiones (ADR ligero)

Formato: contexto → decisión → alternativas → consecuencias. Estado: ✅ aceptada · 🟡 abierta · 🔁 reemplazada.

| ID | Fecha | Decisión | Estado |
|---|---|---|---|
| D-01 | 2026-10 | Patrón **Lambda** con lógica de transformación compartida | ✅ |
| D-02 | 2026-10 | Parquet como formato en Bronze/Silver/Gold; Landing intacta | ✅ |
| D-03 | 2026-10 | Particionar sólo por fecha (`event_date`, `usage_date`, `month`, `ingest_date`) | ✅ |
| D-04 | 2026-10 | Esquemas explícitos; `value` se lee como string y se castea con `try_cast` | ✅ |
| D-05 | 2026-10 | Watermark de 62 días + quarantine de tardíos + recomputo de Gold por fecha afectada | 🟡 |
| D-06 | 2026-10 | Revenue USD = `(subtotal − coalesce(credits,0) + taxes) × exchange_rate_to_usd` | 🟡 |
| D-07 | 2026-10 | `spark.sql.session.timeZone = UTC` obligatorio | ✅ |
| D-08 | 2026-10 | Anomalías de costo con z robusto (MAD) por servicio+métrica, umbral 3,5 | 🟡 |
| D-09 | 2026-10 | Serving con una tabla Cassandra por consulta (*query-first*) | ✅ |
| D-10 | 2026-10 | `email` se hashea en Silver y no llega a Gold ni a Cassandra | ✅ |

---

### D-01 · Patrón Lambda
- **Contexto:** los eventos de uso son un flujo continuo; maestros, facturación y NPS son lotes chicos (≤ 1.500 filas) con cadencia diaria o mensual.
- **Decisión:** streaming para `usage_events_stream`, batch para el resto. Las funciones de Silver/Gold se escriben una sola vez y se reutilizan en `foreachBatch` y en el batch.
- **Alternativas:** Kappa (descartada: convertir CSV estáticos en streams no aporta nada), batch puro (no cumple O1 ni el requisito invariable).
- **Consecuencias:** hay dos rutas de ingesta, pero una sola lógica de negocio y un único serving con upserts.

### D-03 · Particionado por fecha
- **Contexto:** con ~720 eventos/día, particionar por `service` u `org_id` generaría miles de archivos de pocos KB.
- **Decisión:** una partición por fecha y `coalesce(1)` dentro de cada partición en la muestra.
- **Consecuencias:** las consultas por rango de fechas aprovechan la poda de particiones. Para filtrar por organización, el acceso rápido lo da Cassandra (partición por `org_id`).

### D-05 · Late data y watermark (abierta)
- **Contexto:** los 120 archivos traen eventos de los 60 días. Simulando 1 archivo por trigger, un watermark de 1 día marca como tardío el 97,5 % de los eventos y uno de 30 días, el 49,6 % (`docs/01_documento_diseno.md`, Sección 8.3).
- **Decisión propuesta:** el stream a Bronze usa watermark de 62 días + `dropDuplicatesWithinWatermark(event_id)`: 0 % de pérdida con estos datos y un estado de ~43 k claves. Un batch diario reconcilia las líneas por `source_file` (Landing vs Bronze) y manda los tardíos descartados a `quarantine/late_events`. Gold se recalcula sólo para las fechas afectadas.
- **Alternativas:** merge por `event_id` sobre una tabla Delta/Iceberg (más escalable, agrega una dependencia), sin watermark (estado sin límite).
- **A validar** con el docente en la revisión de la Entrega 1.

### D-06 · Normalización de facturación a USD (abierta)
- **Contexto:** hay facturas en USD con `exchange_rate_to_usd` distinto de 1 (0,855–1,118), y 50 orgs facturan en más de una moneda entre meses.
- **Decisión propuesta:** aplicar el FX informado tal cual (es lo que la fuente declara) y marcar con la regla Q08 los USD con FX fuera de [0,99; 1,01]. El FX queda parametrizable en `config/`.
- **Consecuencias:** el revenue USD es reproducible y auditable; si el docente define otra semántica, se cambia en un solo lugar.

### D-07 · Zona horaria UTC
- **Contexto:** al ejecutar Spark con la zona local (America/Argentina/Buenos_Aires, UTC−3), `to_date(timestamp)` corrió eventos al día anterior. El resultado tenía 12.152 claves org×día×servicio en lugar de 11.050.
- **Decisión:** fijar `spark.sql.session.timeZone=UTC` en todas las sesiones (config central).
- **Evidencia:** comparación MapReduce vs Spark en `evidence/mapreduce/`.

### D-08 · Anomalías de costo
- **Contexto:** la distribución de costo cambia mucho por servicio y métrica (genai p50 = 2,04 vs networking p50 = 0,19). Un z robusto global (sin distinguir servicio) marcaba 10.956 eventos como outliers, lo cual no sirve.
- **Decisión propuesta:** z robusto `0,6745·(x − mediana)/MAD` calculado por servicio+métrica, con umbral |z| > 3,5 (312 eventos, 0,7 %). En Gold se agrega a nivel org×día×servicio.
