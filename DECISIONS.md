# Registro de decisiones (ADR ligero)

Formato: contexto → decisión → **trade-offs frente a las alternativas** → consecuencias → evidencia.
Estado: ✅ aceptada · 🟡 abierta · 🔁 reemplazada. Versión 2.0 (Entrega 2, 09/11/2026).

**Respuesta al feedback de la E1** ("los trade-offs podrían explicitarse todavía un poco más frente a algunas alternativas"): desde la v2.0 cada decisión tiene una tabla con lo que **ganaríamos** y lo que **perderíamos** con cada alternativa, y la **señal de reversión**, es decir, el dato observable que nos haría cambiar la decisión. Las decisiones abiertas de la E1 (D-05, D-06, D-08 y A1–A5) quedan cerradas con evidencia de ejecución.

| ID | Fecha | Decisión | Estado |
|---|---|---|---|
| D-01 | 2026-10 | Patrón **Lambda** con lógica de transformación compartida | ✅ |
| D-02 | 2026-10 | Parquet como formato en Bronze/Silver/Gold; Landing intacta (A3) | ✅ |
| D-03 | 2026-10 | Particionar sólo por fecha (`event_date`, `usage_date`, `month`, `ingest_date`) | ✅ |
| D-04 | 2026-10 | Esquemas explícitos; `value` se lee como string y se castea con `try_cast` | ✅ |
| D-05 | 2026-11 | Watermark de 62 días + reconciliación Landing vs Bronze + recomputo de Silver/Gold por fecha | ✅ (cerrada en E2) |
| D-06 | 2026-11 | Revenue USD = `(subtotal − credits + taxes) × exchange_rate_to_usd`, FX tal cual + flag Q08 (A1) | ✅ (cerrada en E2) |
| D-07 | 2026-10 | `spark.sql.session.timeZone = UTC` obligatorio | ✅ |
| D-08 | 2026-11 | Anomalías de costo con z robusto (MAD) por servicio+métrica, umbral 3,5 (A4) | ✅ (cerrada en E2) |
| D-09 | 2026-10 | Serving con una tabla Cassandra por consulta (*query-first*) | ✅ |
| D-10 | 2026-10 | `email` se hashea en Silver y no llega a Gold ni a Cassandra | ✅ |
| D-11 | 2026-11 | Cassandra 5 en Docker como destino por defecto; AstraDB por variables de entorno, mismo código | ✅ |
| D-12 | 2026-11 | Carga a Cassandra con `cassandra-driver` (dentro de `foreachBatch` y en la etapa `serving`) (A5) | ✅ |
| D-13 | 2026-11 | Dos queries de streaming encadenadas (Landing→Bronze y Bronze→Silver/Gold) con checkpoint propio | ✅ |
| D-14 | 2026-11 | Silver compacta los *small files* de Bronze (1 archivo por fecha) | ✅ |
| D-15 | 2026-11 | Parseo tolerante (`try_to_timestamp`, `try_cast`) con ANSI activo: un dato malo va a quarantine, no corta el stream | ✅ |

---

### D-01 · Patrón Lambda
- **Contexto:** los eventos de uso son un flujo continuo; maestros, facturación y NPS son lotes chicos (≤ 1.500 filas) con cadencia diaria o mensual.
- **Decisión:** streaming para `usage_events_stream`, batch para el resto. Las funciones de Silver/Gold (`src/cpa/transforms.py`) se escriben una sola vez y se reutilizan en `foreachBatch` y en el backfill batch (`--stages backfill`).

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Lambda con lógica compartida (elegida)** | Cada fuente con la cadencia que le corresponde; batch simple para 240 facturas | Dos rutas de ingesta para operar | — |
| Kappa (todo como stream) | Una sola ruta y un solo modelo mental | Convertir 7 CSV estáticos en streams sin beneficio; reprocesar = re-stream completo | Que los maestros pasen a llegar como CDC continuo (Debezium/Kafka) |
| Batch puro | La operación más simple | Incumple O1 (≤ 5 min) y el requisito invariable de la consigna | Ninguna: la consigna exige streaming |

- **Consecuencias:** la desventaja clásica de Lambda (dos códigos) se mitiga porque batch y streaming llaman a las mismas funciones. Evidencia: `rebuild_all_dates` (batch) y `process_event_dates` (`foreachBatch`) producen exactamente las mismas 42.989 / 11.050 filas (`evidence/e2/idempotencia/resumen.md`, corridas 1 y 3).

### D-02 · Parquet sin formato de tabla (Delta/Iceberg) (cierra A3)
- **Decisión:** Parquet puro gestionado por Spark en todas las zonas, como pide la consigna.

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Parquet puro (elegida)** | Cero dependencias extra; corre igual en local y en Colab | Sin ACID ni `MERGE`: la idempotencia se resuelve con overwrite dinámico por partición | — |
| Delta Lake | `MERGE` por `event_id`, *time travel*, compactación (`OPTIMIZE`) | Una dependencia más (versión de Delta atada a Spark 4) y otra pieza para depurar en la demo | Volumen que haga inviable reescribir particiones completas, o necesidad de auditoría por versión |
| Iceberg | Lo mismo que Delta + evolución de particiones | Requiere catálogo; más configuración | Ídem |

- **Consecuencias:** sin transacciones, un fallo a mitad de un overwrite puede dejar una partición incompleta; se mitiga porque toda partición es reconstruible desde Bronze (y Bronze desde Landing). Delta queda en el backlog como **deseable**.

### D-03 · Particionado por fecha
- **Contexto:** con ~720 eventos/día, particionar por `service` u `org_id` generaría miles de archivos de pocos KB.
- **Decisión:** una partición por fecha y un archivo por partición (`repartition(col_fecha)` antes de escribir).

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Sólo fecha (elegida)** | 60 particiones de ~63 KB en Silver; poda por rango de fechas | Filtrar por org recorre todas las fechas del rango | — |
| Fecha + servicio | Poda por servicio | ×6 archivos (360 de ~10 KB) | Particiones diarias > 1 GB |
| Fecha + org | Poda por org | ×80 archivos (4.800 de < 1 KB) | Nunca a esta escala: el acceso por org lo da Cassandra |

- **Evidencia:** `evidence/e2/lake_inventory.md` (rutas, particiones, archivos y KB por archivo de cada tabla).

### D-04 · Esquemas explícitos
- **Decisión:** `StructType` explícito para las 8 fuentes (`src/cpa/schemas.py`); nunca `inferSchema`. `value` se lee como string y se castea en Silver con `try_cast`; los CSV usan `mode=PERMISSIVE` + `_corrupt_record`.

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Explícito (elegida)** | Tipos estables entre corridas; las filas corruptas se capturan en vez de cambiar el esquema | Mantener el esquema a mano cuando cambia la fuente | — |
| `inferSchema` | Nada que escribir | Un `value` texto convierte toda la columna a string; doble lectura del archivo | — |
| Schema registry | Versionado formal del contrato | Infraestructura extra (fuera de alcance) | Más productores y versiones de esquema |

- **Lección de la E2:** los CSV escapan comillas duplicándolas (`"[""env:prod""]"`). Con el `escape` por defecto de Spark (`\`), 211 de 400 recursos iban a quarantine como corruptos. Se agregó `escape='"'`; la quarantine de Bronze lo hizo visible en la primera corrida.

### D-05 · Late data y watermark (cerrada en E2)
- **Contexto:** los 120 archivos traen eventos de los 60 días. En la E1 lo simulamos; en la E2 lo medimos con Spark real.
- **Decisión:** el stream a Bronze usa `withWatermark("event_ts", "62 days")` + `dropDuplicatesWithinWatermark(["event_id"])`. Una etapa batch (`reconcile`) hace el balance `Landing = Bronze + duplicados + tardíos + sin_clave` y manda los tardíos a `quarantine/late_events` con su archivo de origen. Silver y Gold se reconstruyen por fecha afectada.
- **Evidencia (Spark real, `scripts/demo_late_data.sh` → `evidence/e2/late_data/resumen.md`):**

| Watermark | Llegan a Bronze | Descartados → `late_events` | % | Balance |
|---|---|---|---|---|
| 1 día | 7.817 | 35.383 | 81,9 % | cierra |
| 7 días | 11.433 | 31.767 | 73,5 % | cierra |
| 30 días | 25.294 | 17.906 | 41,4 % | cierra |
| **62 días** | **43.200** | **0** | **0 %** | cierra |

  El conteo de la reconciliación coincide exactamente con el contador interno de Spark `numRowsDroppedByWatermark`. Los porcentajes son menores que en la simulación de la E1 (97,5 % con 1 día) porque la simulación usaba 1 archivo por trigger y acá son 10: el primer micro-batch entero pasa antes de que exista un watermark.

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Watermark 62 d + reconciliación (elegida)** | 0 % de pérdida; estado acotado (43.200 claves, unos pocos MB); ningún tardío se pierde en silencio | El estado crece con el volumen × 62 días | Estado del dedup > ~1 GB o > 10 M de claves |
| Watermark corto (1–30 d) | Estado chico | Pierde entre 41 % y 82 % de los eventos en Bronze (medido) | Si la fuente pasara a llegar ordenada |
| Sin watermark | Nunca descarta | Estado sin límite: crece para siempre | — |
| Dedup por `MERGE` en Silver (Delta) | Escala sin estado de streaming | Dependencia Delta (D-02); Bronze tendría duplicados | La misma señal de estado; es la opción para producción |

### D-06 · Normalización de facturación a USD (cerrada en E2, cierra A1)
- **Contexto:** 144 de las 160 facturas en USD tienen `exchange_rate_to_usd` fuera de [0,99; 1,01]. 50 orgs facturan en más de una moneda.
- **Decisión:** se aplica el FX informado tal cual (es lo que la fuente declara) y la regla Q08 marca los USD sospechosos con `flag_fx_suspicious`. El cálculo vive en una sola función (`conform_billing`) y la tolerancia en `config/`. Implementado en `silver/fact_billing` (240 facturas: Q08 = 144, Q09 notas de crédito = 13, Q10 créditos imputados = 137).

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **FX tal cual + flag (elegida)** | Reproducible y auditable contra la fuente; no inventa datos | Si el FX de USD es ruido, el revenue USD tiene un error de hasta ±12 % en esas facturas | Que el negocio confirme que USD debe valer 1,0 |
| Forzar 1,0 para USD | Revenue "limpio" para USD | Contradice la fuente sin evidencia | Ídem (sería un cambio de una línea) |
| Tabla FX externa por fecha | FX "real" | Una fuente nueva que no está en el caso | Que se provea una tabla FX oficial |

### D-07 · Zona horaria UTC
- **Contexto:** al ejecutar Spark con la zona local (UTC−3), `to_date(timestamp)` corrió eventos al día anterior: 12.152 claves en lugar de 11.050.
- **Decisión:** `spark.sql.session.timeZone=UTC` en todas las sesiones (`config/pipeline.example.yaml`, `common.spark_session`). La alternativa (zona local por usuario) no tiene ventajas para datos que ya vienen en UTC.
- **Evidencia:** Gold E2 vs MapReduce E1: 11.050 claves en ambos, 0 claves de diferencia (`evidence/e2/gold_vs_mapreduce.json`).

### D-08 · Anomalías de costo (cerrada en E2, cierra A4)
- **Decisión:** z robusto `0,6745·(x − mediana)/MAD` por servicio+métrica sobre Silver, umbral |z| > 3,5, agregado a `gold/cost_anomaly_mart` (org × fecha × servicio). Si MAD = 0, z = 0.
- **Resultado:** 191 eventos anómalos (0,44 %) en 190 filas org×día×servicio de 69 orgs; concentran el 7,5 % del costo (`evidence/e2/analitica/metricas.json`).
- **Trazabilidad con la E1:** el perfil marcó 312 outliers sobre los datos crudos; 123 eran costos < −0,01 que ahora Q02 manda a quarantine antes de Silver. Sin ellos se recalculan mediana y MAD y quedan 191.

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **z robusto MAD por servicio+métrica (elegida)** | Sin entrenamiento; explicable en una línea; robusto a los mismos outliers que busca | Univariado: no ve combinaciones raras (costo normal con requests anómalos) | Que FinOps reporte falsos positivos/negativos sistemáticos |
| z robusto global | Más simple | Marca 10.956 eventos (medido en E1): inútil | — |
| Percentil p99 por grupo | Simple | Marca siempre el 1 % aunque no haya anomalías | — |
| IsolationForest (MLlib/sklearn) | Multivariado | Entrenamiento, hiperparámetros y menos explicable | Etiquetas de anomalías reales para medir precisión |

### D-09 · Serving *query-first*
- **Decisión:** una tabla por consulta; la clave de partición es exactamente el filtro de igualdad de la consulta y la clave de *clustering* el orden pedido (`infra/cql/01_schema.cql`).

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Tabla por consulta (elegida)** | Cada consulta lee una sola partición (O6), ya ordenada | Datos duplicados entre tablas; hay que mantener varias cargas | — |
| Una tabla genérica + índices secundarios / `ALLOW FILTERING` | Un solo modelo | Lecturas multi-partición: no escala | — |
| Q2 calculado en el cliente desde la tabla Q1 | Una tabla menos | Leer 14 días × servicios y ordenar en el cliente | Si el usuario necesitara ventanas arbitrarias (no fijas de 14 días) |

### D-10 · PII
- **Decisión:** `email` → `email_hash` (SHA-256 sobre el email normalizado) en `silver/dim_user`; la columna original no sale de Bronze. Bronze sólo es accesible para el rol de ingeniería (ver gobierno en el documento de la E2).
- **Alternativa descartada:** cifrado reversible (exige gestionar claves y no hay caso de uso que necesite el email).

### D-11 · Cassandra local en Docker; AstraDB por configuración (nueva)
- **Contexto:** riesgo K4 (credenciales o conectividad de AstraDB fallan en la demo).
- **Decisión:** `infra/docker-compose.yml` levanta Cassandra 5 y es el destino por defecto. Si existen `ASTRA_DB_SECURE_BUNDLE_PATH` y `ASTRA_DB_TOKEN` en `.env`, el mismo código (`src/cpa/serving.py`) escribe en AstraDB.

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Docker por defecto + AstraDB opcional (elegida)** | Demo reproducible sin cuenta ni red; el docente puede correrla | Hay que tener Docker; en Colab no hay Docker | — |
| Sólo AstraDB | Sin infraestructura local; es lo que nombra la consigna | Dependencia de red y credenciales en la demo | Si la cátedra exige AstraDB: sólo cambia `.env` |

### D-12 · Carga con `cassandra-driver` (nueva, cierra A5)
- **Decisión:** `INSERT` preparados con `execute_concurrent_with_args` (concurrencia 64), dentro de `foreachBatch` (frescura) y en la etapa `serving` (carga completa + Q2). Q2 borra y reescribe cada partición `(org_id, as_of_date)`.

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Driver Python (elegida)** | Funciona con Spark 4 y Python 3.13 sin JARs; igual para local y AstraDB | Los datos pasan por el driver de Spark (11.050 filas en ~1 s) | Gold diario > ~1 M filas |
| Spark Cassandra Connector | Escritura distribuida desde los ejecutores | JAR extra atado a la versión de Spark/Scala (no lo validamos con PySpark 4.0.1); más configuración en Colab | Gold diario > ~1 M filas, con el conector validado en nuestra versión de Spark |

### D-13 · Dos queries de streaming encadenadas (nueva)
- **Decisión:** query 1 Landing → Bronze (file sink, *exactly-once*); query 2 Bronze → `foreachBatch` → Silver/Gold/Cassandra. Cada una con su checkpoint (`_checkpoints/bronze_usage_events`, `_checkpoints/silver_gold_usage`).

| Alternativa | Ganaríamos | Perderíamos | Señal de reversión |
|---|---|---|---|
| **Dos queries (elegida)** | Bronze queda con garantía *exactly-once* del file sink; Silver/Gold se pueden reprocesar sin re-ingerir | Un salto más de latencia (medido: ~1 s por micro-batch en Bronze, ~6 s en Silver+Gold) | O1 en riesgo (> 5 min) |
| Una query con todo en `foreachBatch` | Menos latencia | `foreachBatch` es *at-least-once*: un reintento podía duplicar Bronze | — |

### D-14 · Compactación en Silver (nueva)
- **Contexto:** como cada micro-batch trae eventos de las 60 fechas, Bronze queda con 720 archivos de ~10 KB (12 micro-batches × 60 fechas).
- **Decisión:** aceptar *small files* en Bronze (es append de streaming) y compactar en Silver: cada reconstrucción escribe 1 archivo por fecha (60 archivos de ~63 KB).
- **Trade-off:** compactar Bronze requeriría un job de mantenimiento que reescribe datos crudos (riesgo sobre la capa de reproceso) a cambio de leer Bronze más rápido. Señal de reversión: lecturas de Bronze dominando la duración del `foreachBatch`.

### D-15 · Parseo tolerante con ANSI activo (nueva)
- **Contexto:** Spark 4 activa ANSI por defecto: `to_timestamp('no-es-fecha')` lanza excepción y corta el stream. Lo detectó un test (`test_reglas_bloqueantes_van_a_quarantine_con_rule_id[Q00]`).
- **Decisión:** `try_to_timestamp` y `try_cast`; el registro queda con nulo y la regla Q00/Q04 lo manda a quarantine o lo marca.
- **Trade-off:** desactivar ANSI resolvería lo mismo de forma global, pero escondería errores de tipos en el resto del código.
