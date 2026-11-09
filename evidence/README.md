# Evidencias

Todo se regenera con scripts: no hay pasos manuales.

## Entrega 2 · `e2/`

Regenerar: `PYTHONPATH=src python -m cpa.evidence` (lee el lake y Cassandra), más las dos demos.

| Carpeta / archivo | Qué demuestra (consigna 6.2 / 6.3) | Cómo se regenera |
|---|---|---|
| `e2/lake_inventory.md` · `.json` | Rutas, filas, particiones, archivos, tamaños y claves duplicadas de cada tabla del lake + filas en Cassandra | `python -m cpa.evidence` |
| `e2/esquemas/*.txt` | `printSchema` real de cada tabla (tipificación explícita, columnas técnicas, linaje) | idem |
| `e2/gold_vs_mapreduce.json` | Gold = MapReduce de la E1 (11.050 claves, 0 diferencias) | idem |
| `e2/quarantine/resumen.json` · `*_muestra.csv` | Conteos por regla y muestras verificables de quarantine | idem |
| `e2/muestras/` | Correcciones Q03/Q04 en Silver, conteos v1/v2, SCD2 de `dim_org`, top 20 de Gold | idem |
| `e2/analitica/` | Métricas del detector de anomalías y top 15 | idem |
| `e2/streaming_bronze_progress.json` | *Query progress* por micro-batch: filas, watermark, estado, descartados, duración | idem |
| `e2/reconciliation.json` | Balance Landing = Bronze + duplicados + tardíos + sin clave | idem |
| `e2/run_log.csv` | `_metadata/run_log`: una fila por corrida × etapa × tabla con conteos, estado, duración y git SHA | idem |
| `e2/serving/cqlsh_consultas.txt` · `describe_keyspace.txt` | CQL de Q1 y Q2 con su salida en `cqlsh` y el esquema del keyspace | idem (con `cpa-cassandra` corriendo) |
| `e2/idempotencia/` | 3 corridas (normal, re-ejecución, backfill) con conteos antes/después y logs | `scripts/demo_idempotencia.sh` |
| `e2/late_data/` | Watermark de 1, 7, 30 y 62 días con Spark real: descartados y balance | `scripts/demo_late_data.sh` |
| `tests_output.txt` | Resultado de `pytest` (24 pruebas) | `pytest -q tests > evidence/tests_output.txt` |

## Entrega 1

| Carpeta / archivo | Qué demuestra | Cómo se regenera |
|---|---|---|
| `profile/profile_report.md` | Lectura y perfil de las 8 fuentes (nulos, tipos, rangos, hallazgos) | `python src/profiling/profile_landing.py` |
| `profile/profile_summary.json` | Las mismas métricas en formato máquina | idem |
| `profile/landing_manifest.sha256` | Huella de Landing (inmutabilidad) | idem |
| `mapreduce/mapreduce_stats.json` | Contadores map/combine/shuffle/reduce | `python src/mapreduce/daily_usage_mapreduce.py` |
| `mapreduce/org_daily_usage_by_service_mr.csv` | Salida del flujo MapReduce (11.050 filas) | idem |
| `mapreduce/org_daily_usage_by_service_spark/` | Salida equivalente en PySpark | `python src/mapreduce/daily_usage_spark.py` |
| `mapreduce/spark_physical_plan.txt` | Plan físico: partial_sum → Exchange → sum | idem |
