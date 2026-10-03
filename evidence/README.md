# Evidencias · Entrega 1

| Carpeta / archivo | Qué demuestra | Cómo se regenera |
|---|---|---|
| `profile/profile_report.md` | Lectura y perfil de las 8 fuentes (nulos, tipos, rangos, hallazgos) | `python src/profiling/profile_landing.py` |
| `profile/profile_summary.json` | Las mismas métricas en formato máquina | idem |
| `profile/landing_manifest.sha256` | Huella de Landing (inmutabilidad) | idem |
| `mapreduce/mapreduce_stats.json` | Contadores map/combine/shuffle/reduce | `python src/mapreduce/daily_usage_mapreduce.py` |
| `mapreduce/org_daily_usage_by_service_mr.csv` | Salida del flujo MapReduce (11.050 filas) | idem |
| `mapreduce/org_daily_usage_by_service_spark/` | Salida equivalente en PySpark | `python src/mapreduce/daily_usage_spark.py` |
| `mapreduce/spark_physical_plan.txt` | Plan físico: partial_sum → Exchange → sum | idem |
| `tests_output.txt` | Resultado de `pytest` | `pytest -q tests > evidence/tests_output.txt` |
