# Plan de correcciones posterior al feedback (Entrega 1 → Entrega 2)

Consigna, Sección 5.6: cada ítem lleva prioridad, responsable, fecha objetivo y evidencia esperada.

## Feedback recibido (Entrega 1 · nota 10/10, rúbrica 9,85)

| Dimensión | Calif. | Feedback del docente |
|---|---|---|
| Comprensión y justificación | 10,0 | Muy buena conexión entre problema, objetivos, 5V y criterios cuantificables. |
| **Arquitectura** | **9,5** | La arquitectura es sólida y apropiada al caso. **Los trade-offs podrían explicitarse todavía un poco más frente a algunas alternativas.** |
| Data Lake | 10,0 | Diseño completo de capas, formatos, particiones, calidad, promoción y metadatos. |
| Datos | 10,0 | Muy buen perfilado, incluyendo 43.200 eventos, evolución de esquema, problemas de calidad y análisis del desorden temporal. |
| Procesamiento batch | 10,0 | Excelente que se disponga tanto de una referencia MapReduce como de su equivalente PySpark verificable. |
| Ingeniería y documentación | 10,0 | Entrega técnicamente muy completa. |
| **Defensa / feedback** | **9,5** | El nivel de registro de decisiones y problemas detectados deja al grupo bien preparado para defender y corregir el diseño. |

Comentario general: *"La prioridad en E2 debería ser **conservar esta trazabilidad** mientras convierten el diseño en **pipeline reproducible**."*

**Nuestra respuesta:** vamos a explicitar los trade-offs un poco más y a tener como prioridad de la E2 seguir con la trazabilidad.

## Plan

| # | Origen | Corrección | Prioridad | Responsable | Fecha objetivo | Evidencia esperada | Estado |
|---|---|---|---|---|---|---|---|
| 1 | Arquitectura (9,5) | Explicitar los trade-offs frente a las alternativas en **todas** las decisiones: qué ganaríamos, qué perderíamos y cuál es la señal de reversión | Alta | Katia Menshikoff | 12/11/2026 | `DECISIONS.md` v2.0 (tabla de trade-offs en D-01 a D-15); documento E2, Sección 12 | ✅ Hecho |
| 2 | Comentario general | Conservar la trazabilidad: cada fila con `run_id` y `source_file`; `run_log` por etapa; matriz requisito → componente → evidencia | Alta | Katia Menshikoff | 16/11/2026 | Documento E2, Sección 13 (matriz); `evidence/e2/run_log.csv`; esquemas en `evidence/e2/esquemas/` | ✅ Hecho |
| 3 | Comentario general | Convertir el diseño en pipeline reproducible desde un entorno limpio | Alta | Iván Josephsohn · Julián A. Szarfmeser | 16/11/2026 | `scripts/run_pipeline.sh`, `scripts/reset.sh`, Quickstart del README, 24 tests | ✅ Hecho |
| 4 | Trazabilidad E1 → E2 | Verificar que la implementación reproduce los números del diseño (11.050 claves del MapReduce, 211 costos inválidos, 312 outliers) | Alta | Katia Menshikoff | 16/11/2026 | `evidence/e2/gold_vs_mapreduce.json` (0 diferencias de claves); DECISIONS D-08 (312 = 123 + 189) | ✅ Hecho |
| 5 | Decisión abierta A2 / D-05 | Cerrar el watermark con Spark real (no simulación) y asegurar que ningún tardío se pierde en silencio | Alta | Iván Josephsohn | 12/11/2026 | `evidence/e2/late_data/resumen.md` (1, 7, 30 y 62 días; balance cierra en los 4) | ✅ Hecho |
| 6 | Decisión abierta A1 / D-06 | Implementar la normalización FX con flag Q08 | Media | Julián A. Szarfmeser | 12/11/2026 | `silver/fact_billing` (Q08 = 144, Q09 = 13, Q10 = 137) | ✅ Hecho · semántica a confirmar con el docente |
| 7 | Decisión abierta A4 / D-08 | Implementar el componente analítico (z robusto MAD) y medirlo | Media | Francisco Gomes | 14/11/2026 | `gold/cost_anomaly_mart`; `evidence/e2/analitica/metricas.json` | ✅ Hecho |
| 8 | Decisión abierta A5 / riesgo K4 | Serving reproducible sin depender de AstraDB en la demo | Media | Francisco Gomes | 14/11/2026 | `infra/docker-compose.yml`; `evidence/e2/serving/cqlsh_consultas.txt` | ✅ Hecho · falta una corrida contra AstraDB real (backlog B-01) |
| 9 | Defensa (9,5) | Registrar los problemas encontrados al implementar y cómo se resolvieron, para defenderlos | Media | Todo el equipo | 16/11/2026 | Documento E2, Sección 15 | ✅ Hecho |

Preguntas que llevamos a la revisión del 16/11: semántica del FX de USD (D-06) y si la demo final debe correr contra AstraDB o alcanza con Cassandra local + capturas (D-11).
