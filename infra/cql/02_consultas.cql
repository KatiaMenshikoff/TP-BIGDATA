-- Consultas mínimas de la Entrega 2 (consigna 6.2: dos consultas con CQL y captura de resultados).
-- Ejecutar: docker exec -i cpa-cassandra cqlsh < infra/cql/02_consultas.cql
-- La captura en evidence/e2/serving/cqlsh_consultas.txt se regenera con: PYTHONPATH=src python -m cpa.evidence

-- Q1 · Costos y requests diarios de una organización por servicio en un rango de fechas (una partición).
SELECT usage_date, service, daily_cost_usd, requests, cpu_hours, storage_gb_hours, genai_tokens, events
FROM cpa.usage_by_org_day_service
WHERE org_id = 'org_pbhsahxt' AND usage_date >= '2025-08-05' AND usage_date <= '2025-08-07';

-- Q2 · Top-5 servicios por costo acumulado en los 14 días que terminan en as_of_date (una partición, ya ordenada).
SELECT service, cost_14d_usd, requests_14d, window_start, as_of_date
FROM cpa.cost_14d_by_org_service
WHERE org_id = 'org_pbhsahxt' AND as_of_date = '2025-08-31'
LIMIT 5;

-- Control de idempotencia: filas totales de la tabla del mart (debe coincidir con Gold y no crecer al re-ejecutar).
SELECT COUNT(*) FROM cpa.usage_by_org_day_service;
