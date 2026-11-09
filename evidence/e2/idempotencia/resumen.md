| Tabla | Corrida 1 (filas) | Corrida 2 (re-ejecución) | Corrida 3 (backfill completo) | Claves duplicadas (1/2/3) | ¿Igual? |
|---|---|---|---|---|---|
| bronze.customers_orgs | 80 | 80 | 80 | 0/0/0 | ✅ |
| bronze.users | 800 | 800 | 800 | 0/0/0 | ✅ |
| bronze.resources | 400 | 400 | 400 | 0/0/0 | ✅ |
| bronze.billing_monthly | 240 | 240 | 240 | 0/0/0 | ✅ |
| bronze.support_tickets | 1,000 | 1,000 | 1,000 | 0/0/0 | ✅ |
| bronze.marketing_touches | 1,500 | 1,500 | 1,500 | 0/0/0 | ✅ |
| bronze.nps_surveys | 92 | 92 | 92 | 0/0/0 | ✅ |
| bronze.usage_events | 43,200 | 43,200 | 43,200 | 0/0/0 | ✅ |
| silver.dim_org | 80 | 80 | 80 | 0/0/0 | ✅ |
| silver.dim_user | 800 | 800 | 800 | 0/0/0 | ✅ |
| silver.dim_resource | 400 | 400 | 400 | 0/0/0 | ✅ |
| silver.fact_billing | 240 | 240 | 240 | 0/0/0 | ✅ |
| silver.fact_usage_events | 42,989 | 42,989 | 42,989 | 0/0/0 | ✅ |
| gold.org_daily_usage_by_service | 11,050 | 11,050 | 11,050 | 0/0/0 | ✅ |
| gold.cost_anomaly_mart | 11,050 | 11,050 | 11,050 | 0/0/0 | ✅ |
| quarantine.silver_usage_events | 211 | 211 | 211 | —/—/— | ✅ |
| quarantine.late_events | 0 | 0 | 0 | —/—/— | ✅ |
| cassandra.usage_by_org_day_service | 11,050 | 11,050 | 11,050 | PK (upsert) | ✅ |
| cassandra.cost_14d_by_org_service | 262 | 262 | 262 | PK (upsert) | ✅ |

**Resultado:** las tres corridas producen exactamente los mismos conteos y 0 claves duplicadas.
