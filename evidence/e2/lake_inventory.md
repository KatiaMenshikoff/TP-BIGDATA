| Tabla | Ruta | Filas | Particiones | Archivos | Tamaño (KB) | KB/archivo | Claves duplicadas |
|---|---|---|---|---|---|---|---|
| bronze.customers_orgs | `datalake/bronze/customers_orgs` | 80 | 1 | 1 | 13.4 | 13.4 | 0 |
| bronze.users | `datalake/bronze/users` | 800 | 1 | 1 | 74.8 | 74.8 | 0 |
| bronze.resources | `datalake/bronze/resources` | 400 | 1 | 1 | 38.2 | 38.2 | 0 |
| bronze.billing_monthly | `datalake/bronze/billing_monthly` | 240 | 3 | 3 | 39.4 | 13.1 | 0 |
| bronze.support_tickets | `datalake/bronze/support_tickets` | 1,000 | 1 | 1 | 86.3 | 86.3 | 0 |
| bronze.marketing_touches | `datalake/bronze/marketing_touches` | 1,500 | 1 | 1 | 123.5 | 123.5 | 0 |
| bronze.nps_surveys | `datalake/bronze/nps_surveys` | 92 | 1 | 1 | 11.1 | 11.1 | 0 |
| bronze.usage_events | `datalake/bronze/usage_events` | 43,200 | 60 | 720 | 7,356.7 | 10.2 | 0 |
| silver.dim_org | `datalake/silver/dim_org` | 80 | 0 | 1 | 14.8 | 14.8 | 0 |
| silver.dim_user | `datalake/silver/dim_user` | 800 | 0 | 1 | 120.3 | 120.3 | 0 |
| silver.dim_resource | `datalake/silver/dim_resource` | 400 | 0 | 1 | 37.1 | 37.1 | 0 |
| silver.fact_billing | `datalake/silver/fact_billing` | 240 | 3 | 3 | 52.8 | 17.6 | 0 |
| silver.fact_usage_events | `datalake/silver/fact_usage_events` | 42,989 | 60 | 60 | 3,789.4 | 63.2 | 0 |
| gold.org_daily_usage_by_service | `datalake/gold/org_daily_usage_by_service` | 11,050 | 60 | 60 | 731.2 | 12.2 | 0 |
| gold.cost_anomaly_mart | `datalake/gold/cost_anomaly_mart` | 11,050 | 60 | 60 | 428.4 | 7.1 | 0 |
| quarantine.silver_usage_events | `datalake/quarantine/silver_usage_events` | 211 | 58 | 58 | 510.9 | 8.8 | — |
| quarantine.late_events | `datalake/quarantine/late_events` | 0 | 0 | 1 | 2.1 | 2.1 | — |

Cassandra: {'usage_by_org_day_service': 11050, 'cost_14d_by_org_service': 262}
