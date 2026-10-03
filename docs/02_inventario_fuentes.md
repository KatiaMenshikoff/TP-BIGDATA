# Inventario y diccionario de fuentes (Landing)

Perfil automático: `python src/profiling/profile_landing.py` → `evidence/profile/profile_report.md`.
El tipo "destino" es el tipo que se aplicará con esquema explícito en Bronze/Silver.

## customers_orgs.csv · 80 filas · clave `org_id` · owner: FinOps
| Columna | Tipo destino | Nulos | Notas |
|---|---|---|---|
| org_id | string | 0 | Clave natural; PK de `dim_org` |
| org_name | string | 0 | |
| industry | string | 0 | 10 valores |
| hq_region | string | 0 | 7 regiones (us-east, us-west, eu-west, eu-central, sa-east, ap-south, ap-northeast) |
| plan_tier | string | 0 | free / standard / pro / enterprise |
| is_enterprise | boolean | 0 | 25 orgs incoherentes con `plan_tier` → se conserva la fuente y se agrega un flag |
| signup_date | date | 0 | 2025-05-04 a 2025-07-02 |
| sales_rep | string | 0 | rep_a … rep_e |
| lifecycle_stage | string | 0 | lead / prospect / active / at_risk / churned → **SCD2** |
| marketing_source | string | 0 | |
| nps_score | double | 11 | Rango −38 a 101 (1 fuera de [-100,100]) |

## users.csv · 800 filas · clave `user_id` · owner: Producto
| Columna | Tipo destino | Nulos | Notas |
|---|---|---|---|
| user_id | string | 0 | |
| org_id | string | 0 | FK → dim_org |
| email | string | 0 | **PII** → `email_hash` (SHA-256) en Silver; no se publica |
| role | string | 0 | 6 roles |
| active | boolean | 0 | |
| created_at | date | 0 | |
| last_login | date | 139 | 232 casos con `last_login < created_at` (Q07) |

## resources.csv · 400 filas · clave `resource_id` · owner: Producto
| Columna | Tipo destino | Nulos | Notas |
|---|---|---|---|
| resource_id | string | 0 | |
| org_id | string | 0 | FK |
| service | string | 0 | compute, storage, database, networking, analytics, genai |
| region | string | 0 | |
| created_at | date | 0 | |
| state | string | 0 | running / stopped / terminated |
| tags_json | string → array<string> | 83 | JSON embebido `["env:prod","pii:true"]` → `from_json` |

## support_tickets.csv · 1.000 filas · clave `ticket_id` · owner: Soporte
| Columna | Tipo destino | Nulos | Notas |
|---|---|---|---|
| ticket_id | string | 0 | |
| org_id | string | 0 | FK |
| category | string | 0 | 6 categorías |
| severity | string | 0 | low / medium / high / critical |
| created_at | date | 0 | 2025-05-09 a 2025-08-31 |
| resolved_at | date | 240 | Nulo = abierto |
| csat | double | 254 | 40 fuera de [1,5] (0, 6, 7) → Q06 |
| sla_breached | boolean | 0 | 95 True |

## marketing_touches.csv · 1.500 filas · clave `touch_id` · owner: Producto
| Columna | Tipo destino | Nulos | Notas |
|---|---|---|---|
| touch_id | string | 0 | |
| org_id | string | 0 | FK |
| campaign | string | 0 | 6 campañas |
| channel | string | 0 | email / ads / event / in_app |
| timestamp | date | 0 | Viene como fecha, sin hora |
| clicked, converted | boolean | 0 | 96 conversiones sin clic (flag) |

## nps_surveys.csv · 92 filas · clave `org_id + survey_date` · owner: Soporte
| Columna | Tipo destino | Nulos | Notas |
|---|---|---|---|
| org_id | string | 0 | 60 orgs con encuesta |
| survey_date | date | 0 | |
| nps_score | double | 19 | Rango −16 a 68 |
| comment | string | 10 | 6 valores categóricos de texto libre |

## billing_monthly.csv · 240 filas · clave `invoice_id` · owner: FinOps
| Columna | Tipo destino | Nulos | Notas |
|---|---|---|---|
| invoice_id | string | 0 | |
| org_id | string | 0 | 80 orgs × 3 meses |
| month | date | 0 | 2025-06-01, 2025-07-01, 2025-08-01 |
| subtotal | decimal(12,2) | 0 | 13 negativos (¿notas de crédito?) |
| credits | decimal(12,2) | 137 | Nulo → 0 |
| taxes | decimal(12,2) | 0 | ≈ 21 % del subtotal |
| currency | string | 0 | USD 160 · ARS 51 · EUR 29; 50 orgs con más de una moneda |
| exchange_rate_to_usd | double | 0 | USD [0,855–1,118] ⚠, EUR [0,998–1,198], ARS [0,00133–0,00162] |

## usage_events_stream/*.jsonl · 43.200 eventos · 120 archivos · clave `event_id` · owner: FinOps-uso
| Campo | Tipo destino | Versión | Nulos | Notas |
|---|---|---|---|---|
| event_id | string | v1, v2 | 0 | Clave de deduplicación |
| timestamp | timestamp (UTC) | v1, v2 | 0 | ISO-8601 con `Z`; 2025-07-03 a 2025-08-31 |
| org_id | string | v1, v2 | 0 | 100 % existe en maestros |
| resource_id | string | v1, v2 | 0 | 100 % existe en maestros |
| service | string | v1, v2 | 0 | Coincide con el servicio del recurso |
| region | string | v1, v2 | 0 | |
| metric | string | v1, v2 | 0 | requests / cpu_hours / storage_gb_hours |
| value | string → double | v1, v2 | 877 | **1.309 vienen como string** → `try_cast` |
| unit | string | v1, v2 | 2.075 | count / hours / gb_hours; se deriva de `metric` |
| cost_usd_increment | double | v1, v2 | 0 | 216 negativos; outliers hasta 317,43 |
| schema_version | int | v1, v2 | 0 | v1: 10.800 (hasta el 17/07) · v2: 32.400 (desde el 18/07) |
| carbon_kg | double | **v2** | 10.800 (todos los v1) | |
| genai_tokens | long | **v2, sólo `service=genai`** | 40.068 | 3.132 eventos con valor |
