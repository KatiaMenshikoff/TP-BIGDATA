#!/usr/bin/env bash
# Demuestra la re-ejecución sin duplicados (consigna 6.2 · Idempotencia) con conteos antes/después:
#   corrida 1: pipeline completo desde un lake limpio
#   corrida 2: misma corrida otra vez (checkpoints intactos → el stream no relee archivos ya confirmados)
#   corrida 3: backfill forzado de Silver/Gold/Cassandra para TODAS las fechas (reprocesamiento completo)
# Salida: evidence/e2/idempotencia/{corrida_1,corrida_2,corrida_3}.json y resumen.md
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3 )}"
OUT=evidence/e2/idempotencia
mkdir -p "$OUT"
scripts/reset.sh
scripts/run_pipeline.sh 2>&1 | grep -E "run_log|run_id" | tee "$OUT/corrida_1.log"
PYTHONPATH=src "$PY" -m cpa.evidence --snapshot "$OUT/corrida_1.json" > /dev/null 2>&1
scripts/run_pipeline.sh 2>&1 | grep -E "run_log|run_id" | tee "$OUT/corrida_2.log"
PYTHONPATH=src "$PY" -m cpa.evidence --snapshot "$OUT/corrida_2.json" > /dev/null 2>&1
scripts/run_pipeline.sh --stages backfill,anomalies,serving 2>&1 | grep -E "run_log|run_id" | tee "$OUT/corrida_3.log"
PYTHONPATH=src "$PY" -m cpa.evidence --snapshot "$OUT/corrida_3.json" > /dev/null 2>&1
"$PY" - "$OUT" <<'PYEOF'
import json, sys, os
out = sys.argv[1]
runs = [json.load(open(os.path.join(out, f"corrida_{i}.json"))) for i in (1, 2, 3)]
lines = ["| Tabla | Corrida 1 (filas) | Corrida 2 (re-ejecución) | Corrida 3 (backfill completo) | Claves duplicadas (1/2/3) | ¿Igual? |",
         "|---|---|---|---|---|---|"]
ok = True
for k, v in runs[0].items():
    if k == "cassandra":
        continue
    rows = [r.get(k, {}).get("rows") for r in runs]
    dups = [str(r.get(k, {}).get("duplicate_keys", "—")) for r in runs]
    same = len(set(rows)) == 1 and all(d in ("0", "—") for d in dups)
    ok &= same
    lines.append(f"| {k} | {rows[0]:,} | {rows[1]:,} | {rows[2]:,} | {'/'.join(dups)} | {'✅' if same else '❌'} |")
for t in runs[0].get("cassandra", {}):
    rows = [r["cassandra"].get(t) for r in runs]
    same = len(set(rows)) == 1
    ok &= same
    lines.append(f"| cassandra.{t} | {rows[0]:,} | {rows[1]:,} | {rows[2]:,} | PK (upsert) | {'✅' if same else '❌'} |")
lines.append(f"\n**Resultado:** {'las tres corridas producen exactamente los mismos conteos y 0 claves duplicadas' if ok else 'HAY DIFERENCIAS'}.")
open(os.path.join(out, "resumen.md"), "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
sys.exit(0 if ok else 1)
PYEOF
