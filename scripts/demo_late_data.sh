#!/usr/bin/env bash
# Late data (D-05): corre el stream a Bronze + reconciliación en lakes temporales con watermarks cortos
# para mostrar qué pasaría con un delay "típico" y que ningún tardío se pierde en silencio (van a quarantine).
# Salida: evidence/e2/late_data/<delay>.json y resumen.md. No toca el lake principal.
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3 )}"
OUT=evidence/e2/late_data
mkdir -p "$OUT"
for DELAY in "1 day" "7 days" "30 days" "62 days"; do
  TAG="${DELAY// /_}"
  LAKE="datalake_demo/wm_$TAG"
  rm -rf "$LAKE" && mkdir -p "$LAKE" && ln -s "$(pwd)/datalake/landing" "$LAKE/landing"
  scripts/run_pipeline.sh --lake-root "$LAKE" --watermark "$DELAY" --stages bronze_stream,reconcile --no-serving \
    2>&1 | grep -E "run_log" | tee "$OUT/$TAG.log"
  cp "$LAKE/_metadata/reconciliation.json" "$OUT/$TAG.json"
  cp "$LAKE/_metadata/streaming_progress/bronze_usage_events.json" "$OUT/${TAG}_progress.json"
done
"$PY" - "$OUT" <<'PYEOF'
import json, sys, os
out = sys.argv[1]
lines = ["| Watermark | Eventos en Landing | Llegan a Bronze | Descartados por watermark → quarantine/late_events | % descartado | Balance |",
         "|---|---|---|---|---|---|"]
for tag in ["1_day", "7_days", "30_days", "62_days"]:
    r = json.load(open(os.path.join(out, f"{tag}.json")))
    pct = 100 * r["late_dropped_by_watermark"] / r["landing_rows"]
    lines.append(f"| {r['watermark']} | {r['landing_rows']:,} | {r['bronze_rows']:,} | {r['late_dropped_by_watermark']:,} | "
                 f"{pct:.1f} % | {'✅ cierra' if r['balanced'] else '❌'} |")
open(os.path.join(out, "resumen.md"), "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PYEOF
rm -rf datalake_demo
