#!/usr/bin/env bash
# Ejecuta el pipeline completo (o las etapas indicadas) con el Python del venv si existe.
# Uso: scripts/run_pipeline.sh [--stages a,b,c] [--no-serving] [--watermark "1 day"] [--lake-root ruta]
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3 )}"
PYTHONPATH=src "$PY" -m cpa.pipeline "$@"
