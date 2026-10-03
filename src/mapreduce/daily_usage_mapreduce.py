"""Flujo batch de referencia expresado como MapReduce (Entrega 1, ítem 9).

Calcula el grano del mart Gold `org_daily_usage_by_service`
(org_id, usage_date, service) -> costo diario, requests, cpu_hours, storage_gb_hours,
genai_tokens, carbon_kg y cantidad de eventos, a partir de usage_events_stream/*.jsonl.

Es una simulación didáctica en Python puro de Hadoop MapReduce con DOS jobs encadenados
(en Hadoop cada job escribe su salida a HDFS y el siguiente la vuelve a leer):

  Job 1 · Dedup + calidad   map: line -> (event_id, evento limpio) | quarantine
                            reduce: (event_id, [eventos]) -> un único evento (el de menor timestamp)
  Job 2 · Agregación diaria map: evento -> ((org_id, date, service), métricas)
                            combine: suma parcial dentro de cada split (achica el shuffle)
                            shuffle: hash(clave) % R  -> reducer
                            reduce: suma final por clave

La implementación productiva será PySpark (ver daily_usage_spark.py): el mismo plan lógico corre como
un único DAG, sin materializar a disco entre jobs; groupBy/agg genera el Exchange (shuffle) y Catalyst
aplica el combiner automáticamente (HashAggregate partial_sum antes del Exchange).

Uso:
    python src/mapreduce/daily_usage_mapreduce.py [--landing datalake/landing] [--reducers 4]
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import zlib
from collections import defaultdict

METRICS = ("cost_usd", "requests", "cpu_hours", "storage_gb_hours", "genai_tokens", "carbon_kg", "events")
USAGE_METRICS = ("requests", "cpu_hours", "storage_gb_hours")
MIN_COST = -0.01  # regla de calidad: costos menores se envían a quarantine
QUARANTINE = "__quarantine__"


def to_float(x):
    """Cast con fallback controlado: '12.5' -> 12.5; None, '' o 'abc' -> None."""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def run_job(splits, mapper, reducer, reducers: int, combiner=None):
    """Motor MapReduce mínimo: map (+combine) por split, shuffle por hash y reduce por partición."""
    shuffle = [defaultdict(list) for _ in range(reducers)]
    counters = defaultdict(int)
    for split in splits:  # cada split = una tarea map independiente
        local = defaultdict(list)
        for record in split:
            counters["map_input_records"] += 1
            for key, value in mapper(record):
                counters["map_output_records"] += 1
                local[key].append(value)
        for key, values in local.items():
            if combiner and key != QUARANTINE:
                values = [combiner(key, values)]
            counters["shuffle_records"] += len(values)
            shuffle[zlib.crc32(repr(key).encode()) % reducers][key].extend(values)
    output = []
    for part in shuffle:  # cada partición = una tarea reduce
        for key in sorted(part, key=repr):
            output.extend(reducer(key, part[key]))
    counters["reduce_output_records"] = len(output)
    return output, dict(counters)


# ------------------------------------------------------------------ Job 1: calidad + dedup por event_id
def map_clean(line: str):
    try:
        e = json.loads(line)
    except json.JSONDecodeError:
        yield QUARANTINE, "json_invalido"
        return
    if not e.get("event_id") or not e.get("org_id") or not e.get("timestamp"):
        yield QUARANTINE, "clave_nula"
        return
    cost = to_float(e.get("cost_usd_increment"))
    if cost is None or cost < MIN_COST:
        yield QUARANTINE, "costo_invalido"
        return
    yield e["event_id"], {
        "org_id": e["org_id"],
        "timestamp": e["timestamp"],
        "service": (e.get("service") or "unknown").strip().lower(),
        "metric": e.get("metric"),
        "value": to_float(e.get("value")),           # string -> float con fallback
        "cost_usd": cost,
        "genai_tokens": to_float(e.get("genai_tokens")) or 0.0,  # sólo schema v2 + genai
        "carbon_kg": to_float(e.get("carbon_kg")) or 0.0,        # sólo schema v2
    }


def reduce_dedup(key, values):
    if key == QUARANTINE:
        return [(QUARANTINE, reason) for reason in values]
    first = min(values, key=lambda v: v["timestamp"])  # determinístico ante reprocesos
    out = [(key, first)]
    out += [(QUARANTINE, "event_id_duplicado")] * (len(values) - 1)
    return out


# ------------------------------------------------------------------ Job 2: agregación diaria
def map_daily(event: dict):
    m = dict.fromkeys(METRICS, 0.0)
    m["cost_usd"], m["events"] = event["cost_usd"], 1
    if event["metric"] in USAGE_METRICS and event["value"] is not None:
        m[event["metric"]] = event["value"]
    m["genai_tokens"], m["carbon_kg"] = event["genai_tokens"], event["carbon_kg"]
    yield (event["org_id"], event["timestamp"][:10], event["service"]), m


def sum_metrics(key, values):
    total = dict.fromkeys(METRICS, 0.0)
    for v in values:
        for k in METRICS:
            total[k] += v[k]
    return total


def reduce_daily(key, values):
    return [(key, sum_metrics(key, values))]


def run(landing: str, reducers: int = 4):
    files = sorted(glob.glob(os.path.join(landing, "usage_events_stream", "*.jsonl")))
    splits = [open(p).read().splitlines() for p in files]

    job1_out, c1 = run_job(splits, map_clean, reduce_dedup, reducers)
    clean = [v for k, v in job1_out if k != QUARANTINE]
    quarantine = defaultdict(int)
    for k, v in job1_out:
        if k == QUARANTINE:
            quarantine[v] += 1

    # La salida del Job 1 se "re-splitea" (en Hadoop: part-r-0000N en HDFS -> input del Job 2).
    clean_splits = [clean[i::reducers] for i in range(reducers)]
    job2_out, c2 = run_job(clean_splits, map_daily, reduce_daily, reducers, combiner=sum_metrics)
    job2_out.sort(key=lambda kv: kv[0])
    stats = {"input_files": len(files), "job1_dedup": c1, "job2_aggregate": c2,
             "clean_events": len(clean), "quarantine": dict(quarantine)}
    return job2_out, stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default="datalake/landing")
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--out", default="evidence/mapreduce")
    args = ap.parse_args()
    results, stats = run(args.landing, args.reducers)
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "org_daily_usage_by_service_mr.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["org_id", "usage_date", "service", *METRICS])
        for (org, day, svc), m in results:
            w.writerow([org, day, svc, *(round(m[k], 4) for k in METRICS)])
    with open(os.path.join(args.out, "mapreduce_stats.json"), "w") as fh:
        json.dump(stats, fh, indent=2)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
