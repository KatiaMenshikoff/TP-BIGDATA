"""Perfil inicial de las fuentes de Landing (Entrega 1 - evidencia de lectura/exploración).

Lee los CSV y los JSONL de Landing SIN modificarlos y genera:
  - evidence/profile/profile_report.md   (reporte legible)
  - evidence/profile/profile_summary.json (métricas para reutilizar en reglas de calidad)
  - evidence/profile/landing_manifest.sha256 (huellas para verificar inmutabilidad de Landing)

Uso:
    python src/profiling/profile_landing.py [--landing datalake/landing] [--out evidence/profile]
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
from datetime import datetime, timezone

import pandas as pd

# Clave natural esperada y columnas de fecha por fuente (supuestos a validar).
CSV_SOURCES = {
    "customers_orgs.csv": {"key": ["org_id"], "dates": ["signup_date"]},
    "users.csv": {"key": ["user_id"], "dates": ["created_at", "last_login"]},
    "resources.csv": {"key": ["resource_id"], "dates": ["created_at"]},
    "support_tickets.csv": {"key": ["ticket_id"], "dates": ["created_at", "resolved_at"]},
    "marketing_touches.csv": {"key": ["touch_id"], "dates": ["timestamp"]},
    "nps_surveys.csv": {"key": ["org_id", "survey_date"], "dates": ["survey_date"]},
    "billing_monthly.csv": {"key": ["invoice_id"], "dates": ["month"]},
}
EXPECTED_UNIT = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def profile_columns(df: pd.DataFrame) -> list[dict]:
    """Nulos, cardinalidad, tipo inferido y ejemplos por columna (leyendo todo como texto)."""
    out = []
    for col in df.columns:
        s = df[col]
        non_null = s.dropna()
        numeric = pd.to_numeric(non_null, errors="coerce")
        inferred = "numeric" if len(non_null) and numeric.notna().all() else "string"
        if inferred == "string" and non_null.isin(["True", "False"]).all() and len(non_null):
            inferred = "boolean"
        elif inferred == "string" and pd.to_datetime(non_null, errors="coerce", format="mixed").notna().all() and len(non_null):
            inferred = "date"
        info = {
            "column": col,
            "inferred_type": inferred,
            "nulls": int(s.isna().sum()),
            "null_pct": round(100 * s.isna().mean(), 2),
            "distinct": int(non_null.nunique()),
            "examples": [str(x) for x in non_null.unique()[:4]],
        }
        if inferred == "numeric":
            info.update(min=float(numeric.min()), max=float(numeric.max()), mean=round(float(numeric.mean()), 4))
        if inferred == "date":
            d = pd.to_datetime(non_null, format="mixed")
            info.update(min=str(d.min().date()), max=str(d.max().date()))
        out.append(info)
    return out


def csv_findings(name: str, df: pd.DataFrame) -> list[str]:
    """Hallazgos de calidad específicos del dominio (reglas candidatas para Silver)."""
    f = []
    num = lambda c: pd.to_numeric(df[c], errors="coerce")
    date = lambda c: pd.to_datetime(df[c], errors="coerce")
    if name == "customers_orgs.csv":
        nps = num("nps_score")
        f.append(f"nps_score nulo en {int(nps.isna().sum())} orgs; fuera de [-100,100]: {int(((nps < -100) | (nps > 100)).sum())}")
        incoh = ((df.plan_tier == "enterprise") != (df.is_enterprise == "True")).sum()
        f.append(f"plan_tier vs is_enterprise inconsistentes: {int(incoh)} de {len(df)}")
    if name == "users.csv":
        f.append(f"last_login nulo: {int(df.last_login.isna().sum())}; last_login < created_at: {int((date('last_login') < date('created_at')).sum())}")
        f.append("email es PII -> enmascarar/hashear en Silver")
    if name == "resources.csv":
        f.append(f"tags_json nulo: {int(df.tags_json.isna().sum())}; tags 'pii:true': {int(df.tags_json.fillna('').str.contains('pii:true').sum())}")
    if name == "support_tickets.csv":
        csat = num("csat")
        f.append(f"csat nulo: {int(csat.isna().sum())}; fuera de [1,5]: {int(((csat < 1) | (csat > 5)).sum())}")
        f.append(f"tickets abiertos (resolved_at nulo): {int(df.resolved_at.isna().sum())}; resolved < created: {int((date('resolved_at') < date('created_at')).sum())}")
        f.append(f"sla_breached=True: {int((df.sla_breached == 'True').sum())}; critical: {int((df.severity == 'critical').sum())}")
    if name == "marketing_touches.csv":
        f.append(f"converted=True sin clicked=True: {int(((df.converted == 'True') & (df.clicked != 'True')).sum())}")
    if name == "nps_surveys.csv":
        f.append(f"nps_score nulo: {int(df.nps_score.isna().sum())}; comment nulo: {int(df.comment.isna().sum())}")
    if name == "billing_monthly.csv":
        fx = num("exchange_rate_to_usd")
        f.append(f"credits nulo: {int(df.credits.isna().sum())} (se asume 0)")
        f.append(f"subtotal negativo: {int((num('subtotal') < 0).sum())}; taxes negativo: {int((num('taxes') < 0).sum())}")
        for cur, g in df.assign(fx=fx).groupby("currency"):
            f.append(f"currency={cur}: {len(g)} facturas, exchange_rate_to_usd en [{g.fx.min()}, {g.fx.max()}]")
        f.append(f"orgs con más de una moneda: {int((df.groupby('org_id').currency.nunique() > 1).sum())}")
    return f


def profile_events(landing: str) -> tuple[dict, list[str]]:
    files = sorted(glob.glob(os.path.join(landing, "usage_events_stream", "*.jsonl")))
    rows, raw_value_types, key_sets, bad_lines = [], {}, {}, 0
    for path in files:
        with open(path) as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines += 1
                    continue
                t = type(rec.get("value")).__name__
                raw_value_types[t] = raw_value_types.get(t, 0) + 1
                ks = ",".join(sorted(rec))
                key_sets[ks] = key_sets.get(ks, 0) + 1
                rec["_source_file"] = os.path.basename(path)
                rows.append(rec)
    ev = pd.DataFrame(rows)
    ev["value_num"] = pd.to_numeric(ev["value"], errors="coerce")
    ev["ts"] = pd.to_datetime(ev["timestamp"], utc=True)
    ev["event_date"] = ev.ts.dt.date.astype(str)
    cost = ev.cost_usd_increment
    # z robusto (MAD) por servicio+métrica: el costo "normal" difiere mucho entre combinaciones.
    grp = ev.groupby(["service", "metric"]).cost_usd_increment
    med = grp.transform("median")
    mad = (cost - med).abs().groupby([ev.service, ev.metric]).transform("median")
    robust_z = 0.6745 * (cost - med) / mad
    per_file = ev.groupby("_source_file").agg(n=("event_id", "size"), ts_min=("ts", "min"), ts_max=("ts", "max"))
    unit_mismatch = ev.dropna(subset=["unit"]).pipe(lambda d: (d.unit != d.metric.map(EXPECTED_UNIT)).sum())

    summary = {
        "files": len(files),
        "bad_json_lines": bad_lines,
        "rows": len(ev),
        "distinct_event_id": int(ev.event_id.nunique()),
        "duplicated_event_id": int(ev.event_id.duplicated().sum()),
        "rows_per_file_avg": round(len(ev) / max(len(files), 1), 1),
        "bytes_total": int(sum(os.path.getsize(p) for p in files)),
        "ts_min": str(ev.ts.min()),
        "ts_max": str(ev.ts.max()),
        "days": int(ev.event_date.nunique()),
        "events_per_day_avg": round(len(ev) / ev.event_date.nunique(), 1),
        "files_spanning_full_range": int(((per_file.ts_max - per_file.ts_min).dt.days >= 50).sum()),
        "key_sets": key_sets,
        "raw_value_types": raw_value_types,
        "value_null": int(ev.value.isna().sum()),
        "unit_null": int(ev.unit.isna().sum()),
        "unit_inconsistent_with_metric": int(unit_mismatch),
        "schema_version_counts": {str(k): int(v) for k, v in ev.schema_version.value_counts().items()},
        "schema_v2_first_ts": str(ev[ev.schema_version == 2].ts.min()),
        "schema_v1_last_ts": str(ev[ev.schema_version == 1].ts.max()),
        "genai_tokens_non_null": int(ev.genai_tokens.notna().sum()) if "genai_tokens" in ev else 0,
        "genai_tokens_services": ev[ev.get("genai_tokens").notna()].service.unique().tolist() if "genai_tokens" in ev else [],
        "carbon_kg_non_null": int(ev.carbon_kg.notna().sum()) if "carbon_kg" in ev else 0,
        "cost_negative": int((cost < 0).sum()),
        "cost_below_minus_0_01": int((cost < -0.01).sum()),
        "cost_stats": {k: round(float(v), 4) for k, v in cost.describe(percentiles=[.5, .9, .99, .999]).items()},
        "cost_outliers_robust_z_gt_3_5": int((robust_z.abs() > 3.5).sum()),
        "services": ev.service.value_counts().to_dict(),
        "regions": ev.region.value_counts().to_dict(),
        "metrics": ev.metric.value_counts().to_dict(),
        "orgs": int(ev.org_id.nunique()),
        "resources": int(ev.resource_id.nunique()),
    }
    findings = [
        f"{summary['rows']} eventos en {summary['files']} archivos (~{summary['rows_per_file_avg']} por archivo); {summary['duplicated_event_id']} event_id duplicados en este corte",
        f"{summary['files_spanning_full_range']} de {summary['files']} archivos contienen eventos de todo el rango temporal -> llegada desordenada (late data)",
        f"value: {raw_value_types.get('str', 0)} como string, {summary['value_null']} nulos",
        f"unit nulo en {summary['unit_null']} eventos (derivable desde metric); inconsistencias metric/unit: {summary['unit_inconsistent_with_metric']}",
        f"schema_version=1 hasta {summary['schema_v1_last_ts']}, schema_version=2 desde {summary['schema_v2_first_ts']}",
        f"genai_tokens sólo en v2 y servicios {summary['genai_tokens_services']} ({summary['genai_tokens_non_null']} eventos); carbon_kg en {summary['carbon_kg_non_null']} eventos v2",
        f"cost_usd_increment negativo: {summary['cost_negative']} (< -0.01: {summary['cost_below_minus_0_01']}); outliers |z robusto por servicio+métrica|>3.5: {summary['cost_outliers_robust_z_gt_3_5']}",
    ]
    return summary, findings


def to_md_table(rows: list[dict], cols: list[str]) -> str:
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        lines.append("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default="datalake/landing")
    ap.add_argument("--out", default="evidence/profile")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    report = [f"# Perfil inicial de Landing\n\nGenerado: {datetime.now(timezone.utc).isoformat(timespec='seconds')} · Landing: `{args.landing}`\n"]
    summary: dict = {"csv": {}}
    inventory = []
    for name, meta in CSV_SOURCES.items():
        path = os.path.join(args.landing, name)
        df = pd.read_csv(path, dtype=str)  # todo como texto: el perfil no debe "arreglar" tipos
        dup = int(df.duplicated(meta["key"]).sum())
        cols = profile_columns(df)
        findings = csv_findings(name, df)
        summary["csv"][name] = {"rows": len(df), "cols": df.shape[1], "bytes": os.path.getsize(path),
                                "key": meta["key"], "duplicated_key": dup, "columns": cols, "findings": findings}
        inventory.append({"fuente": name, "filas": len(df), "columnas": df.shape[1],
                          "KB": round(os.path.getsize(path) / 1024, 1), "clave": "+".join(meta["key"]), "dup_clave": dup})
        report.append(f"\n## {name}\n\nFilas: **{len(df)}** · Columnas: {df.shape[1]} · Clave `{'+'.join(meta['key'])}` duplicada: {dup}\n")
        report.append(to_md_table(cols, ["column", "inferred_type", "nulls", "null_pct", "distinct", "min", "max", "examples"]))
        report.append("\n**Hallazgos:**\n" + "\n".join(f"- {x}" for x in findings) + "\n")

    ev_summary, ev_findings = profile_events(args.landing)
    summary["usage_events_stream"] = ev_summary
    summary["usage_events_stream"]["findings"] = ev_findings
    inventory.append({"fuente": "usage_events_stream/*.jsonl", "filas": ev_summary["rows"], "columnas": "11-13",
                      "KB": round(ev_summary["bytes_total"] / 1024, 1), "clave": "event_id", "dup_clave": ev_summary["duplicated_event_id"]})
    report.append("\n## usage_events_stream/*.jsonl\n")
    report.append("\n".join(f"- {x}" for x in ev_findings))
    report.append("\n\n```json\n" + json.dumps({k: ev_summary[k] for k in ["cost_stats", "services", "metrics", "regions", "key_sets", "raw_value_types"]}, indent=2) + "\n```\n")
    report.insert(1, "## Inventario\n\n" + to_md_table(inventory, ["fuente", "filas", "columnas", "KB", "clave", "dup_clave"]) + "\n")

    with open(os.path.join(args.out, "profile_report.md"), "w") as fh:
        fh.write("\n".join(report))
    with open(os.path.join(args.out, "profile_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    with open(os.path.join(args.out, "landing_manifest.sha256"), "w") as fh:
        for path in sorted(glob.glob(os.path.join(args.landing, "**", "*.*"), recursive=True)):
            fh.write(f"{sha256(path)}  {os.path.relpath(path, args.landing)}\n")
    print(f"OK -> {args.out}/profile_report.md")
    for x in ev_findings:
        print(" -", x)


if __name__ == "__main__":
    main()
