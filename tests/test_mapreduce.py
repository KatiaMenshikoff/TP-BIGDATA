import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "mapreduce"))
import daily_usage_mapreduce as mr  # noqa: E402


def ev(**kw):
    base = {"event_id": "e1", "timestamp": "2025-07-20T10:00:00Z", "org_id": "org_a", "service": "genai",
            "metric": "requests", "value": "10", "cost_usd_increment": 1.5, "schema_version": 2,
            "carbon_kg": 0.1, "genai_tokens": 100}
    base.update(kw)
    return json.dumps(base)


def test_value_string_se_castea():
    [(key, e)] = list(mr.map_clean(ev(value="12.5")))
    assert key == "e1" and e["value"] == 12.5


@pytest.mark.parametrize("line,reason", [
    ("{no json", "json_invalido"),
    (ev(event_id=None), "clave_nula"),
    (ev(cost_usd_increment=-5), "costo_invalido"),
])
def test_registros_invalidos_van_a_quarantine(line, reason):
    assert list(mr.map_clean(line)) == [(mr.QUARANTINE, reason)]


def test_costo_negativo_tolerado():
    assert list(mr.map_clean(ev(cost_usd_increment=-0.01)))[0][0] == "e1"


def test_dedup_y_agregacion(tmp_path):
    stream = tmp_path / "usage_events_stream"
    stream.mkdir()
    (stream / "p0.jsonl").write_text("\n".join([ev(), ev(event_id="e2", value=None)]) + "\n")
    (stream / "p1.jsonl").write_text("\n".join([ev(), ev(event_id="e3", schema_version=1, carbon_kg=None,
                                                         genai_tokens=None, cost_usd_increment=-9)]) + "\n")
    results, stats = mr.run(str(tmp_path), reducers=2)
    assert stats["quarantine"] == {"event_id_duplicado": 1, "costo_invalido": 1}
    [(key, m)] = results
    assert key == ("org_a", "2025-07-20", "genai")
    assert m["events"] == 2 and m["cost_usd"] == 3.0 and m["requests"] == 10.0 and m["genai_tokens"] == 200
