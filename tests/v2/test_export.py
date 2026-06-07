"""Unit tests for clean dataset export formatting (pure, no DB)."""

import csv
import io
import json
from datetime import date

from src.edu_cti_v2 import export


def test_datasets_listed():
    assert set(export.DATASETS) == {"incidents", "iocs", "mitre", "cves", "campaigns"}
    # every advertised dataset has a backing query
    for name in export.DATASETS:
        assert name in export._QUERIES


def test_to_csv_roundtrips_headers_and_rows():
    rows = [
        {"incident_id": "a", "institution_type": "university", "incident_date": date(2025, 1, 2)},
        {"incident_id": "b", "institution_type": "school_district", "incident_date": None},
    ]
    out = export.to_csv(rows)
    parsed = list(csv.DictReader(io.StringIO(out)))
    assert [r["incident_id"] for r in parsed] == ["a", "b"]
    assert parsed[0]["institution_type"] == "university"
    # dates are serialized via isoformat
    assert parsed[0]["incident_date"] == "2025-01-02"
    assert parsed[1]["incident_date"] == ""


def test_to_csv_empty_is_empty_string():
    assert export.to_csv([]) == ""


def test_to_json_serializes_dates():
    rows = [{"incident_id": "a", "incident_date": date(2025, 1, 2)}]
    payload = json.loads(export.to_json(rows))
    assert payload[0]["incident_id"] == "a"
    assert payload[0]["incident_date"] == "2025-01-02"


def test_export_dataset_rejects_unknown_dataset():
    try:
        export.export_dataset(session=None, dataset="bogus", fmt="csv")
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "bogus" in str(exc)


def test_export_dataset_rejects_unknown_format():
    class _FakeSession:
        def execute(self, *_a, **_k):
            class _R:
                def keys(self_inner):
                    return []

                def fetchall(self_inner):
                    return []

            return _R()

    try:
        export.export_dataset(_FakeSession(), "incidents", "xml")
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "xml" in str(exc)
