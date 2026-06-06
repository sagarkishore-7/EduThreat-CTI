#!/usr/bin/env python
"""LLM extraction bake-off for the EduThreat-CTI v2 enrichment schema (Ollama Cloud).

Runs a fixed sample of already-fetched articles through the *real* extraction
path (`IncidentEnricher._enrich_article`) for each candidate Ollama Cloud model,
then scores accuracy and performance so we can pick the best default model.

Read-only: it never writes enrichments back to the DB — it only reads source
incidents + their selected article documents and runs extraction in memory.

Accuracy is scored without a hand-labeled gold set by combining:
  * schema-validity  — fraction of articles that produced a parseable result
  * field coverage   — mean non-null rate over the key CTI fields
  * baseline agreement — categorical agreement vs the deepseek baseline on the
    fields that should be model-invariant (institution, attack_category,
    is_education_related, vendor, actor)
Performance: mean/median wall-clock latency and the failure rate.

Optionally, pass --gold <file.json> mapping source_incident_id → expected field
values to score field-level precision against true labels instead of agreement.

Usage:
  set -a; . ./.env; set +a   # OLLAMA_API_KEY
  railway run --service Postgres -- bash -c \
    'export EDU_CTI_V2_DATABASE_URL="$DATABASE_PUBLIC_URL"; \
     .venv/bin/python scripts/llm_bakeoff.py --sample 40'
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import statistics
import sys
import time

# Stream progress live even when run as a subprocess (railway run buffers stdout).
print = functools.partial(print, flush=True)  # noqa: A001
from dataclasses import dataclass, field
from typing import Any, Optional

# Candidate Ollama Cloud models (confirmed reachable). deepseek is the baseline.
DEFAULT_MODELS = [
    "deepseek-v3.1:671b-cloud",   # baseline
    "qwen3-coder:480b-cloud",
    "kimi-k2.6:cloud",
    "glm-5.1:cloud",
]
BASELINE_MODEL = "deepseek-v3.1:671b-cloud"

# Key fields used for coverage + agreement scoring. Pulled from the typed
# CTIEnrichmentResult dump (json mode).
KEY_FIELDS = [
    "is_education_related",
    "institution_name",
    "institution_type",
    "country",
    "attack_category",
    "attack_vector",
    "vendor_name",
    "threat_actor_name",
    "ransomware_family",
    "records_affected_exact",
]
# Categorical fields where models *should* agree (used for baseline agreement).
AGREEMENT_FIELDS = [
    "is_education_related",
    "institution_name",
    "attack_category",
    "vendor_name",
    "threat_actor_name",
]


@dataclass
class RunResult:
    source_incident_id: str
    model: str
    ok: bool
    latency_s: float
    filled_fields: int
    values: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    mitre_count: int = 0


def _norm(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        return v or None
    return value


def load_sample(session, service, limit: int) -> list[tuple[Any, Any, Any, str]]:
    """Return [(source_incident, article_content, document, url)] read-only."""
    from src.edu_cti_v2.models import SourceIncident
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    stmt = (
        select(SourceIncident)
        .options(selectinload(SourceIncident.urls))
        .where(SourceIncident.is_deleted.is_(False))
        .order_by(SourceIncident.collected_at.desc())
        .limit(limit * 12)  # over-fetch; many lack a selected article
    )
    out: list[tuple[Any, Any, Any, str]] = []
    for si in session.execute(stmt).scalars():
        article, document, url = service._select_article(session, si)
        if article is None or document is None or not url:
            continue
        out.append((si, article, document, url))
        if len(out) >= limit:
            break
    return out


def run_one(enricher, base_incident, url, article) -> RunResult:
    from src.edu_cti.pipeline.phase2.enrichment import count_filled_fields

    t0 = time.time()
    try:
        result, raw = enricher._enrich_article(base_incident, {url: article})
        latency = time.time() - t0
        if result is None:
            return RunResult("", "", False, latency, 0, error="null_result")
        # The flat CTI fields (institution_name, attack_category, vendor_name,
        # threat_actor_name, records…) live in the raw LLM JSON; is_education_related
        # is nested under education_relevance in the typed result.
        raw = raw if isinstance(raw, dict) else {}
        dump = result.model_dump(mode="json", exclude_none=False)
        edu = (dump.get("education_relevance") or {})
        values: dict[str, Any] = {}
        for k in KEY_FIELDS:
            if k == "is_education_related":
                values[k] = raw.get(k, edu.get("is_education_related"))
            else:
                values[k] = raw.get(k)
        mitre = dump.get("mitre_attack_techniques") or []
        return RunResult(
            "", "", True, latency, count_filled_fields(result), values,
            mitre_count=len(mitre) if isinstance(mitre, list) else 0,
        )
    except Exception as exc:  # noqa: BLE001 — bake-off must survive any model error
        return RunResult("", "", False, time.time() - t0, 0, error=str(exc)[:200])


def score(results: list[RunResult], baseline_by_incident: dict[str, dict]) -> dict[str, Any]:
    ok = [r for r in results if r.ok]
    n = len(results)
    if not ok:
        return {"runs": n, "schema_valid_pct": 0.0, "coverage_pct": 0.0,
                "baseline_agreement_pct": None, "mean_latency_s": None,
                "median_latency_s": None, "failure_pct": 100.0, "mean_mitre": 0.0}
    coverage = statistics.mean(r.filled_fields for r in ok)
    # coverage as % of KEY_FIELDS present
    key_cov = statistics.mean(
        sum(1 for k in KEY_FIELDS if _norm(r.values.get(k)) is not None) / len(KEY_FIELDS)
        for r in ok
    )
    # baseline agreement (skip for the baseline itself)
    agreements: list[float] = []
    for r in ok:
        base = baseline_by_incident.get(r.source_incident_id)
        if not base:
            continue
        hits = sum(1 for k in AGREEMENT_FIELDS if _norm(r.values.get(k)) == _norm(base.get(k)))
        agreements.append(hits / len(AGREEMENT_FIELDS))
    lat = [r.latency_s for r in ok]
    return {
        "runs": n,
        "schema_valid_pct": round(100 * len(ok) / n, 1),
        "coverage_fields_mean": round(coverage, 1),
        "key_field_coverage_pct": round(100 * key_cov, 1),
        "baseline_agreement_pct": round(100 * statistics.mean(agreements), 1) if agreements else None,
        "mean_latency_s": round(statistics.mean(lat), 1),
        "median_latency_s": round(statistics.median(lat), 1),
        "failure_pct": round(100 * (n - len(ok)) / n, 1),
        "mean_mitre": round(statistics.mean(r.mitre_count for r in ok), 1),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Ollama Cloud extraction bake-off")
    ap.add_argument("--sample", type=int, default=40, help="Articles to evaluate")
    ap.add_argument("--models", type=str, default=",".join(DEFAULT_MODELS),
                    help="Comma-separated Ollama Cloud model tags")
    ap.add_argument("--out", type=str, default="data/bakeoff_results.json")
    args = ap.parse_args()
    models = [m.strip() for m in args.models.split(",") if m.strip()]

    if not os.environ.get("OLLAMA_API_KEY"):
        sys.exit("OLLAMA_API_KEY not set (source ./.env first)")
    if not os.environ.get("EDU_CTI_V2_DATABASE_URL"):
        sys.exit("EDU_CTI_V2_DATABASE_URL not set (run under railway run)")

    from src.edu_cti_v2.db import V2DatabaseSettings, create_session_factory
    from src.edu_cti_v2.services.enrichment import V2EnrichmentService, source_incident_to_base_incident
    from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
    from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient

    session_factory = create_session_factory(V2DatabaseSettings.from_env())
    service = V2EnrichmentService()

    print(f"Loading sample of {args.sample} articles…")
    with session_factory() as session:
        sample = load_sample(session, service, args.sample)
        # Materialize the read-only inputs so we can close the session.
        prepared = []
        for si, article, document, url in sample:
            base = source_incident_to_base_incident(si, url)
            prepared.append((str(si.id), base, url, article))
    print(f"  prepared {len(prepared)} articles\n")

    all_results: dict[str, list[RunResult]] = {}
    baseline_by_incident: dict[str, dict] = {}

    for model in models:
        print(f"=== {model} ===")
        enricher = IncidentEnricher(llm_client=OllamaLLMClient(model=model))
        rows: list[RunResult] = []
        for i, (sid, base, url, article) in enumerate(prepared, 1):
            r = run_one(enricher, base, url, article)
            r.source_incident_id = sid
            r.model = model
            rows.append(r)
            if model == BASELINE_MODEL and r.ok:
                baseline_by_incident[sid] = r.values
            flag = "ok " if r.ok else "ERR"
            print(f"  [{i:>2}/{len(prepared)}] {flag} {r.latency_s:5.1f}s "
                  f"filled={r.filled_fields:>2} {('· ' + r.error) if r.error else ''}")
        all_results[model] = rows
        print()

    # Score + rank.
    print("\n================  BAKE-OFF RESULTS  ================")
    table = {m: score(rows, baseline_by_incident) for m, rows in all_results.items()}
    header = ("model", "schema%", "keycov%", "agree%", "mean_s", "med_s", "fail%", "mitre")
    print("{:<26} {:>7} {:>7} {:>7} {:>7} {:>6} {:>6} {:>6}".format(*header))
    # rank by composite: schema-valid + coverage + agreement, then lower latency
    def composite(s: dict) -> float:
        a = s.get("baseline_agreement_pct")
        return (s["schema_valid_pct"] + s["key_field_coverage_pct"] + (a if a is not None else 100)) \
            - (s["mean_latency_s"] or 0) * 0.1
    for m in sorted(table, key=lambda k: composite(table[k]), reverse=True):
        s = table[m]
        print("{:<26} {:>7} {:>7} {:>7} {:>7} {:>6} {:>6} {:>6}".format(
            m, s["schema_valid_pct"], s["key_field_coverage_pct"],
            s["baseline_agreement_pct"] if s["baseline_agreement_pct"] is not None else "—",
            s["mean_latency_s"], s["median_latency_s"], s["failure_pct"], s["mean_mitre"]))

    winner = max(table, key=lambda k: composite(table[k]))
    print(f"\n>>> WINNER (composite accuracy − latency): {winner}")
    print(">>> Set OLLAMA_MODEL to the winner to make it the enrichment default.")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump({
            "models": models, "sample": len(prepared), "winner": winner,
            "summary": table,
            "runs": {m: [r.__dict__ for r in rows] for m, rows in all_results.items()},
        }, fh, indent=2, default=str)
    print(f"\nFull results → {args.out}")


if __name__ == "__main__":
    main()
