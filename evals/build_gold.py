"""Stratified sampler that builds *candidate* rows for the eval gold set.

Produces ready-to-label JSONL from the live admin API, so growing the gold set is
"review/correct the pipeline's guess" rather than "label from scratch":

* **Titles** — pulls a balanced sample by **oversampling positives** (the corpus
  is ~9% relevant, so a uniform draw would be almost all negatives and give a
  useless recall estimate). Splits the request across the ``relevant`` and
  ``irrelevant`` buckets via ``/admin/v2/title-samples``.
* **Extractions** — pulls a uniform-random sample of processed canonicals with
  their article excerpt + the pipeline's extracted fields via
  ``/admin/v2/extraction-samples?random=true``.

Each candidate row carries the raw material + the pipeline's prediction and an
empty label slot. A human (or model-assisted labeller) fills the label slot from
the SOURCE (title+snippet / article text) — independently of the prediction — then
the labelled rows become ``gold/titles.jsonl`` / ``gold/extractions.jsonl``.

Usage:
    python -m evals.build_gold --titles 160 --extractions 40 --out evals/gold
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from pathlib import Path

ADMIN = os.environ.get(
    "EDUTHREAT_ADMIN_BASE", "https://v2-api-production-e3d1.up.railway.app"
) + "/api/admin/v2"
_USER = os.environ.get("EDUTHREAT_ADMIN_USER", "admin")
_PASS = os.environ.get("EDUTHREAT_ADMIN_PASS", "")


def _login() -> str:
    body = json.dumps({"username": _USER, "password": _PASS}).encode()
    req = urllib.request.Request(ADMIN + "/login", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    return json.load(urllib.request.urlopen(req, timeout=30))["session_token"]


def _get(path: str, token: str) -> dict:
    req = urllib.request.Request(ADMIN + path, method="GET")
    req.add_header("X-Session-Token", token)
    return json.load(urllib.request.urlopen(req, timeout=90))


def build_title_candidates(token: str, n: int) -> list[dict]:
    """Half from the relevant bucket, half from irrelevant (oversample positives)."""
    half = max(1, n // 2)
    rel = _get(f"/title-samples?relevance=relevant&limit={half}&random=true", token)["samples"]
    irr = _get(f"/title-samples?relevance=irrelevant&limit={n - half}&random=true", token)["samples"]
    out = []
    for s in rel + irr:
        out.append({
            "id": "t-" + s["source_incident_id"][:8],
            "title": s["raw_title"],
            "snippet": s.get("raw_subtitle") or "",
            "source": s.get("source_name"),
            "pipeline_relevant": s.get("relevance_status") == "relevant",
            "pipeline_score": s.get("title_relevance_score"),
            "label_relevant": None,  # <-- fill from title+snippet, independently
        })
    return out


def build_extraction_candidates(token: str, n: int) -> list[dict]:
    samples = _get(f"/extraction-samples?random=true&limit={min(n, 30)}", token)["samples"]
    # extraction-samples caps at 30/call; page a few times for larger n
    while len(samples) < n:
        more = _get(f"/extraction-samples?random=true&limit=30", token)["samples"]
        seen = {s["canonical_id"] for s in samples}
        samples += [m for m in more if m["canonical_id"] not in seen]
        if not more:
            break
    out = []
    for s in samples[:n]:
        out.append({
            "id": "x-" + s["canonical_id"][:8],
            "title": s.get("raw_title"),
            "article_text": (s.get("article_excerpt") or "")[:1600],
            "pipeline": {
                "institution_name": s.get("institution_name"),
                "incident_date": s.get("incident_date"),
                "country": s.get("country"),
                "attack_category": s.get("attack_category"),
                "threat_actor": s.get("threat_actor_name"),
                "institution_type": s.get("institution_type"),
            },
            "expected": None,  # <-- fill field-by-field from article_text, independently
        })
    return out


def _write(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"  wrote {path}  ({len(rows)} candidates)")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build candidate rows for the eval gold set")
    ap.add_argument("--titles", type=int, default=160)
    ap.add_argument("--extractions", type=int, default=40)
    ap.add_argument("--out", default="evals/gold")
    args = ap.parse_args()
    if not _PASS:
        raise SystemExit("Set EDUTHREAT_ADMIN_PASS (admin password) to pull samples.")

    token = _login()
    out = Path(args.out)
    print(f"[gold] sampling from {ADMIN} ...")
    if args.titles:
        _write(build_title_candidates(token, args.titles), out / "_candidates_titles.jsonl")
    if args.extractions:
        _write(build_extraction_candidates(token, args.extractions), out / "_candidates_extractions.jsonl")
    print("[gold] done — label the `label_relevant` / `expected` slots from the source text, "
          "then promote the labelled rows into titles.jsonl / extractions.jsonl")


if __name__ == "__main__":
    main()
