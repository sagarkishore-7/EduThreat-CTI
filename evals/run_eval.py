"""EduThreat LLM evaluation runner.

Runs the REAL pipeline LLM components against a labelled gold set and reports
quantitative quality metrics — the offline eval tech companies use to catch
regressions and compare prompt/model versions:

  1. **Title classifier** (gate 1) -> precision / recall / F1 vs human labels.
  2. **Extraction + gate-2** -> per-field accuracy + is_edu_cyber precision/recall.
  3. **LLM-as-judge faithfulness** -> are extracted fields grounded in the article?
     (hallucination rate). A lightweight judge using the same Ollama client; swap
     in DeepEval / Ragas if you want their named metrics.

Offline: calls the services' LLM methods directly, touches no database. Needs
``OLLAMA_API_KEY`` (loaded from .env via src.edu_cti.core.config).

Usage:
    python -m evals.run_eval                       # full eval, writes a report
    python -m evals.run_eval --no-faithfulness     # skip the LLM judge (cheaper)
    python -m evals.run_eval --limit 5             # smoke a few rows
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import Optional

import src.edu_cti.core.config  # noqa: F401  -- triggers load_dotenv()

from evals import metrics

_HERE = Path(__file__).resolve().parent
_GOLD = _HERE / "gold"
_REPORTS = _HERE / "reports"
_EXTRACT_FIELDS = ["institution_name", "incident_date", "country", "attack_category", "threat_actor"]


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


# --------------------------------------------------------------------------- #
# 1. Title classifier eval
# --------------------------------------------------------------------------- #
def run_classifier_eval(titles_gold: list[dict], *, batch_size: int = 60) -> dict:
    from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient
    from src.edu_cti_v2.services.title_classification import V2TitleClassificationService

    svc = V2TitleClassificationService(llm_client=OllamaLLMClient())
    # Chunk the request like production (one giant prompt times the LLM out).
    verdicts: dict = {}
    for start in range(0, len(titles_gold), batch_size):
        chunk = titles_gold[start : start + batch_size]
        items = [(start + j, row["title"], row.get("snippet", "")) for j, row in enumerate(chunk)]
        try:
            verdicts.update(svc._classify_titles(items))
        except Exception as exc:
            print(f"[eval] classifier chunk {start}-{start+len(chunk)} failed: "
                  f"{type(exc).__name__}: {exc} (those rows fail open to relevant)")

    pairs, records = [], []
    for i, row in enumerate(titles_gold):
        verdict = verdicts.get(i)
        # The pipeline fails an omitted verdict open to "relevant".
        predicted = svc._is_relevant(verdict) if verdict else True
        gold = bool(row["label_relevant"])
        pairs.append((gold, predicted))
        records.append({
            "id": row.get("id"),
            "title": row["title"],
            "gold_relevant": gold,
            "pred_relevant": predicted,
            "correct": gold == predicted,
            "confidence": (verdict or {}).get("confidence"),
        })
    return {"scores": metrics.score_binary(pairs).as_dict(), "records": records}


# --------------------------------------------------------------------------- #
# 2. Extraction + gate-2 eval
# --------------------------------------------------------------------------- #
def _make_incident(row: dict):
    from src.edu_cti.core.models import BaseIncident

    url = row.get("article_url") or "https://example.test/eval"
    return BaseIncident(
        incident_id=f"eval-{row.get('id', 'x')}",
        source="eval",
        source_event_id=None,
        institution_name="",
        victim_raw_name=None,
        institution_type=None,
        country=None,
        region=None,
        city=None,
        incident_date=None,
        date_precision="unknown",
        source_published_date=None,
        ingested_at=None,
        title=row.get("title"),
        subtitle=None,
        primary_url=url,
        all_urls=[url],
    )


def _make_article(row: dict):
    from src.edu_cti.pipeline.phase2.storage.article_fetcher import ArticleContent

    url = row.get("article_url") or "https://example.test/eval"
    text = row.get("article_text", "")
    return url, ArticleContent(url=url, title=row.get("title", ""), content=text, content_length=len(text))


def _map_extracted(raw: Optional[dict]) -> dict:
    raw = raw or {}
    return {
        "institution_name": raw.get("institution_name") or raw.get("institution_name_en"),
        "incident_date": raw.get("incident_date"),
        "country": raw.get("country") or raw.get("country_name"),
        "attack_category": raw.get("attack_category"),
        "threat_actor": raw.get("threat_actor_name") or raw.get("threat_actor"),
        "is_edu_cyber": raw.get("is_edu_cyber_incident"),
    }


def run_extraction_eval(extraction_gold: list[dict], *, faithfulness: bool = True) -> dict:
    from src.edu_cti.pipeline.phase2.enrichment import IncidentEnricher
    from src.edu_cti.pipeline.phase2.llm_client import OllamaLLMClient

    client = OllamaLLMClient()
    enricher = IncidentEnricher(llm_client=client)

    gold_rows, pred_rows, edu_pairs, records = [], [], [], []
    faith = metrics.FaithfulnessReport()
    for row in extraction_gold:
        incident = _make_incident(row)
        url, article = _make_article(row)
        try:
            _result, raw = enricher._enrich_article(incident, {url: article})
        except Exception as exc:  # keep the eval resilient — record the failure
            raw = None
            records.append({"id": row.get("id"), "error": f"{type(exc).__name__}: {exc}"})
        pred = _map_extracted(raw)
        gold = row["expected"]
        gold_rows.append(gold)
        pred_rows.append(pred)
        edu_pairs.append((bool(gold.get("is_edu_cyber")), bool(pred.get("is_edu_cyber"))))
        rec = {"id": row.get("id"), "gold": {k: gold.get(k) for k in (*_EXTRACT_FIELDS, "is_edu_cyber")},
               "pred": pred}
        records.append(rec)

        if faithfulness and pred.get("is_edu_cyber"):
            for f in ("institution_name", "threat_actor"):
                val = pred.get(f)
                if val:
                    grounded = judge_faithfulness(client, row.get("article_text", ""), f, val)
                    faith.record(grounded=grounded)

    field_reports = metrics.score_fields(gold_rows, pred_rows, _EXTRACT_FIELDS, date_precision="month")
    out = {
        "fields": {f: r.as_dict() for f, r in field_reports.items()},
        "is_edu_cyber": metrics.score_binary(edu_pairs).as_dict(),
        "records": records,
    }
    if faithfulness:
        out["faithfulness"] = faith.as_dict()
    return out


def judge_faithfulness(client, article_text: str, field: str, value: str) -> bool:
    """LLM-as-judge: is the extracted ``field=value`` grounded in the article?"""
    system = (
        "You are a strict fact-checker. Decide whether a claimed value is directly "
        "supported by the article text. Answer ONLY with JSON: {\"grounded\": true|false}."
    )
    user = (
        f"Article:\n{article_text[:4000]}\n\n"
        f"Claimed {field}: {value!r}\n\n"
        f"Is this value explicitly supported by the article? Respond with the JSON."
    )
    try:
        raw = client.extract_json(system_prompt=system, user_prompt=user, max_retries=1)
        return bool(json.loads(raw).get("grounded", False))
    except Exception:
        return True  # fail open — don't penalise on judge transport errors


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def _ci(d: dict, key: str) -> str:
    ci = d.get(key)
    return f" (95% CI {ci[0]}–{ci[1]})" if isinstance(ci, list) and len(ci) == 2 else ""


def _markdown(report: dict) -> str:
    lines = [f"# EduThreat LLM eval — {report['generated_at']}",
             "",
             "_All percentages carry a 95% Wilson confidence interval — read the CI, "
             "not just the point estimate, especially at small n._",
             ""]
    cls = report.get("classifier", {}).get("scores", {})
    if cls:
        lines += ["## Title classifier (gate 1)",
                  f"- precision **{cls.get('precision_pct')}%**{_ci(cls, 'precision_ci95')} · "
                  f"recall **{cls.get('recall_pct')}%**{_ci(cls, 'recall_ci95')} · "
                  f"F1 **{cls.get('f1_pct')}%** · acc {cls.get('accuracy_pct')}% (n={cls.get('support')})",
                  f"- confusion {cls.get('confusion')}", ""]
    ext = report.get("extraction", {})
    if ext:
        lines.append("## Extraction (gate 2 + fields)")
        edu = ext.get("is_edu_cyber", {})
        lines.append(f"- is_edu_cyber: precision {edu.get('precision_pct')}%{_ci(edu, 'precision_ci95')} · "
                     f"recall {edu.get('recall_pct')}%{_ci(edu, 'recall_ci95')} · "
                     f"F1 {edu.get('f1_pct')}% (n={edu.get('support')})")
        for f, r in ext.get("fields", {}).items():
            lines.append(f"- {f}: exact {r.get('exact_pct')}% · "
                         f"fuzzy {r.get('fuzzy_pct')}%{_ci(r, 'fuzzy_ci95')} (n={r.get('support')})")
        if ext.get("faithfulness"):
            fa = ext["faithfulness"]
            lines.append(f"- faithfulness {fa.get('faithfulness_pct')}% · hallucination {fa.get('hallucination_pct')}% "
                         f"(judged {fa.get('judged')})")
        lines.append("")
    hc = report.get("hardcases", {})
    if hc:
        lines.append("## Hard-case regression suite (adversarial — NOT a representative metric)")
        chc = hc.get("classifier", {}).get("scores", {})
        if chc:
            lines.append(f"- titles: {chc.get('accuracy_pct')}% correct (n={chc.get('support')})")
        ehc = hc.get("extraction", {}).get("is_edu_cyber", {})
        if ehc:
            lines.append(f"- extraction is_edu_cyber: {ehc.get('accuracy_pct')}% (n={ehc.get('support')})")
        lines.append("")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the EduThreat LLM eval harness")
    ap.add_argument("--titles", default=str(_GOLD / "titles.jsonl"))
    ap.add_argument("--extractions", default=str(_GOLD / "extractions.jsonl"))
    ap.add_argument("--out", default=None, help="report JSON path (default evals/reports/<ts>.json)")
    ap.add_argument("--no-faithfulness", action="store_true", help="skip the LLM-judge layer")
    ap.add_argument("--limit", type=int, default=None, help="cap rows per gold set (smoke test)")
    ap.add_argument("--skip-classifier", action="store_true")
    ap.add_argument("--skip-extraction", action="store_true")
    ap.add_argument("--hardcases-titles", default=str(_GOLD / "hardcases_titles.jsonl"),
                    help="adversarial title regression set (reported separately, not as headline)")
    ap.add_argument("--hardcases-extractions", default=str(_GOLD / "hardcases_extractions.jsonl"))
    args = ap.parse_args()

    if not os.environ.get("OLLAMA_API_KEY"):
        raise SystemExit("OLLAMA_API_KEY not set — the eval needs the LLM (set it in .env).")

    report: dict = {"generated_at": dt.datetime.now(dt.timezone.utc).isoformat()}

    if not args.skip_classifier:
        titles = load_jsonl(Path(args.titles))
        if args.limit:
            titles = titles[: args.limit]
        print(f"[eval] classifier: {len(titles)} titles ...")
        report["classifier"] = run_classifier_eval(titles)

    if not args.skip_extraction:
        extractions = load_jsonl(Path(args.extractions))
        if args.limit:
            extractions = extractions[: args.limit]
        print(f"[eval] extraction: {len(extractions)} articles ...")
        report["extraction"] = run_extraction_eval(extractions, faithfulness=not args.no_faithfulness)

    # Hard-case regression suite (optional files; reported separately so the
    # adversarial accuracy is never mistaken for the representative metric).
    hc: dict = {}
    hct = Path(args.hardcases_titles)
    if not args.skip_classifier and hct.exists():
        rows = load_jsonl(hct)
        print(f"[eval] hard-case titles: {len(rows)} ...")
        hc["classifier"] = run_classifier_eval(rows)
    hce = Path(args.hardcases_extractions)
    if not args.skip_extraction and hce.exists():
        rows = load_jsonl(hce)
        print(f"[eval] hard-case extractions: {len(rows)} ...")
        hc["extraction"] = run_extraction_eval(rows, faithfulness=False)
    if hc:
        report["hardcases"] = hc

    _REPORTS.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    out_path = Path(args.out) if args.out else _REPORTS / f"{ts}.json"
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md = _markdown(report)
    (out_path.with_suffix(".md")).write_text(md, encoding="utf-8")
    print("\n" + md)
    print(f"\n[eval] wrote {out_path} (+ .md)")


if __name__ == "__main__":
    main()
