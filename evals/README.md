# EduThreat-CTI — LLM Evaluation Harness

Offline, repeatable **AI evaluation** for the pipeline's three LLM components. This is the
practice tech teams call "LLM evals": run the model against a **fixed, human-labelled gold set**,
score it with quantitative metrics, and track those numbers across prompt/model changes so
regressions are caught before they ship.

## What it evaluates

| Component | What | Metrics |
|-----------|------|---------|
| **Title classifier** (gate 1) | `V2TitleClassificationService` — is a news title an education-sector cyber incident? | precision · recall · F1 · confusion matrix vs **human** labels |
| **Extraction** (gate 2 + fields) | `IncidentEnricher._enrich_article` — pull institution / date / country / category / actor + `is_edu_cyber` | per-field exact + fuzzy accuracy; `is_edu_cyber` precision/recall |
| **Faithfulness** (LLM-as-judge) | are the extracted fields *grounded* in the article? | faithfulness % · hallucination % |

These mirror, but go beyond, the live `/api/admin/v2/classifier-quality` endpoint: that endpoint
measures the title gate against the **gate-2 LLM proxy**; the eval here measures both gates against
**human gold labels** — the real ground truth.

## Layout

```
evals/
  metrics.py          # pure-Python scoring: precision/recall/F1, exact/fuzzy match, faithfulness agg
  run_eval.py         # runner: calls the REAL LLM components, writes a JSON + markdown report
  gold/
    titles.jsonl      # human-labelled title relevance (edu-cyber yes/no), stratified + hard cases
    extractions.jsonl # article -> expected fields, incl. a non-education negative control
  reports/            # timestamped run reports (JSON + .md)
tests/evals/          # tier-1 CI gate: metric unit tests + gold-set schema validation (NO LLM)
```

## Running it

```bash
# Needs OLLAMA_API_KEY (loaded from .env). Makes real LLM calls.
python -m evals.run_eval                    # full eval -> evals/reports/<ts>.json + .md
python -m evals.run_eval --no-faithfulness  # skip the LLM judge (cheaper)
python -m evals.run_eval --skip-extraction  # classifier only (fast)
python -m evals.run_eval --limit 4          # smoke a few rows
```

## CI: two tiers

- **Tier 1 — every PR** (`ci.yml` pytest job, no key): `tests/evals/` runs the metric unit tests +
  gold-set schema validation. Catches malformed gold / broken scoring before any expensive run.
- **Tier 2 — weekly + on-demand** (`.github/workflows/llm-eval.yml`, needs `OLLAMA_API_KEY`
  secret): the full LLM eval; publishes the precision/recall/F1 + faithfulness summary and uploads
  the report artifact.

## Growing the gold set

The gold sets are seeded with real cases (verified during data-quality monitoring) and known hard
cases — plural "Public Schools", "Institute of Technology", EdTech/LMS vendors, academic medical
centers, and general-security negatives. To expand: sample real incidents via
`GET /api/admin/v2/extraction-samples?random=true`, hand-label the expected fields, and append a
JSONL row. A field whose gold value is `null` is **not scored** (we only score what we confidently
labelled) — so partial labels are fine.

## How this maps to the research paper

The classifier P/R/F1, extraction field-accuracy, and faithfulness/hallucination numbers are exactly
the validation evidence the paper's evaluation/threats-to-validity sections need — this harness *is*
the reproducible form of the manual validation study.

## Tooling note

The metrics here are a small, dependency-free implementation so the harness runs anywhere. The
faithfulness layer is a lightweight LLM-as-judge; for named, citable metrics you can drop in
**DeepEval** (pytest-native faithfulness/hallucination) or **Ragas**, and **promptfoo** for
declarative prompt-diffing — all standard LLM-eval tools that plug into this same gold set.
