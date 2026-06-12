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

## Gold-set sizing & sampling (the methodology)

**Sample size depends on absolute n, not a fraction of the corpus.** A poll of 1,000 people
estimates a country of 1M or 300M equally well; likewise here, whether the pipeline holds 14k or
30k or 300k source incidents, the eval needs the *same* labelled n. Targets (95% confidence):

| precision wanted | n needed (worst case) | n if accuracy ~90% |
|---|---|---|
| ±10pp | ~100 | ~35 |
| ±7pp  | ~200 | ~70 |
| ±5pp  | ~385 | ~140 |

Two sampling rules this repo follows:
1. **Oversample positives for the title classifier.** The corpus is only ~9% relevant, so a uniform
   draw is almost all negatives and gives a useless *recall* estimate. `build_gold.py` pulls half
   from each relevance bucket; precision and recall are reported separately.
2. **Stratify extractions** across institution type, source group, country (incl. non-English),
   and attack category.

**Two sets, kept separate:**
- **`gold/{titles,extractions}.jsonl`** — the representative, stratified sample → the *headline*
  P/R/F1 with confidence intervals.
- **`gold/hardcases_{titles,extractions}.jsonl`** — deliberately adversarial cases (plural "Public
  Schools", "Institute of Technology", EdTech/LMS, academic-medical, general-security negatives).
  Reported **separately** so their accuracy is never mistaken for the system's true accuracy.

### Label provenance (be honest in the paper)

Current labels carry `label_source: "model_assisted_v1"` — adjudicated against the **source text**
(title+snippet for relevance, the article body for extraction), *independently of the pipeline's own
prediction*. For extraction this is essentially reading-comprehension of a fixed article (strong);
for relevance it follows a written rubric. For a top-tier publication claim, do a **human review
pass** over these labels (review, not from-scratch — `build_gold.py` pre-fills the pipeline guess)
and bump `label_source`.

### To grow it
```bash
EDUTHREAT_ADMIN_PASS=... python -m evals.build_gold --titles 200 --extractions 60
# -> evals/gold/_candidates_*.jsonl  (pipeline guess pre-filled, label slot empty)
# label the label_relevant / expected slots from the source, then promote into the gold files.
```
A field whose gold value is `null` is **not scored** (we only score what we confidently labelled),
so partial extraction labels are fine.

## How this maps to the research paper

The classifier P/R/F1, extraction field-accuracy, and faithfulness/hallucination numbers are exactly
the validation evidence the paper's evaluation/threats-to-validity sections need — this harness *is*
the reproducible form of the manual validation study.

## Tooling note

The metrics here are a small, dependency-free implementation so the harness runs anywhere. The
faithfulness layer is a lightweight LLM-as-judge; for named, citable metrics you can drop in
**DeepEval** (pytest-native faithfulness/hallucination) or **Ragas**, and **promptfoo** for
declarative prompt-diffing — all standard LLM-eval tools that plug into this same gold set.
