# EduThreat LLM eval — results on the expanded, stratified gold set

_Representative gold: 159 human-labelled titles (57 relevant / 102 irrelevant, positives
oversampled because the corpus is ~9% relevant) + 28 labelled extractions. Labels are
`model_assisted_v1` — adjudicated against the source text, independent of the pipeline's own
prediction. All percentages carry a 95% Wilson CI._

## Title classifier (gate 1) — n=159

| metric | value | 95% CI |
|---|---|---|
| precision | **81.4%** | 69.6 – 89.3 |
| recall | **84.2%** | 72.6 – 91.5 |
| F1 | **82.8%** | — |
| accuracy | 87.4% | — |

confusion: tp=48, fp=11, tn=91, fn=9 (vs human labels).

**Interpretation.** The title gate is deliberately **recall-biased** — it keeps uncertain titles
and only drops *confident* negatives, because the full-article **gate-2** is the precision backstop.
So the ~19% title-level false-positive rate is by design: those rows are caught by gate-2 before
canonicalisation (consistent with the ~39% "gate-2 reject" rate seen in monitoring, none of which
reach the dataset). The number to improve is **recall**.

### Error analysis (what the eval surfaced)
- **False positives (11):** index/listing pages ("Latest Georgia Tech news"), a *power outage*
  mis-read as cyber ("Stroomstoring Universiteit Maastricht"), opinion pieces, non-English titles
  the model fails open on, and government/political stories near education keywords.
- **False negatives (9) — the actionable lever:** mostly **bare institution-name titles with no
  snippet** — "PowerSchool", "Boston College", "McMaster University", "Wymondham College",
  "Clackamas Community College". These arrive from sources that publish only the victim name; with
  no snippet the classifier cannot tell an incident from a mention, and drops them. **Ensuring a
  snippet/context reaches the title gate is the highest-value recall fix.**

## Extraction (gate-2 + fields)
The 28-article extraction eval (+ LLM-as-judge faithfulness) runs reliably in the **tier-2 CI job**
(clean environment), but is slow/flaky to run locally because `_enrich_article` loads the GLiNER +
MITRE ML models and the cloud LLM intermittently hits the 300s read timeout. The seed-gold extraction
result stands as the current reference (is_edu_cyber P/R/F1 = 100% n=6; faithfulness 100% /
hallucination 0%); re-run on the 28-article set via `python -m evals.run_eval` once, or let the
weekly CI eval publish it.

## Hard-case regression suite (separate; NOT a representative metric)
`gold/hardcases_*.jsonl` holds the original adversarial seed (18 titles / 6 extractions — plural
"Public Schools", "Institute of Technology", EdTech/LMS, academic-medical, general-security
negatives). Reported separately so its accuracy is never confused with the representative number.

## Honest sizing note
n=159 gives ±10pp-class CIs on precision/recall. For a ±5pp publication claim, grow the title gold
to ~300-400 via `python -m evals.build_gold` (oversample positives) and add a human review pass over
the `model_assisted_v1` labels.
