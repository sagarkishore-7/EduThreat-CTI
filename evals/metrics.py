"""Scoring metrics for the EduThreat LLM evaluation harness.

Pure-Python, dependency-free, fully unit-testable. Two families:

* **Binary classification** (title relevance, gate-2 ``is_edu_cyber``):
  precision / recall / F1 + a confusion matrix against *human* gold labels.
* **Field extraction** (institution / date / country / category / actor):
  per-field exact + fuzzy match, aggregated to a field-accuracy report.

The faithfulness / hallucination layer (LLM-as-judge) is aggregated here too,
but the judging itself happens in ``run_eval.py`` (it needs an LLM call).
"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable, Optional, Sequence


# --------------------------------------------------------------------------- #
# Confidence intervals
# --------------------------------------------------------------------------- #
def wilson_interval(k: int, n: int, z: float = 1.96) -> Optional[tuple[float, float]]:
    """Wilson score 95% CI for a binomial proportion (k successes of n).

    Far better than the normal approximation at small n / extreme proportions
    (e.g. 8/8 -> [0.68, 1.0], honestly reflecting how little 8 samples tell you).
    Returns ``None`` when ``n == 0``.
    """
    if n <= 0:
        return None
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, center - margin), min(1.0, center + margin))


def _ci_pct(k: int, n: int) -> Optional[list]:
    ci = wilson_interval(k, n)
    return [round(ci[0] * 100, 1), round(ci[1] * 100, 1)] if ci else None


# --------------------------------------------------------------------------- #
# Binary classification
# --------------------------------------------------------------------------- #
@dataclass
class BinaryScores:
    tp: int = 0
    fp: int = 0
    tn: int = 0
    fn: int = 0

    @property
    def total(self) -> int:
        return self.tp + self.fp + self.tn + self.fn

    @property
    def precision(self) -> Optional[float]:
        denom = self.tp + self.fp
        return self.tp / denom if denom else None

    @property
    def recall(self) -> Optional[float]:
        denom = self.tp + self.fn
        return self.tp / denom if denom else None

    @property
    def f1(self) -> Optional[float]:
        p, r = self.precision, self.recall
        if not p or not r:
            return None
        return 2 * p * r / (p + r)

    @property
    def accuracy(self) -> Optional[float]:
        return (self.tp + self.tn) / self.total if self.total else None

    def as_dict(self) -> dict:
        def pct(v: Optional[float]) -> Optional[float]:
            return round(v * 100, 1) if v is not None else None

        return {
            "confusion": {"tp": self.tp, "fp": self.fp, "tn": self.tn, "fn": self.fn},
            "support": self.total,
            "precision_pct": pct(self.precision),
            "precision_ci95": _ci_pct(self.tp, self.tp + self.fp),
            "recall_pct": pct(self.recall),
            "recall_ci95": _ci_pct(self.tp, self.tp + self.fn),
            "f1_pct": pct(self.f1),
            "accuracy_pct": pct(self.accuracy),
            "accuracy_ci95": _ci_pct(self.tp + self.tn, self.total),
        }


def score_binary(pairs: Iterable[tuple[bool, bool]]) -> BinaryScores:
    """Score ``(gold, predicted)`` boolean pairs (positive class = ``True``)."""
    s = BinaryScores()
    for gold, pred in pairs:
        if gold and pred:
            s.tp += 1
        elif not gold and pred:
            s.fp += 1
        elif not gold and not pred:
            s.tn += 1
        else:
            s.fn += 1
    return s


# --------------------------------------------------------------------------- #
# String / field matching
# --------------------------------------------------------------------------- #
def normalize_text(value: Optional[str]) -> str:
    """Lower, strip accents + punctuation, collapse whitespace — for fuzzy match."""
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


_STOPWORDS = {"the", "of", "a", "an", "at", "in", "for", "and", "school", "district",
              "university", "college", "public", "schools", "inc", "llc"}


def _tokens(value: Optional[str]) -> set:
    return {t for t in normalize_text(value).split() if t and t not in _STOPWORDS}


def fuzzy_match(gold: Optional[str], pred: Optional[str], *, threshold: float = 0.6) -> bool:
    """Token-overlap (Jaccard) match on the *distinctive* tokens of two labels.

    Tolerant of word order, casing, accents, and boilerplate words ("University
    of X" vs "X University") while still distinguishing genuinely different
    institutions. ``threshold`` is the minimum Jaccard overlap to count as a match.
    """
    g, p = _tokens(gold), _tokens(pred)
    if not g and not p:
        return True
    if not g or not p:
        return False
    inter = len(g & p)
    union = len(g | p)
    return (inter / union) >= threshold if union else False


def exact_match(gold: Optional[str], pred: Optional[str]) -> bool:
    return normalize_text(gold) == normalize_text(pred)


def date_match(gold: Optional[str], pred: Optional[str], *, precision: str = "day") -> bool:
    """Compare ISO dates at the requested precision (day / month / year).

    ``None``-vs-``None`` matches; one-sided ``None`` does not.
    """
    if not gold and not pred:
        return True
    if not gold or not pred:
        return False
    take = {"year": 4, "month": 7, "day": 10}.get(precision, 10)
    return str(gold)[:take] == str(pred)[:take]


# --------------------------------------------------------------------------- #
# Field-extraction aggregation
# --------------------------------------------------------------------------- #
@dataclass
class FieldReport:
    field: str
    total: int = 0
    exact: int = 0
    fuzzy: int = 0  # fuzzy includes exact

    def record(self, *, is_exact: bool, is_fuzzy: bool) -> None:
        self.total += 1
        if is_exact:
            self.exact += 1
        if is_fuzzy or is_exact:
            self.fuzzy += 1

    def as_dict(self) -> dict:
        return {
            "field": self.field,
            "support": self.total,
            "exact_pct": round(self.exact / self.total * 100, 1) if self.total else None,
            "fuzzy_pct": round(self.fuzzy / self.total * 100, 1) if self.total else None,
            "fuzzy_ci95": _ci_pct(self.fuzzy, self.total),
        }


# String fields use fuzzy matching; date is handled separately; the rest are exact.
_STRING_FIELDS = {"institution_name", "threat_actor"}
_DATE_FIELDS = {"incident_date"}


def score_fields(
    gold_rows: Sequence[dict],
    pred_rows: Sequence[dict],
    fields: Sequence[str],
    *,
    date_precision: str = "day",
    skip_null_gold: bool = True,
) -> dict[str, FieldReport]:
    """Per-field exact/fuzzy accuracy across aligned gold/predicted dict rows.

    With ``skip_null_gold`` (default), a field whose gold value is ``None`` for a
    given row is not scored for that row — the standard gold-set convention of not
    penalising the model on fields we couldn't confidently label.
    """
    if len(gold_rows) != len(pred_rows):
        raise ValueError("gold_rows and pred_rows must align 1:1")
    reports = {f: FieldReport(f) for f in fields}
    for gold, pred in zip(gold_rows, pred_rows):
        for f in fields:
            g, p = gold.get(f), (pred or {}).get(f)
            if skip_null_gold and g is None:
                continue
            if f in _DATE_FIELDS:
                ok = date_match(g, p, precision=date_precision)
                reports[f].record(is_exact=ok, is_fuzzy=ok)
            elif f in _STRING_FIELDS:
                reports[f].record(is_exact=exact_match(g, p), is_fuzzy=fuzzy_match(g, p))
            else:
                ok = exact_match(g, p)
                reports[f].record(is_exact=ok, is_fuzzy=ok)
    return reports


# --------------------------------------------------------------------------- #
# Faithfulness / hallucination aggregation (judging done in run_eval.py)
# --------------------------------------------------------------------------- #
@dataclass
class FaithfulnessReport:
    judged: int = 0
    grounded: int = 0
    hallucinated: int = 0
    scores: list = field(default_factory=list)

    def record(self, *, grounded: bool, score: Optional[float] = None) -> None:
        self.judged += 1
        if grounded:
            self.grounded += 1
        else:
            self.hallucinated += 1
        if score is not None:
            self.scores.append(score)

    def as_dict(self) -> dict:
        mean = round(sum(self.scores) / len(self.scores), 3) if self.scores else None
        return {
            "judged": self.judged,
            "faithfulness_pct": round(self.grounded / self.judged * 100, 1) if self.judged else None,
            "hallucination_pct": round(self.hallucinated / self.judged * 100, 1) if self.judged else None,
            "mean_score": mean,
        }
