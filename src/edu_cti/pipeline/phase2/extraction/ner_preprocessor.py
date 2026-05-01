"""
GLiNER-based Named Entity Recognition pre-pass for LLM prompt enrichment.

Runs a lightweight zero-shot NER model over raw article text before the main
LLM extraction call to surface institution names, locations, and threat-actor
names as structured hints in the prompt. This reduces null rates for the fields
most frequently missed: institution_name, country, city, region, ransomware_family.

Design decisions:
- Model is loaded once per process and held in memory (~150 MB for small variant).
- Gracefully no-ops if GLiNER is not installed or the model fails to load.
- Text is truncated to MAX_NER_CHARS (8 000) — full articles are not needed;
  NER is most useful on the first few paragraphs where key facts appear.
- HuggingFace cache is redirected to DATA_DIR so it survives Railway container
  restarts on the persistent volume.

Usage in enrichment.py:
    from src.edu_cti.pipeline.phase2.extraction.ner_preprocessor import build_ner_hint_block
    ner_hints = build_ner_hint_block(article_text, title)
    # Inject ner_hints into the LLM system prompt before calling Ollama.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_MODEL_ID = "urchade/gliner_small-v2.1"
_MAX_NER_CHARS = 8_000  # first ~1 600 words — where key facts appear

# Entity types passed to GLiNER (natural-language descriptions)
_ENTITY_TYPES = [
    "educational institution",
    "city",
    "country",
    "US state or Canadian province",
    "threat actor group",
    "ransomware family",
]

# Map GLiNER label → canonical flat_data field
_LABEL_TO_FIELD: Dict[str, str] = {
    "educational institution": "institution_name",
    "city": "city",
    "country": "country",
    "us state or canadian province": "region",
    "threat actor group": "threat_actor",
    "ransomware family": "ransomware_family",
}

# Runtime cache
_gliner_model = None
_gliner_load_failed = False


def _get_hf_cache_dir() -> Optional[str]:
    """Return a path inside DATA_DIR for the HuggingFace model cache."""
    try:
        from src.edu_cti.core import config
        hf_dir = Path(config.DATA_DIR) / "hf_cache"
        hf_dir.mkdir(parents=True, exist_ok=True)
        return str(hf_dir)
    except Exception:
        return None


def _load_model():
    """Load GLiNER model (once per process). Sets _gliner_load_failed on error."""
    global _gliner_model, _gliner_load_failed
    if _gliner_load_failed:
        return None
    if _gliner_model is not None:
        return _gliner_model

    try:
        hf_cache = _get_hf_cache_dir()
        if hf_cache:
            os.environ.setdefault("HF_HOME", hf_cache)
            os.environ.setdefault("TRANSFORMERS_CACHE", hf_cache)

        from gliner import GLiNER
        logger.info("GLiNER: loading model %s (first use — may download ~150 MB)", _MODEL_ID)
        _gliner_model = GLiNER.from_pretrained(_MODEL_ID)
        logger.info("GLiNER: model loaded successfully")
        return _gliner_model
    except Exception as exc:
        logger.warning("GLiNER: model load failed — NER pre-pass disabled: %s", exc)
        _gliner_load_failed = True
        return None


def _clean_text(text: str) -> str:
    """Normalize whitespace and remove HTML artifacts."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_entities(text: str, title: str = "") -> Dict[str, List[str]]:
    """
    Run GLiNER NER on article text and return extracted entities grouped by field.

    Returns a dict mapping field names to lists of extracted text spans.
    Returns {} if GLiNER is unavailable.

    Args:
        text: raw article body text
        title: article title (prepended to text to boost entity prominence)
    """
    model = _load_model()
    if model is None:
        return {}

    combined = f"{title}\n\n{text}" if title else text
    combined = _clean_text(combined)[:_MAX_NER_CHARS]

    try:
        raw_entities = model.predict_entities(combined, _ENTITY_TYPES, threshold=0.5)
    except Exception as exc:
        logger.debug("GLiNER prediction error: %s", exc)
        return {}

    result: Dict[str, List[str]] = {}
    for ent in raw_entities:
        label_key = ent["label"].lower()
        field = _LABEL_TO_FIELD.get(label_key)
        if not field:
            continue
        span = ent["text"].strip()
        if not span or len(span) < 2:
            continue
        result.setdefault(field, [])
        if span not in result[field]:
            result[field].append(span)

    return result


def build_ner_hint_block(text: str, title: str = "") -> Optional[str]:
    """
    Build a structured hint block string for injection into the LLM prompt.

    Returns a multi-line string like:
        === NER PRE-EXTRACTION HINTS ===
        institution_name candidates: ["Albuquerque Public Schools"]
        city candidates: ["Albuquerque"]
        region candidates: ["New Mexico"]
        ransomware_family candidates: ["RansomHub"]
        ================================

    Returns None if no entities were extracted or GLiNER is unavailable.
    """
    entities = extract_entities(text, title)
    if not entities:
        return None

    lines = ["=== NER PRE-EXTRACTION HINTS (from automatic entity recognition) ==="]
    for field, spans in entities.items():
        quoted = ", ".join(f'"{s}"' for s in spans[:3])  # cap at 3 per field
        lines.append(f"{field} candidates: [{quoted}]")
    lines.append("Use these hints to fill empty fields — do not override article evidence.")
    lines.append("=" * 67)
    return "\n".join(lines)


def get_ner_structured(text: str, title: str = "") -> Dict[str, str]:
    """
    Extract entities and return the single most-confident value per field
    (first span for each field, since GLiNER returns spans in document order).

    Useful for direct comparison against LLM output in post-processing.
    """
    entities = extract_entities(text, title)
    return {field: spans[0] for field, spans in entities.items() if spans}
