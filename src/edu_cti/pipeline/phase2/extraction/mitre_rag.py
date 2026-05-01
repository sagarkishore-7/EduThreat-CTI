"""
IntelEX-style Retrieval-Augmented Generation for MITRE ATT&CK technique selection.

Inspired by IntelEX (arxiv 2406.01560): semantic retrieval of MITRE technique
descriptions is used to ground the LLM's technique selection in evidence rather
than parametric memory.

How it works:
1. All 697 active ATT&CK technique descriptions are embedded once using
   all-MiniLM-L6-v2 (~90 MB, 384-dim vectors) and cached to DATA_DIR/mitre_embeddings.npy.
2. For each article, the first MAX_ARTICLE_CHARS are embedded (fast: ~10ms on CPU).
3. Cosine similarity finds the top-K most semantically similar techniques.
4. Those technique descriptions are formatted into a compact context block and
   appended to the LLM user prompt — giving the model grounded candidates to
   select from instead of relying on training memory.

Expected impact (from IntelEX paper): reduces technique hallucination and improves
recall for techniques mentioned in indirect or paraphrased language.

Gracefully no-ops if sentence-transformers is not installed or embeddings fail.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

_EMBED_MODEL_ID = "all-MiniLM-L6-v2"
_EMBED_CACHE_FILENAME = "mitre_embeddings.npy"
_INDEX_CACHE_FILENAME = "mitre_embeddings_index.json"  # tid → array row
_MAX_ARTICLE_CHARS = 2_000   # first ~400 words — where attack context is densest
_TOP_K = 5                   # number of candidate techniques to surface per article

# Runtime cache
_embed_model = None
_embed_model_load_failed = False
_technique_embeddings: Optional[np.ndarray] = None   # shape (N, 384)
_technique_index: Optional[List[Dict]] = None        # [{id, name, tactic, description}]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_data_dir() -> Path:
    try:
        from src.edu_cti.core import config
        return Path(config.DATA_DIR)
    except Exception:
        return Path("data")


def _get_hf_cache_dir() -> str:
    hf = _get_data_dir() / "hf_cache"
    hf.mkdir(parents=True, exist_ok=True)
    return str(hf)


def _load_embed_model():
    global _embed_model, _embed_model_load_failed
    if _embed_model_load_failed:
        return None
    if _embed_model is not None:
        return _embed_model
    try:
        os.environ.setdefault("HF_HOME", _get_hf_cache_dir())
        os.environ.setdefault("TRANSFORMERS_CACHE", _get_hf_cache_dir())
        from sentence_transformers import SentenceTransformer
        logger.info("MITRE RAG: loading embedding model %s", _EMBED_MODEL_ID)
        _embed_model = SentenceTransformer(_EMBED_MODEL_ID)
        logger.info("MITRE RAG: embedding model loaded")
        return _embed_model
    except Exception as exc:
        logger.warning("MITRE RAG: embedding model load failed — RAG disabled: %s", exc)
        _embed_model_load_failed = True
        return None


def _build_technique_corpus() -> List[Dict]:
    """Load all active MITRE techniques with descriptions for embedding."""
    from src.edu_cti.pipeline.phase2.extraction.mitre_stix import load_technique_map
    tech_map = load_technique_map()
    corpus = []
    for tid, info in tech_map.items():
        if not info.get("description"):
            continue
        corpus.append({
            "id": tid,
            "name": info["name"],
            "tactic": info["tactic"],
            "description": info["description"],
        })
    return corpus


def _embed_path() -> Path:
    return _get_data_dir() / _EMBED_CACHE_FILENAME


def _index_path() -> Path:
    return _get_data_dir() / _INDEX_CACHE_FILENAME


def _embeddings_are_valid() -> bool:
    """True when both the .npy and .json caches exist and sizes match."""
    ep, ip = _embed_path(), _index_path()
    if not ep.exists() or not ip.exists():
        return False
    try:
        arr = np.load(str(ep), mmap_mode="r")
        with open(ip, "r") as f:
            idx = json.load(f)
        return len(arr) == len(idx) and len(arr) > 0
    except Exception:
        return False


def build_mitre_index(force: bool = False) -> bool:
    """
    Build (or rebuild) the MITRE technique embedding index.

    Downloads the embedding model if needed. Writes two files to DATA_DIR:
    - mitre_embeddings.npy  — float32 array (N, 384)
    - mitre_embeddings_index.json — list of {id, name, tactic, description}

    Returns True on success.
    """
    if not force and _embeddings_are_valid():
        logger.debug("MITRE RAG: embedding index already exists, skipping build")
        return True

    model = _load_embed_model()
    if model is None:
        return False

    corpus = _build_technique_corpus()
    if not corpus:
        logger.warning("MITRE RAG: empty technique corpus, cannot build index")
        return False

    texts = [f"{t['name']}. {t['description']}" for t in corpus]
    logger.info("MITRE RAG: embedding %d technique descriptions...", len(texts))
    try:
        vecs = model.encode(texts, batch_size=64, show_progress_bar=False, convert_to_numpy=True)
        # Normalise for efficient cosine similarity via dot product
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        vecs = vecs / np.maximum(norms, 1e-9)

        np.save(str(_embed_path()), vecs.astype(np.float32))
        with open(_index_path(), "w", encoding="utf-8") as f:
            json.dump(corpus, f, ensure_ascii=False, separators=(",", ":"))

        logger.info("MITRE RAG: index built — %d techniques, shape %s", len(corpus), vecs.shape)
        return True
    except Exception as exc:
        logger.warning("MITRE RAG: index build failed: %s", exc)
        return False


def _load_index() -> Tuple[Optional[np.ndarray], Optional[List[Dict]]]:
    """Load embeddings and metadata index from disk into runtime cache."""
    global _technique_embeddings, _technique_index
    if _technique_embeddings is not None and _technique_index is not None:
        return _technique_embeddings, _technique_index

    if not _embeddings_are_valid():
        if not build_mitre_index():
            return None, None

    try:
        _technique_embeddings = np.load(str(_embed_path()))
        with open(_index_path(), "r", encoding="utf-8") as f:
            _technique_index = json.load(f)
        return _technique_embeddings, _technique_index
    except Exception as exc:
        logger.warning("MITRE RAG: failed to load index: %s", exc)
        return None, None


# ── Public API ────────────────────────────────────────────────────────────────

def retrieve_similar_techniques(article_text: str, top_k: int = _TOP_K) -> List[Dict]:
    """
    Find the top-K MITRE ATT&CK techniques most semantically similar to the article.

    Args:
        article_text: raw article body (first MAX_ARTICLE_CHARS are used)
        top_k: number of candidates to return

    Returns:
        List of dicts [{id, name, tactic, description, score}] sorted by score desc.
        Empty list if embeddings/model unavailable.
    """
    model = _load_embed_model()
    if model is None:
        return []

    embeddings, index = _load_index()
    if embeddings is None or index is None:
        return []

    query_text = article_text[:_MAX_ARTICLE_CHARS].strip()
    if not query_text:
        return []

    try:
        q_vec = model.encode([query_text], show_progress_bar=False, convert_to_numpy=True)[0]
        q_norm = np.linalg.norm(q_vec)
        if q_norm < 1e-9:
            return []
        q_vec = q_vec / q_norm

        scores = embeddings @ q_vec  # dot product = cosine sim (both normalised)
        top_indices = np.argsort(scores)[::-1][:top_k]

        results = []
        for i in top_indices:
            tech = dict(index[int(i)])
            tech["score"] = float(scores[i])
            results.append(tech)
        return results
    except Exception as exc:
        logger.debug("MITRE RAG retrieval error: %s", exc)
        return []


def build_mitre_rag_block(article_text: str, top_k: int = _TOP_K) -> Optional[str]:
    """
    Build a MITRE ATT&CK context block for injection into the LLM user prompt.

    Returns a compact block like:
        === MITRE ATT&CK CANDIDATE TECHNIQUES (semantic retrieval) ===
        T1566 · Phishing · Initial Access
          Adversaries may send phishing messages to gain access to victim systems.
        T1486 · Data Encrypted for Impact · Impact
          Adversaries may encrypt data on target systems or on large numbers of systems...
        ...
        Select technique_id values ONLY from the list above when evidence supports them.
        ===============================================================

    Returns None if retrieval fails or no techniques found.
    """
    candidates = retrieve_similar_techniques(article_text, top_k=top_k)
    if not candidates:
        return None

    lines = ["=== MITRE ATT&CK CANDIDATE TECHNIQUES (from semantic retrieval) ==="]
    for c in candidates:
        lines.append(f"{c['id']} · {c['name']} · {c['tactic']}")
        if c.get("description"):
            lines.append(f"  {c['description'][:120]}")
    lines.append("Prefer technique_id values from this list when article evidence supports them.")
    lines.append("=" * 67)
    return "\n".join(lines)
