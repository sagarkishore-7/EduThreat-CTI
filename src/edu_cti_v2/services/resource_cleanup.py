"""Best-effort cleanup for optional in-process ML resources.

The v2 worker is intentionally long-lived on Railway. Enrichment lazily loads
GLiNER and sentence-transformer helpers, but those module-level caches can keep
hundreds of MB resident after the queue drains. These helpers release only
local process resources; remote Ollama/cloud model state is unaffected.
"""

from __future__ import annotations

import ctypes
import gc
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def _clear_torch_caches() -> None:
    try:
        import torch

        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    except Exception:
        return


def _trim_malloc() -> None:
    """Return freed arenas to the OS on glibc-based Linux containers."""
    if os.name != "posix":
        return
    try:
        libc = ctypes.CDLL("libc.so.6")
        trim = getattr(libc, "malloc_trim", None)
        if trim is not None:
            trim(0)
    except Exception:
        return


def release_idle_ml_resources() -> dict[str, Any]:
    """Release optional ML helper caches loaded by enrichment.

    Returns a small report for logs/tests. This is safe to call repeatedly.
    """
    released: dict[str, Any] = {
        "gliner_model": False,
        "mitre_embed_model": False,
        "mitre_index": False,
    }

    try:
        from src.edu_cti.pipeline.phase2.extraction import ner_preprocessor

        with ner_preprocessor._model_lock:
            if ner_preprocessor._gliner_model is not None:
                ner_preprocessor._gliner_model = None
                released["gliner_model"] = True
    except Exception as exc:
        logger.debug("Idle resource cleanup skipped GLiNER release: %s", exc)

    try:
        from src.edu_cti.pipeline.phase2.extraction import mitre_rag

        with mitre_rag._model_lock:
            if mitre_rag._embed_model is not None:
                mitre_rag._embed_model = None
                released["mitre_embed_model"] = True
            if mitre_rag._technique_embeddings is not None or mitre_rag._technique_index is not None:
                mitre_rag._technique_embeddings = None
                mitre_rag._technique_index = None
                released["mitre_index"] = True
    except Exception as exc:
        logger.debug("Idle resource cleanup skipped MITRE RAG release: %s", exc)

    gc.collect()
    _clear_torch_caches()
    _trim_malloc()
    return released
