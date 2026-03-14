"""
Multi-language translation layer for EduThreat-CTI.

Cost-effective approach:
- Uses Google Translate (free tier via googletrans or deep_translator)
- Caches all translations in DB to avoid re-translating
- Language detection via langdetect

For production, can be upgraded to DeepL API ($5.49/million chars)
or Google Cloud Translation ($20/million chars).
"""

import hashlib
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Try to import translation libraries (all free/open-source)
try:
    from deep_translator import GoogleTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    TRANSLATOR_AVAILABLE = False
    logger.debug("deep_translator not available. Install with: pip install deep-translator")

try:
    from langdetect import detect as detect_language
    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False
    logger.debug("langdetect not available. Install with: pip install langdetect")


def content_hash(text: str) -> str:
    """Generate a hash for translation caching."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def detect_lang(text: str) -> Optional[str]:
    """
    Detect the language of text.

    Returns:
        ISO 639-1 language code (e.g., 'en', 'zh-cn', 'de') or None
    """
    if not LANGDETECT_AVAILABLE:
        return None
    if not text or len(text.strip()) < 20:
        return None
    try:
        return detect_language(text)
    except Exception:
        return None


def translate_text(
    text: str,
    source_lang: str = "auto",
    target_lang: str = "en",
    conn=None,
) -> Tuple[str, str]:
    """
    Translate text to English (or target language).

    Uses DB cache to avoid re-translating.

    Args:
        text: Text to translate
        source_lang: Source language code (or 'auto' for detection)
        target_lang: Target language code
        conn: Optional DB connection for caching

    Returns:
        Tuple of (translated_text, detected_language)
    """
    if not text or len(text.strip()) < 10:
        return text, "en"

    # Detect language if auto
    if source_lang == "auto":
        detected = detect_lang(text)
        if detected and detected.startswith("en"):
            return text, "en"  # Already English
        source_lang = detected or "auto"

    # Check cache
    text_hash = content_hash(text)
    if conn:
        try:
            from src.edu_cti.core.db import get_cached_translation
            cached = get_cached_translation(conn, text_hash)
            if cached:
                return cached, source_lang
        except Exception:
            pass

    # Translate
    if not TRANSLATOR_AVAILABLE:
        logger.debug("Translation not available - returning original text")
        return text, source_lang or "unknown"

    try:
        # Chunk long text (Google Translate has 5000 char limit)
        max_chunk = 4500
        if len(text) <= max_chunk:
            translated = GoogleTranslator(
                source=source_lang if source_lang != "auto" else "auto",
                target=target_lang,
            ).translate(text)
        else:
            # Split by paragraphs and translate chunks
            chunks = _split_text(text, max_chunk)
            translated_chunks = []
            for chunk in chunks:
                t = GoogleTranslator(
                    source=source_lang if source_lang != "auto" else "auto",
                    target=target_lang,
                ).translate(chunk)
                translated_chunks.append(t or chunk)
            translated = "\n".join(translated_chunks)

        # Cache result
        if conn and translated:
            try:
                from src.edu_cti.core.db import cache_translation
                cache_translation(
                    conn,
                    original_text=text[:10000],  # Cap storage
                    original_language=source_lang or "unknown",
                    translated_text=translated[:10000],
                    content_hash=text_hash,
                    engine="google",
                )
            except Exception as e:
                logger.debug(f"Failed to cache translation: {e}")

        return translated or text, source_lang or "unknown"

    except Exception as e:
        logger.warning(f"Translation failed: {e}")
        return text, source_lang or "unknown"


def _split_text(text: str, max_size: int) -> list:
    """Split text into chunks at paragraph boundaries."""
    paragraphs = text.split("\n")
    chunks = []
    current = ""

    for para in paragraphs:
        if len(current) + len(para) + 1 > max_size:
            if current:
                chunks.append(current)
            current = para
        else:
            current = current + "\n" + para if current else para

    if current:
        chunks.append(current)

    return chunks
