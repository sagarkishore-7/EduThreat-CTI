import time
from typing import Any, Optional

_cache: dict[str, tuple[float, Any]] = {}

def cache_get(key: str, ttl_seconds: int = 300) -> Optional[Any]:
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < ttl_seconds:
            return val
        del _cache[key]
    return None

def cache_set(key: str, value: Any):
    _cache[key] = (time.time(), value)

def cache_invalidate(prefix: str = ""):
    keys = [k for k in _cache if k.startswith(prefix)]
    for k in keys:
        del _cache[k]
