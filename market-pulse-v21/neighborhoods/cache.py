"""File-based JSON cache with per-key TTLs.

ZIP-level data moves slowly — Census ACS refreshes annually, Walk Score
rarely changes, restaurant density drifts seasonally — so default TTLs
are measured in days. Each adapter sets its own TTL to match how stale
its data can be without misleading the user.

Storage: /tmp/market_pulse_cache/neighborhoods/<sanitized_key>.json
"""
from __future__ import annotations
import json
import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_ROOT = Path("/tmp/market_pulse_cache/neighborhoods")
CACHE_ROOT.mkdir(parents=True, exist_ok=True)


def _safe_path(key: str) -> Path:
    safe = key.replace("/", "_").replace(":", "_").replace(" ", "_")
    return CACHE_ROOT / f"{safe}.json"


def cache_get(key: str, ttl_days: float):
    """Return cached value if it exists and is younger than ttl_days; else None."""
    p = _safe_path(key)
    if not p.exists():
        return None
    age_days = (time.time() - p.stat().st_mtime) / 86400.0
    if age_days >= ttl_days:
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        logger.warning("Cache read failed for %s: %s", key, e)
        return None


def cache_set(key: str, value):
    """Persist value to the cache. Errors are logged but never raised — a
    failed cache write should never break the request path."""
    p = _safe_path(key)
    try:
        p.write_text(json.dumps(value, default=str))
    except Exception as e:
        logger.warning("Cache write failed for %s: %s", key, e)
