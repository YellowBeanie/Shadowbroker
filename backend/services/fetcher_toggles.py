"""Operator-side kill switch for individual data fetchers.

``SB_DISABLED_FETCHERS`` is a comma-separated list of fetcher names, with or
without the ``fetch_`` prefix (``satnogs,tinygs`` == ``fetch_satnogs,fetch_tinygs``).
Matching fetchers are removed from the fast/slow tiers before they run, so a
dead or unwanted upstream (e.g. TinyGS timeouts, SatNOGS 400s) stops costing
network calls and error lines without a code change. The corresponding map
layer simply stays empty.

Pure module (no imports from the fetcher graph) so it can be unit-tested and
imported cheaply.
"""

from __future__ import annotations

import logging
import os
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

ENV_VAR = "SB_DISABLED_FETCHERS"


def _normalize(name: str) -> str:
    name = (name or "").strip().lower()
    return name[len("fetch_"):] if name.startswith("fetch_") else name


def disabled_fetchers(raw: str | None = None) -> frozenset[str]:
    """Normalized set of disabled fetcher names from ``raw`` (default: env var)."""
    value = os.environ.get(ENV_VAR, "") if raw is None else raw
    return frozenset(n for n in (_normalize(p) for p in value.split(",")) if n)


def filter_fetchers(
    funcs: Iterable[Callable], disabled: frozenset[str] | None = None, *, tier: str = ""
) -> list[Callable]:
    """Return ``funcs`` minus those named in ``disabled`` (log the skipped ones once per call)."""
    disabled = disabled_fetchers() if disabled is None else disabled
    if not disabled:
        return list(funcs)
    kept, skipped = [], []
    for fn in funcs:
        if _normalize(getattr(fn, "__name__", "")) in disabled:
            skipped.append(getattr(fn, "__name__", str(fn)))
        else:
            kept.append(fn)
    if skipped:
        logger.info("%s fetchers disabled via %s: %s", tier or "tier", ENV_VAR, ", ".join(skipped))
    return kept
