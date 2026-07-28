import time as _time_mod
from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from limiter import limiter
from auth import require_admin
from services.schemas import HealthResponse
import os

APP_VERSION = os.environ.get("_HEALTH_APP_VERSION", "0.9.82")

router = APIRouter()


def _get_app_version() -> str:
    # Import lazily to avoid circular import; main sets APP_VERSION before including routers
    try:
        import main as _main
        return _main.APP_VERSION
    except Exception:
        return APP_VERSION


_start_time_ref: dict = {"value": None}


def _get_start_time() -> float:
    if _start_time_ref["value"] is None:
        try:
            import main as _main
            _start_time_ref["value"] = _main._start_time
        except Exception:
            _start_time_ref["value"] = _time_mod.time()
    return _start_time_ref["value"]


# Store keys the health payload reads. /api/health is polled by the k8s
# readiness (15s) and liveness (30s) probes, so it must never clone the store:
# ``get_latest_data()`` deep-copies ~100k nested objects on the event loop,
# which froze the endpoint for 15-30s and let the liveness probe SIGKILL the
# pod roughly once a day (incident 2026-07-27). Nothing here reads the copied
# payloads — only ``len()`` and ``last_updated`` — so direct references are
# both correct and effectively free. Do NOT "fix" a slow health check by
# offloading the deepcopy to a thread: it is CPU-bound pure-Python work that
# holds the GIL, so it would still stall the loop.
_HEALTH_SOURCE_KEYS = (
    "commercial_flights",
    "military_flights",
    "ships",
    "satellites",
    "earthquakes",
    "cctv",
    "news",
    "uavs",
    "firms_fires",
    "liveuamap",
    "gdelt",
    "uap_sightings",
)


def _count(store: dict, key: str) -> int:
    """Length of a store entry, tolerating absent/None values.

    ``get_latest_data_subset_refs`` inserts ``None`` for keys the store does
    not have yet, so ``store.get(key, [])`` would return ``None`` rather than
    the default. Mirrors the same guard in ``slo.compute_all_statuses``.
    """
    value = store.get(key)
    return len(value) if hasattr(value, "__len__") else 0


@router.get("/api/health", response_model=HealthResponse)
@limiter.limit("30/minute")
async def health_check(request: Request):
    from services.fetchers._store import (
        get_latest_data_subset_refs,
        get_source_timestamps_snapshot,
    )
    from services.slo import SLO_REGISTRY, compute_all_statuses, summarise_statuses

    # Union of the keys rendered below and those walked by
    # compute_all_statuses(); SLO_REGISTRY is expanded dynamically so newly
    # registered SLO sources stay covered without touching this call.
    d = get_latest_data_subset_refs("last_updated", *_HEALTH_SOURCE_KEYS, *SLO_REGISTRY)
    last = d.get("last_updated")
    timestamps = get_source_timestamps_snapshot()
    slo_statuses = compute_all_statuses(d, timestamps)
    slo_summary = summarise_statuses(slo_statuses)
    # Top-level status reflects worst SLO result — "degraded" if any
    # yellow, "error" if any red, "ok" otherwise. This is the single
    # field an external probe / pager can watch.
    top_status = "ok"
    if slo_summary.get("red", 0) > 0:
        top_status = "error"
    elif slo_summary.get("yellow", 0) > 0:
        top_status = "degraded"

    # Issue #258: surface AIS proxy degraded TLS state so operators can see
    # when the SPKI-pinned fallback is in effect. The data plane keeps
    # flowing (this is by design — see ais_proxy.js comments) but observers
    # who care about MITM-protection posture deserve a visible signal.
    #
    # Plus connectivity health (added 2026-05-23 when stream.aisstream.io
    # went fully offline): ``connected`` tells the frontend whether ship
    # data is actually flowing. When false, a banner explains that ships
    # are unavailable due to an upstream outage — better than the user
    # silently seeing an empty ocean and assuming we broke something.
    ais_status: dict = {}
    try:
        from services.ais_stream import ais_proxy_status
        ais_status = ais_proxy_status() or {}
    except Exception:
        ais_status = {}
    if ais_status.get("degraded_tls") and top_status == "ok":
        # Don't override a worse top-level status if SLOs already failed,
        # but escalate ok -> degraded so the field surfaces in dashboards.
        top_status = "degraded"
    # AIS_API_KEY not configured is "feature off", not "system broken" —
    # so we only escalate when the operator opted into AIS (key set) AND
    # the stream is currently offline.
    if (
        os.environ.get("AIS_API_KEY")
        and ais_status.get("connected") is False
        and top_status == "ok"
    ):
        top_status = "degraded"

    return {
        "status": top_status,
        "version": _get_app_version(),
        "last_updated": last,
        "sources": {
            "flights": _count(d, "commercial_flights"),
            "military": _count(d, "military_flights"),
            "ships": _count(d, "ships"),
            "satellites": _count(d, "satellites"),
            "earthquakes": _count(d, "earthquakes"),
            "cctv": _count(d, "cctv"),
            "news": _count(d, "news"),
            "uavs": _count(d, "uavs"),
            "firms_fires": _count(d, "firms_fires"),
            "liveuamap": _count(d, "liveuamap"),
            "gdelt": _count(d, "gdelt"),
            "uap_sightings": _count(d, "uap_sightings"),
        },
        "freshness": timestamps,
        "uptime_seconds": round(_time_mod.time() - _get_start_time()),
        "slo": slo_statuses,
        "slo_summary": slo_summary,
        "ais_proxy": ais_status,
    }


@router.get("/api/debug-latest", dependencies=[Depends(require_admin)])
@limiter.limit("30/minute")
async def debug_latest_data(request: Request):
    # Key names only — never clone the store to enumerate it (see the note on
    # _HEALTH_SOURCE_KEYS above).
    from services.fetchers._store import get_latest_data_keys

    return get_latest_data_keys()
