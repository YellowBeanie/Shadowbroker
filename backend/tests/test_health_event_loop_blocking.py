"""Regression tests for incident 2026-07-27 — event-loop blocking.

Two handlers used to run blocking work directly on the asyncio event loop:

* ``/api/health`` called ``get_latest_data()``, which deep-copies the whole
  dashboard store (~100k nested objects) only to call ``len()`` on it. Under
  load the copy took 15-30s, the endpoint stopped answering, and the k8s
  liveness probe SIGKILLed the pod roughly once a day.
* ``/api/cctv/media`` called a blocking ``requests.get`` (timeouts up to 20s)
  inline, at 120 req/min.

These tests lock in the invariants so neither regresses. Note the two defects
need *opposite* remedies: the deepcopy must be avoided entirely (it is
CPU-bound and holds the GIL, so a worker thread would not help), while the
CCTV fetch is I/O-bound and belongs in a thread.
"""

import threading

import pytest


_SEEDED = {
    "ships": [{"id": i, "nested": {"trail": [1, 2, 3]}} for i in range(50)],
    "military_flights": [{"id": i} for i in range(7)],
    "news": [{"title": "x"}],
}
_EXPECTED = {"ships": 50, "military": 7, "news": 1}


@pytest.fixture()
def seeded_store():
    """Seed the shared store, then restore it.

    ``latest_data`` is process-global, so a test that seeds it without
    restoring leaks into every test that runs afterwards. Always put back
    exactly what was there before.
    """
    from services.fetchers._store import _data_lock, latest_data

    sentinel = object()
    previous = {key: latest_data.get(key, sentinel) for key in _SEEDED}
    with _data_lock:
        latest_data.update(_SEEDED)
    try:
        yield _EXPECTED
    finally:
        with _data_lock:
            for key, value in previous.items():
                if value is sentinel:
                    latest_data.pop(key, None)
                else:
                    latest_data[key] = value


def test_health_never_deepcopies_the_store(client, monkeypatch, seeded_store):
    """The health path must not touch the deepcopy snapshot at all."""
    expected = seeded_store

    def _tripwire(*args, **kwargs):
        raise AssertionError(
            "/api/health deep-copied the store — see incident 2026-07-27. "
            "Use get_latest_data_subset_refs(); do not offload the deepcopy "
            "to a thread, it is CPU-bound and holds the GIL."
        )

    monkeypatch.setattr(
        "services.fetchers._store.get_latest_data_deepcopy_snapshot", _tripwire
    )

    resp = client.get("/api/health")

    assert resp.status_code == 200
    body = resp.json()
    # Counts must stay accurate now that we read references instead of copies.
    assert body["sources"]["ships"] == expected["ships"]
    assert body["sources"]["military"] == expected["military"]
    assert body["sources"]["news"] == expected["news"]


def test_health_reports_every_slo_source(client, monkeypatch, seeded_store):
    """Subset-refs must cover SLO_REGISTRY, or SLO statuses silently go blank.

    The handler now asks for an explicit key list instead of receiving the
    whole store, so a source registered in SLO_REGISTRY but missing from that
    list would be scored as absent. This guards that coupling.
    """

    def _tripwire(*args, **kwargs):
        raise AssertionError("/api/health deep-copied the store")

    monkeypatch.setattr(
        "services.fetchers._store.get_latest_data_deepcopy_snapshot", _tripwire
    )

    from services.slo import SLO_REGISTRY

    resp = client.get("/api/health")
    assert resp.status_code == 200
    slo = resp.json()["slo"]

    missing = [source for source in SLO_REGISTRY if source not in slo]
    assert not missing, f"SLO sources absent from /api/health payload: {missing}"


def test_get_latest_data_keys_does_not_copy(monkeypatch, seeded_store):
    """Enumerating keys must not clone values either."""
    from services.fetchers._store import get_latest_data_keys

    monkeypatch.setattr(
        "services.fetchers._store.get_latest_data_deepcopy_snapshot",
        lambda *a, **kw: pytest.fail("key enumeration deep-copied the store"),
    )

    keys = get_latest_data_keys()

    assert isinstance(keys, list)
    assert "ships" in keys


def test_cctv_media_proxy_runs_off_the_event_loop(client, monkeypatch):
    """The blocking upstream fetch must happen in a worker thread."""
    from fastapi.responses import Response

    loop_thread = threading.get_ident()
    seen = {}

    def _fake_proxy(request, target_url):
        seen["thread"] = threading.get_ident()
        return Response(content=b"ok", media_type="text/plain")

    monkeypatch.setattr("routers.cctv._cctv_host_allowed", lambda host: True)
    monkeypatch.setattr("routers.cctv._proxy_cctv_media_response", _fake_proxy)

    resp = client.get("/api/cctv/media?url=https://example.com/stream.m3u8")

    assert resp.status_code == 200
    assert "thread" in seen, "the proxy helper was never invoked"
    assert seen["thread"] != loop_thread, (
        "cctv_media_proxy ran the blocking upstream fetch on the event loop "
        "thread — see incident 2026-07-27"
    )
