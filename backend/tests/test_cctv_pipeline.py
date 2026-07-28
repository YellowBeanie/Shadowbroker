"""Regression tests for CCTV ingestion and persistence."""

import sqlite3
import threading
from unittest.mock import patch

from services import cctv_pipeline


class DummyIngestor(cctv_pipeline.BaseCCTVIngestor):
    def __init__(self, cameras):
        self._cameras = cameras

    def fetch_data(self):
        return self._cameras


def test_ingestor_can_run_from_another_thread(tmp_path, monkeypatch):
    db_path = tmp_path / "data" / "cctv.db"
    monkeypatch.setattr(cctv_pipeline, "DB_PATH", db_path)
    cctv_pipeline.init_db()

    ingestor = DummyIngestor(
        [
            {
                "id": "cam-1",
                "source_agency": "Test",
                "lat": 51.5,
                "lon": -0.12,
                "direction_facing": "North",
                "media_url": "https://example.com/camera.jpg",
                "refresh_rate_seconds": 30,
            }
        ]
    )

    thread = threading.Thread(target=ingestor.ingest)
    thread.start()
    thread.join()

    cameras = cctv_pipeline.get_all_cameras()
    assert len(cameras) == 1
    assert cameras[0]["id"] == "cam-1"
    assert cameras[0]["media_type"] == "image"


def test_ingest_updates_existing_rows_in_persistent_data_dir(tmp_path, monkeypatch):
    db_path = tmp_path / "persistent" / "cctv.db"
    monkeypatch.setattr(cctv_pipeline, "DB_PATH", db_path)
    cctv_pipeline.init_db()

    DummyIngestor(
        [
            {
                "id": "cam-2",
                "source_agency": "Test",
                "lat": 40.71,
                "lon": -74.0,
                "direction_facing": "East",
                "media_url": "https://example.com/old.jpg",
                "refresh_rate_seconds": 60,
            }
        ]
    ).ingest()
    DummyIngestor(
        [
            {
                "id": "cam-2",
                "source_agency": "Test",
                "lat": 40.71,
                "lon": -74.0,
                "direction_facing": "East",
                "media_url": "https://example.com/live.m3u8",
                "refresh_rate_seconds": 60,
            }
        ]
    ).ingest()

    cameras = cctv_pipeline.get_all_cameras()
    assert db_path.exists()
    assert len(cameras) == 1
    assert cameras[0]["media_url"] == "https://example.com/live.m3u8"
    assert cameras[0]["media_type"] == "hls"


def test_scheduled_cctv_ingestors_include_asfinag_and_alpr():
    names = {ing.__class__.__name__ for ing, _ in cctv_pipeline.scheduled_cctv_ingestors()}
    assert "AsfinagIngestor" in names
    assert "OSMALPRCameraIngestor" in names
    assert "OSMTrafficCameraIngestor" in names
    assert "Ontario511Ingestor" in names
    assert "Alberta511Ingestor" in names
    assert "Florida511Ingestor" in names
    assert "AustraliaLiveTrafficIngestor" in names
    # NetherlandsRWSIngestor is disabled: opendata.ndw.nu 404 permanent (2026-07-27)
    assert "NetherlandsRWSIngestor" not in names
    assert len(names) == 20


def test_fetch_traveliq_v2_cameras_parses_views(monkeypatch):
    class FakeResp:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "Id": 9,
                    "Latitude": 45.0,
                    "Longitude": -75.0,
                    "Location": "Test Highway",
                    "Views": [
                        {
                            "Id": 42,
                            "Url": "/map/Cctv/42",
                            "Status": "Enabled",
                            "Description": "Northbound",
                        }
                    ],
                }
            ]

    monkeypatch.setattr(cctv_pipeline, "fetch_with_curl", lambda *a, **k: FakeResp())
    cameras = cctv_pipeline._fetch_traveliq_v2_cameras(
        api_url="https://511on.ca/api/v2/get/cameras",
        base_url="https://511on.ca",
        id_prefix="ON511",
        source_agency="511 Ontario",
    )
    assert len(cameras) == 1
    assert cameras[0]["id"] == "ON511-9-42"
    assert cameras[0]["media_url"] == "https://511on.ca/map/Cctv/42"


def test_ensure_https_upgrades_http_media_urls():
    assert (
        cctv_pipeline._ensure_https_url("http://example.com/camera.jpg")
        == "https://example.com/camera.jpg"
    )
    assert (
        cctv_pipeline._ensure_https_url("https://secure.example.com/live.m3u8")
        == "https://secure.example.com/live.m3u8"
    )


# ---------------------------------------------------------------------------
# Regression tests: SQLite WAL + timeout (incident 2026-07-27)
# ---------------------------------------------------------------------------

_CAM_TEMPLATE = {
    "source_agency": "Test",
    "lat": 51.5,
    "lon": -0.12,
    "direction_facing": "North",
    "media_url": "https://example.com/camera.jpg",
    "refresh_rate_seconds": 30,
}


def test_init_db_enables_wal_mode(tmp_path, monkeypatch):
    """init_db() must switch cctv.db to WAL journal mode (persists in file)."""
    db_path = tmp_path / "data" / "cctv.db"
    monkeypatch.setattr(cctv_pipeline, "DB_PATH", db_path)

    cctv_pipeline.init_db()

    conn = sqlite3.connect(str(db_path))
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()
    assert mode == "wal", f"Expected wal, got {mode!r}"


def test_concurrent_writers_do_not_raise_locked(tmp_path, monkeypatch):
    """Two ingestors writing simultaneously must not raise 'database is locked'.

    With WAL mode and a 30 s timeout, concurrent writers queue instead of
    failing immediately.
    """
    db_path = tmp_path / "data" / "cctv.db"
    monkeypatch.setattr(cctv_pipeline, "DB_PATH", db_path)
    cctv_pipeline.init_db()

    errors: list[Exception] = []

    def write(cam_id: str) -> None:
        try:
            DummyIngestor([{**_CAM_TEMPLATE, "id": cam_id}]).ingest()
        except Exception as exc:
            errors.append(exc)

    # Use distinct ID prefixes ("TFL-*" and "SGP-*") so neither writer's
    # prefix-based DELETE removes the other's rows.
    t1 = threading.Thread(target=write, args=("TFL-1",))
    t2 = threading.Thread(target=write, args=("SGP-1",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert not errors, f"Unexpected errors from concurrent writers: {errors}"
    cameras = cctv_pipeline.get_all_cameras()
    assert {c["id"] for c in cameras} == {"TFL-1", "SGP-1"}


def test_ingestor_opens_db_connection_after_fetch_data(tmp_path, monkeypatch):
    """The DB connection must be opened only after fetch_data() returns.

    This ensures the SQLite write lock is held for the shortest possible
    window and does not overlap with the upstream network call.
    """
    db_path = tmp_path / "data" / "cctv.db"
    monkeypatch.setattr(cctv_pipeline, "DB_PATH", db_path)
    cctv_pipeline.init_db()

    call_order: list[str] = []

    class OrderTrackingIngestor(cctv_pipeline.BaseCCTVIngestor):
        def fetch_data(self):
            call_order.append("fetch_data")
            return []

    original_connect = cctv_pipeline.sqlite3.connect

    def tracking_connect(*args, **kwargs):
        call_order.append("connect")
        return original_connect(*args, **kwargs)

    with patch.object(cctv_pipeline.sqlite3, "connect", tracking_connect):
        OrderTrackingIngestor().ingest()

    assert "fetch_data" in call_order, "fetch_data was never called"
    assert "connect" in call_order, "sqlite3.connect was never called"
    fetch_idx = call_order.index("fetch_data")
    connect_idx = call_order.index("connect")
    assert fetch_idx < connect_idx, (
        f"DB connect (pos {connect_idx}) must come after fetch_data (pos {fetch_idx})"
    )
