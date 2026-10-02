"""Tests for the /healthz endpoint (W7: real checks + 503 on degraded)."""

import time

from app import __version__, db
from app.worker import WORKER_STALENESS_MARGIN_S


def test_healthz_structure(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    data = response.json()
    assert set(data.keys()) == {"status", "uptime_s", "version", "counters"}
    assert data["status"] == "ok"
    assert isinstance(data["uptime_s"], (int, float))
    assert data["uptime_s"] >= 0
    assert data["version"] == __version__
    assert isinstance(data["counters"], dict)


def test_healthz_exposes_counters(client, config_file):
    from app import db

    # The app DB lives next to the config file (relative db_path in the fixture).
    db_path = config_file.parent / "data" / "gateway.db"
    conn = db.open_db(str(db_path))
    try:
        db.increment_counter(conn, "messages_in")
    finally:
        conn.close()

    response = client.get("/healthz")
    assert response.status_code == 200
    # M1: the fixture config runs in mock mode, so the startup counters are
    # present alongside the business counters.
    assert response.json()["counters"] == {
        "messages_in": 1,
        "mock_llm": 1,
        "mock_gateway": 1,
    }


# ---------------------------------------------------------------------------
# W7: degraded states must surface as 503 with machine-readable reasons
# ---------------------------------------------------------------------------


def test_healthz_503_when_worker_stalled(client):
    """A worker that has not cycled beyond the margin reports degraded.

    The real worker keeps cycling (and refreshing its stamp), so the test
    swaps in a static stale stub - deterministic, no sleeps.
    """

    class _StalledWorker:
        last_cycle_ts = 0.0  # never cycled

    client.app.state.worker = _StalledWorker()
    # Push the startup stamp beyond the margin so the max() floor does not
    # mask the stalled worker.
    client.app.state.started_at = time.monotonic() - (WORKER_STALENESS_MARGIN_S + 60.0)

    response = client.get("/healthz")
    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "degraded"
    assert "worker_stalled" in data["degraded"]
    assert "counters" in data  # the DB check still ran


def test_healthz_503_when_worker_missing(client):
    client.app.state.worker = None

    response = client.get("/healthz")
    assert response.status_code == 503
    assert "worker_not_running" in response.json()["degraded"]


def test_healthz_503_when_database_unreachable(client):
    """A dead DB connection flips the probe to 503 (no counters)."""
    client.app.state.health_conn.close()

    response = client.get("/healthz")
    assert response.status_code == 503
    data = response.json()
    assert data["status"] == "degraded"
    assert "database_unreachable" in data["degraded"]
    assert data["counters"] == {}


def test_healthz_503_payload_has_no_degraded_key_when_healthy(client):
    response = client.get("/healthz")
    assert response.status_code == 200
    assert "degraded" not in response.json()
