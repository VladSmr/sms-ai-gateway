"""Health endpoint for external monitoring (e.g. UptimeRobot).

W7: the endpoint performs REAL checks and reports ``503`` on degradation
(the README promise is now implemented):

- **DB reachable** - a cheap ``SELECT`` on the dedicated health connection
  (W3: separate connection, so a busy writer never blocks the probe);
- **Worker alive** - the worker loop's ``last_cycle_ts`` must be recent.
  The margin (``WORKER_STALENESS_MARGIN_S``, see worker.py for the exact
  arithmetic) covers the worst case between liveness refreshes - one row's
  processing time: a worst-case LLM completion plus a worst-case reply
  send (~305 s at the default timeouts; the 330 s margin clears it). The
  Docker healthcheck must NOT flip during such processing. A genuinely
  dead worker task (crash) stops cycling immediately and is caught well
  within the margin.

The response also carries a ``degraded`` reason list (machine-readable)
and the mock-mode counters (M1) alongside the business counters.
"""

import asyncio
import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from .. import __version__, db
from ..worker import WORKER_STALENESS_MARGIN_S

logger = logging.getLogger(__name__)

router = APIRouter()


def _collect_health(request: Request) -> tuple[dict[str, int], list[str]]:
    """Run the checks in a thread; return (counters, degraded reasons)."""
    reasons: list[str] = []

    # -- DB probe (cheap SELECT via the dedicated health connection) ------
    try:
        counters = db.get_counters(request.app.state.health_conn)
    except Exception:  # noqa: BLE001 - any DB failure is a degraded state
        logger.warning("Health check: database probe failed")
        return {}, ["database_unreachable"]

    # -- Worker liveness (last loop cycle freshness) -----------------------
    worker = getattr(request.app.state, "worker", None)
    if worker is None:
        reasons.append("worker_not_running")
    else:
        # Reference floor = app startup: a worker task that has not made its
        # first loop iteration yet must not immediately flag the app as
        # degraded (the margin is measured from the older of the two stamps,
        # so a worker that NEVER cycles trips the check once the margin
        # passes since startup).
        reference = max(worker.last_cycle_ts, request.app.state.started_at)
        stale_s = time.monotonic() - reference
        if stale_s > WORKER_STALENESS_MARGIN_S:
            reasons.append("worker_stalled")

    return counters, reasons


@router.get("/healthz")
async def healthz(request: Request) -> JSONResponse:
    """Report service status, uptime, version, business counters, mock flags.

    Returns ``200`` while the DB answers and the worker loop is cycling;
    ``503`` with a ``degraded`` reason list otherwise. Note: the Docker
    container healthcheck (docker-compose) only inspects the HTTP status.
    """
    started_at = request.app.state.started_at
    try:
        counters, degraded = await asyncio.to_thread(_collect_health, request)
    except Exception:  # noqa: BLE001 - never 500 on the probe itself
        counters, degraded = {}, ["health_probe_failed"]
    payload = {
        "status": "ok" if not degraded else "degraded",
        "uptime_s": round(time.monotonic() - started_at, 3),
        "version": __version__,
        "counters": counters,
    }
    if degraded:
        payload["degraded"] = degraded
        return JSONResponse(status_code=503, content=payload)
    return JSONResponse(status_code=200, content=payload)
