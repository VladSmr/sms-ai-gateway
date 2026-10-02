"""Device heartbeat watchdog (step 5).

The phone posts ``system:ping`` events (pull cycles) - each one refreshes
``device_state['last_ping']``. The worker runs this watchdog on its
periodic sweep: when the last ping is older than ``3 x heartbeat_interval``
(a single missed interval is normal GSM jitter; three in a row means the
device/server link is dead by design), a Telegram alert fires EXACTLY ONCE
- the ``watchdog_alerted`` flag in device_state prevents re-alerting on
every sweep. The flag is cleared (and a one-line "recovered" alert sent)
on the next ping.

No heartbeat ever recorded -> no alert (fresh-install baseline; the first
ping establishes it).

Privacy: alerts and logs carry statuses/timestamps only - never health
values, message text or phone numbers (152-FZ).
"""

import logging
from datetime import datetime, timezone

from .. import db

logger = logging.getLogger(__name__)

HEARTBEAT_KEY = "last_ping"
ALERTED_KEY = "watchdog_alerted"


def record_heartbeat(conn) -> str | None:
    """Store a fresh heartbeat (``system:ping``) and clear the alert flag.

    Returns ``"recovered"`` when this ping ends a watchdog alert episode
    (the caller may emit a one-line recovery notification), else ``None``.
    PRIVACY: only the timestamp is persisted - never the health values.
    """
    db.set_device_state(conn, HEARTBEAT_KEY, db.utcnow_iso())
    if db.get_device_state(conn, ALERTED_KEY) is not None:
        db.delete_device_state(conn, ALERTED_KEY)
        return "recovered"
    return None


def check_heartbeat(
    conn,
    config,
    notifier,
    *,
    now: datetime | None = None,
) -> bool:
    """Watchdog tick (worker sweep): alert once when the phone went silent
    past 3x the expected heartbeat interval.

    Returns ``True`` when an alert episode is active (fresh alert or the
    already-flagged silence), ``False`` when the device looks healthy.
    """
    threshold_s = 3 * config.notify.heartbeat_interval_min * 60
    current = now or datetime.now(timezone.utc)
    last_ping = db.get_device_state(conn, HEARTBEAT_KEY)
    if last_ping is None:
        # Fresh install / no ping yet: do not alert without a baseline.
        return False
    try:
        last_dt = datetime.fromisoformat(last_ping)
    except ValueError:
        logger.warning("Watchdog: unparsable last_ping timestamp, skipping tick")
        return False
    if last_dt.tzinfo is None:
        # Defensive: treat a naive stamp as UTC.
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    silent_s = (current - last_dt).total_seconds()
    if silent_s < threshold_s:
        # Healthy: clear a stale alerted flag.
        if db.get_device_state(conn, ALERTED_KEY) is not None:
            db.delete_device_state(conn, ALERTED_KEY)
        return False

    if db.get_device_state(conn, ALERTED_KEY) is not None:
        return True  # already alerted; do not spam on every sweep
    db.set_device_state(conn, ALERTED_KEY, db.utcnow_iso())
    db.increment_counter(conn, "watchdog_alerts")
    notifier.send_alert(
        "SMS gateway: device silent for >%d min (heartbeat watchdog)"
        % (threshold_s // 60)
    )
    logger.warning(
        "Watchdog: heartbeat silent, alert issued: silent_s=%d threshold_s=%d",
        int(silent_s),
        int(threshold_s),
    )
    return True
