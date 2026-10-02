"""Single asyncio worker: polls the FIFO queue and processes messages.

Design notes (see ARCHITECTURE.md sections 4 and 9):
- Exactly one worker instance may run at a time (GigaChat freemium allows a
  single concurrent request; a single consumer also avoids SQLite contention).
- Each cycle claims at most ``batch_size`` (default 10) pending rows,
  marks them ``processing``, processes them one by one, then removes the
  successfully processed rows from the queue. Failed rows are put back to
  ``pending`` for retry, with a per-row attempt counter (schema v5): once
  the counter reaches ``max_row_attempts`` the row is moved to the
  ``failed`` dead-letter status instead - a poisoned row must not block
  the FIFO queue forever. Rows whose reply send was deferred (W5,
  :class:`~app.pipeline.ReplySendDeferred`) return to ``pending`` via a
  dedicated path that does NOT consume the dead-letter budget: the
  pipeline stores the reply text in ``queue.deferred_reply`` (schema v6)
  and bumps ``attempts`` by exactly one for its single re-send; the next
  claim replays the stored text verbatim (no second LLM call).
- On startup, rows left in ``processing`` by a previous crash are recovered
  back to ``pending`` (see :meth:`Worker.recover`).
- Two maintenance cadences (W1 split): the heartbeat watchdog runs on the
  FAST tick so a dead phone is detected within ~one threshold, while the
  session-expiry sweep stays on the slow daily tick.
- Graceful shutdown: finish the current batch, then exit the loop.
- PRIVACY: never log payload text; metadata only (ids, lengths, counts).
"""

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any

from . import db
from .pipeline import ReplySendDeferred

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 10
DEFAULT_POLL_INTERVAL_S = 1.0
DEFAULT_SWEEP_INTERVAL_S = 24 * 60 * 60  # daily maintenance sweep
DEFAULT_WATCHDOG_INTERVAL_S = 5 * 60  # fast tick: dead-device detection
DEFAULT_MAX_ROW_ATTEMPTS = 5  # dead-letter after this many failed attempts

# Worker-liveness margin for the /healthz probe (W7): the loop cycles at
# least once per poll interval (1 s) when idle; under load the liveness
# stamp is refreshed after EVERY processed row (heartbeat-per-item), so
# the worst case between refreshes is ONE row's processing time: one
# worst-case LLM completion (3 x 90 s + backoff ~3 s = ~273 s at the
# defaults) PLUS one worst-case reply send (3 x 10 s + backoff ~1.5 s =
# ~32 s) = ~305 s. The margin is set to 330 s so a healthy worker that is
# merely stuck in a worst-case row never trips the probe (a 300 s margin
# would race that worst case). A genuinely dead worker task (crash) stops
# cycling immediately and is caught well within the margin.
WORKER_STALENESS_MARGIN_S = 330.0

ProcessFunc = Callable[[dict[str, Any]], Any]
SweepFunc = Callable[[], int]
WatchdogFunc = Callable[[], object]


def _default_process(row: dict[str, Any]) -> None:
    """Placeholder pipeline until steps 3-5 implement the real handling."""
    logger.info(
        "Processing message: queue_id=%d message_id=%s payload_len=%d",
        row["id"],
        row["message_id"],
        len(row["payload"]),
    )


class Worker:
    """FIFO queue consumer. Must be used as a single instance."""

    def __init__(
        self,
        conn: Any,
        process_func: ProcessFunc | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        sweep_func: SweepFunc | None = None,
        sweep_interval_s: float = DEFAULT_SWEEP_INTERVAL_S,
        watchdog_func: WatchdogFunc | None = None,
        watchdog_interval_s: float = DEFAULT_WATCHDOG_INTERVAL_S,
        max_row_attempts: int = DEFAULT_MAX_ROW_ATTEMPTS,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be positive")
        if sweep_interval_s <= 0:
            raise ValueError("sweep_interval_s must be positive")
        if watchdog_interval_s <= 0:
            raise ValueError("watchdog_interval_s must be positive")
        if max_row_attempts < 1:
            raise ValueError("max_row_attempts must be >= 1")
        self._conn = conn
        self._process_func = process_func or _default_process
        self._batch_size = batch_size
        self._poll_interval_s = poll_interval_s
        self._sweep_func = sweep_func
        self._sweep_interval_s = sweep_interval_s
        self._watchdog_func = watchdog_func
        self._watchdog_interval_s = watchdog_interval_s
        self._max_row_attempts = max_row_attempts
        self._stop_requested = False
        self._wake_event: asyncio.Event | None = None  # created inside run()
        self._last_sweep = 0.0  # monotonic stamp, reset in run()
        self._last_watchdog = 0.0  # monotonic stamp, reset in run()
        self._last_cycle_ts = 0.0  # monotonic stamp of the last loop cycle

    # ------------------------------------------------------------------
    # Synchronous primitives (also callable via asyncio.to_thread)
    # ------------------------------------------------------------------

    def recover(self) -> int:
        """Requeue rows stuck in ``processing`` after a crash. Returns count."""
        cursor = self._conn.execute(
            "UPDATE queue SET status = 'pending', updated_at = ? "
            "WHERE status = 'processing'",
            (db.utcnow_iso(),),
        )
        self._conn.commit()
        return int(cursor.rowcount)

    def _claim_batch(self) -> list[dict[str, Any]]:
        """Claim up to ``batch_size`` oldest pending rows (pending -> processing)."""
        now = db.utcnow_iso()
        with self._conn:
            # ``attempts`` (v5) travels for the budget logging; the W5
            # deferred-re-send marker (``deferred_reply``, v6) is what makes
            # the pipeline replay the stored reply instead of reprocessing.
            rows = self._conn.execute(
                "SELECT id, message_id, send_id, payload, attempts, deferred_reply "
                "FROM queue WHERE status = 'pending' ORDER BY id LIMIT ?",
                (self._batch_size,),
            ).fetchall()
            if not rows:
                return []
            ids = [row["id"] for row in rows]
            placeholders = ",".join("?" * len(ids))
            # The status guard prevents re-claiming rows that changed status
            # between the SELECT and this UPDATE (SELECT+UPDATE are not
            # atomic; the single worker makes it unlikely, but not impossible
            # if more consumers are ever added).
            self._conn.execute(
                "UPDATE queue SET status = 'processing', updated_at = ? "
                "WHERE status = 'pending' AND id IN (%s)" % placeholders,
                [now] + list(ids),
            )
        return [dict(row) for row in rows]

    def _complete(self, row_ids: list[int]) -> None:
        with self._conn:
            self._conn.executemany(
                "DELETE FROM queue WHERE id = ?", [(row_id,) for row_id in row_ids]
            )

    def _requeue(self, row_ids: list[int]) -> list[int]:
        """Requeue failed rows with the dead-letter policy (W2).

        Every failed row gets its ``attempts`` counter incremented (schema
        v5). Rows still inside the retry budget return to ``pending``; rows
        that reached ``max_row_attempts`` move to ``failed`` (dead-letter)
        with a WARN metadata log - the FIFO queue moves on instead of being
        blocked forever by a poisoned row. Returns the dead-lettered ids.
        """
        dead: list[int] = []
        for row_id in row_ids:
            attempts = db.increment_queue_attempts(self._conn, row_id)
            if attempts >= self._max_row_attempts:
                dead.append(row_id)
        if dead:
            db.mark_queue_failed(self._conn, dead)
        survivors = [row_id for row_id in row_ids if row_id not in dead]
        if survivors:
            with self._conn:
                self._conn.executemany(
                    "UPDATE queue SET status = 'pending', updated_at = ? WHERE id = ?",
                    [(db.utcnow_iso(), row_id) for row_id in survivors],
                )
        for row_id in dead:
            logger.warning(
                "Message dead-lettered: attempts=%d max=%d queue_id=%d "
                "(payload kept in queue for inspection)",
                self._max_row_attempts,
                self._max_row_attempts,
                row_id,
            )
        return dead

    def _requeue_deferred(self, row_ids: list[int]) -> None:
        """Return deferred rows (W5) to ``pending`` - no budget consumed.

        The pipeline already stored the reply marker (``deferred_reply``,
        v6) and incremented ``attempts`` by exactly one for its single
        re-send in one committed write (:func:`db.defer_queue_row`); here
        the row only changes status back. Deliberately NOT the dead-letter
        path: a deferral is a transient gateway outage, not a poisoned row.
        """
        with self._conn:
            self._conn.executemany(
                "UPDATE queue SET status = 'pending', updated_at = ? WHERE id = ?",
                [(db.utcnow_iso(), row_id) for row_id in row_ids],
            )

    def _process_one(self, row: dict[str, Any]) -> None:
        self._process_func(row)

    # ------------------------------------------------------------------
    # Async loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Main polling loop; exits after ``request_stop`` and batch completion."""
        self._stop_requested = False
        self._wake_event = asyncio.Event()
        self._last_sweep = time.monotonic()
        self._last_watchdog = time.monotonic()
        logger.info(
            "Worker started (batch_size=%d, poll_interval_s=%.3f, "
            "watchdog_interval_s=%.0f, sweep_interval_s=%.0f, max_row_attempts=%d)",
            self._batch_size,
            self._poll_interval_s,
            self._watchdog_interval_s,
            self._sweep_interval_s,
            self._max_row_attempts,
        )
        try:
            while not self._stop_requested:
                self._last_cycle_ts = time.monotonic()
                await self._maybe_watchdog()
                await self._maybe_sweep()
                rows = await asyncio.to_thread(self._claim_batch)
                if not rows:
                    await self._wait_for_wake_or_timeout()
                    continue
                completed: list[int] = []
                failed: list[int] = []
                deferred: list[int] = []
                for row in rows:
                    try:
                        await asyncio.to_thread(self._process_one, row)
                        completed.append(row["id"])
                    except ReplySendDeferred:
                        # W5: controlled single re-send, already counted by
                        # the pipeline (sms_send_requeued, attempts bump).
                        logger.info(
                            "Reply deferred, row requeued once: "
                            "queue_id=%d message_id=%s attempts=%d",
                            row["id"],
                            row["message_id"],
                            row.get("attempts", 0),
                        )
                        deferred.append(row["id"])
                    except Exception:  # noqa: BLE001 - keep the batch going
                        # Metadata only - never log the payload text (152-FZ).
                        logger.exception(
                            "Message processing failed, requeued: "
                            "queue_id=%d message_id=%s payload_len=%d",
                            row["id"],
                            row["message_id"],
                            len(row["payload"]),
                        )
                        failed.append(row["id"])
                    # Heartbeat-per-item (W7 margin fix): refresh the
                    # liveness stamp after EVERY row so a long batch cannot
                    # stall the /healthz probe (the margin covers one row's
                    # worst-case processing time, not the whole batch).
                    self._last_cycle_ts = time.monotonic()
                if completed:
                    await asyncio.to_thread(self._complete, completed)
                if deferred:
                    await asyncio.to_thread(self._requeue_deferred, deferred)
                if failed:
                    await asyncio.to_thread(self._requeue, failed)
                logger.debug(
                    "Batch finished: processed=%d deferred=%d failed=%d",
                    len(completed),
                    len(deferred),
                    len(failed),
                )
        finally:
            logger.info("Worker stopped")

    async def _maybe_watchdog(self) -> None:
        """Run the heartbeat watchdog on the FAST tick (W1 fix).

        The watchdog is a coarse "device dead" check whose alert threshold is
        ``3 x heartbeat_interval_min`` (45 min by default); running it once a
        day (the old sweep cadence) made detection a day late. It now ticks
        every ``watchdog_interval_s`` (default 5 min), so an alert fires
        within ~one threshold of the actual silence. A watchdog failure is
        logged (metadata only) and never kills the loop.
        """
        if self._watchdog_func is None:
            return
        now = time.monotonic()
        if now - self._last_watchdog < self._watchdog_interval_s:
            return
        self._last_watchdog = now
        try:
            await asyncio.to_thread(self._watchdog_func)
        except Exception:  # noqa: BLE001 - monitoring must not stop the worker
            logger.exception("Watchdog tick failed")

    async def _maybe_sweep(self) -> None:
        """Run the daily maintenance sweep when the interval has elapsed.

        The sweep covers session expiry only; the heartbeat watchdog has its
        own fast tick (see :meth:`_maybe_watchdog`). A sweep failure is
        logged (metadata only) and never kills the loop.
        """
        if self._sweep_func is None:
            return
        now = time.monotonic()
        if now - self._last_sweep < self._sweep_interval_s:
            return
        self._last_sweep = now
        try:
            removed = await asyncio.to_thread(self._sweep_func)
            if removed:
                logger.info("Periodic sweep: removed=%d", removed)
        except Exception:  # noqa: BLE001 - maintenance must not stop the worker
            logger.exception("Periodic sweep failed")

    async def _wait_for_wake_or_timeout(self) -> None:
        event = self._wake_event
        assert event is not None
        event.clear()
        try:
            await asyncio.wait_for(event.wait(), timeout=self._poll_interval_s)
        except asyncio.TimeoutError:
            pass

    def request_stop(self) -> None:
        """Request graceful shutdown: finish the current batch, then stop."""
        self._stop_requested = True
        if self._wake_event is not None:
            self._wake_event.set()

    @property
    def last_cycle_ts(self) -> float:
        """Monotonic liveness stamp (health probe input).

        Refreshed at every loop cycle AND after every processed row
        (heartbeat-per-item), so ``/healthz`` liveness stays honest during
        long batches: the gap between refreshes is bounded by one row's
        worst-case processing time, not the batch duration.
        """
        return self._last_cycle_ts
