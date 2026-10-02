"""Tests for the worker: FIFO processing, recovery, graceful shutdown."""

import asyncio
import threading

from app import db
from app.worker import Worker


def test_worker_processes_pending_fifo(tmp_path):
    async def scenario():
        conn = db.open_db(tmp_path / "worker.db")
        try:
            db.run_migrations(conn)
            for message_id in ("m1", "m2", "m3"):
                db.enqueue_message(conn, message_id, '{"sender": "+79001234567"}')

            processed = []
            worker = Worker(
                conn,
                process_func=lambda row: processed.append(row["message_id"]),
                poll_interval_s=0.05,
            )
            task = asyncio.ensure_future(worker.run())
            try:
                for _ in range(250):
                    if len(processed) >= 3:
                        break
                    await asyncio.sleep(0.02)
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, timeout=5)

            assert sorted(processed) == ["m1", "m2", "m3"]
            # FIFO order preserved
            assert processed == ["m1", "m2", "m3"]
            assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0
        finally:
            conn.close()

    asyncio.run(scenario())


def test_worker_requeues_failed_message(tmp_path):
    async def scenario():
        conn = db.open_db(tmp_path / "worker_fail.db")
        try:
            db.run_migrations(conn)
            db.enqueue_message(conn, "boom", "{}")

            attempts = []

            def failing_process(row):
                attempts.append(row["message_id"])
                raise RuntimeError("simulated failure")

            worker = Worker(conn, process_func=failing_process, poll_interval_s=0.05)
            task = asyncio.ensure_future(worker.run())
            try:
                for _ in range(250):
                    if len(attempts) >= 2:
                        break
                    await asyncio.sleep(0.02)
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, timeout=5)

            # The failed message was retried (requeued), never lost.
            assert len(attempts) >= 2
            row = conn.execute(
                "SELECT status FROM queue WHERE message_id = 'boom'"
            ).fetchone()
            assert row["status"] == "pending"
        finally:
            conn.close()

    asyncio.run(scenario())


class _RacingConn:
    """Connection proxy that emulates a concurrent consumer.

    Right before the claim UPDATE runs, it marks the given message as
    ``processing`` (as a competing consumer would between the SELECT and the
    UPDATE of ``_claim_batch``). This makes the test sensitive to the
    ``AND status = 'pending'`` guard: without the guard the batch UPDATE
    re-claims the row and overwrites the other consumer's ``updated_at``.
    """

    def __init__(self, conn, message_id):
        self._conn = conn
        self._message_id = message_id

    def _simulate_other_consumer(self):
        self._conn.execute(
            "UPDATE queue SET status = 'processing', updated_at = ? "
            "WHERE message_id = ?",
            ("2000-01-01T00:00:00.000+00:00", self._message_id),
        )
        self._conn.commit()

    def execute(self, sql, *args, **kwargs):
        if sql.lstrip().upper().startswith("UPDATE QUEUE SET STATUS = 'PROCESSING'"):
            # Another consumer wins the race for the row exactly between the
            # SELECT and the claim UPDATE.
            self._simulate_other_consumer()
        return self._conn.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self._conn.__exit__(exc_type, exc_value, traceback)


def test_claim_batch_guard_does_not_steal_claimed_rows(tmp_path):
    """The claim UPDATE must guard on status.

    Without the ``AND status = 'pending'`` guard the batch UPDATE would
    re-claim a row that another consumer grabbed between the SELECT and the
    UPDATE, overwriting its ``updated_at`` — the assertion below catches
    exactly that.
    """
    conn = db.open_db(tmp_path / "claim.db")
    try:
        db.run_migrations(conn)
        db.enqueue_message(conn, "m1", "{}")
        db.enqueue_message(conn, "m2", "{}")

        # m2 is still pending at SELECT time, but another consumer claims it
        # right before the worker's UPDATE runs (see _RacingConn).
        racing = _RacingConn(conn, "m2")
        worker = Worker(racing)
        claimed = worker._claim_batch()

        # The SELECT legitimately returned both rows (m2 was pending then).
        assert sorted(row["message_id"] for row in claimed) == ["m1", "m2"]

        # m2's claim metadata belongs to the "other consumer": neither its
        # updated_at (frozen marker) nor its ownership was overwritten.
        row = conn.execute(
            "SELECT status, updated_at FROM queue WHERE message_id = 'm2'"
        ).fetchone()
        assert row["status"] == "processing"
        assert row["updated_at"] == "2000-01-01T00:00:00.000+00:00", (
            "the guardless UPDATE overwrote another consumer's claim metadata"
        )
        # m1 is claimed by this worker as usual.
        row_m1 = conn.execute(
            "SELECT status FROM queue WHERE message_id = 'm1'"
        ).fetchone()
        assert row_m1["status"] == "processing"
    finally:
        conn.close()


def test_worker_recovery_processing_to_pending(tmp_path):
    conn = db.open_db(tmp_path / "recovery.db")
    try:
        db.run_migrations(conn)
        db.enqueue_message(conn, "stuck", "{}")
        conn.execute(
            "UPDATE queue SET status = 'processing', updated_at = ? "
            "WHERE message_id = 'stuck'",
            (db.utcnow_iso(),),
        )
        conn.commit()

        worker = Worker(conn)
        recovered = worker.recover()
        assert recovered == 1
        row = conn.execute(
            "SELECT status FROM queue WHERE message_id = 'stuck'"
        ).fetchone()
        assert row["status"] == "pending"
        # Second recovery is a no-op (idempotent).
        assert worker.recover() == 0
    finally:
        conn.close()


def test_worker_graceful_shutdown_finishes_batch(tmp_path):
    """A stop requested mid-batch must not drop already claimed rows."""

    async def scenario():
        conn = db.open_db(tmp_path / "graceful.db")
        try:
            db.run_migrations(conn)
            db.enqueue_message(conn, "a1", "{}")
            db.enqueue_message(conn, "a2", "{}")

            release_first = threading.Event()
            processed = []

            def blocking_process(row):
                processed.append(row["message_id"])
                if row["message_id"] == "a1":
                    # Block the first message until the test asks to continue.
                    assert release_first.wait(timeout=5)
                    # Simulate stop arriving while the batch is in flight.
                    worker.request_stop()

            worker = Worker(
                conn, process_func=blocking_process, poll_interval_s=0.05
            )
            task = asyncio.ensure_future(worker.run())

            # Wait until a1 is claimed (status = processing).
            claimed = False
            for _ in range(250):
                status = conn.execute(
                    "SELECT status FROM queue WHERE message_id = 'a1'"
                ).fetchone()["status"]
                if status == "processing":
                    claimed = True
                    break
                await asyncio.sleep(0.02)
            assert claimed, "worker did not claim the message in time"

            release_first.set()  # let the batch finish
            await asyncio.wait_for(task, timeout=5)

            # The whole claimed batch (a1, a2) was completed despite the stop.
            assert sorted(processed) == ["a1", "a2"]
            assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0
        finally:
            conn.close()

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Shutdown budget: must cover the LLM client's worst case (review fix)
# ---------------------------------------------------------------------------


def test_worker_shutdown_timeout_covers_llm_worst_case():
    from app.llm.gigachat import MAX_ATTEMPTS
    from app.main import SHUTDOWN_EXTRA_MARGIN_S, worker_shutdown_timeout_s

    budget = worker_shutdown_timeout_s(llm_timeout_s=90.0, max_attempts=MAX_ATTEMPTS)
    # Worst case: 3 attempts x 90 s + backoff (1 s + 2 s) + margin.
    assert budget == 3 * 90.0 + 1.0 + 2.0 + SHUTDOWN_EXTRA_MARGIN_S
    # And it must strictly exceed the raw worst case without the margin.
    assert budget > MAX_ATTEMPTS * 90.0


def test_worker_shutdown_timeout_scales_with_config():
    from app.main import worker_shutdown_timeout_s

    small = worker_shutdown_timeout_s(llm_timeout_s=5.0, max_attempts=3)
    big = worker_shutdown_timeout_s(llm_timeout_s=120.0, max_attempts=3)
    assert small < big
    assert big > 3 * 120.0  # retries included
