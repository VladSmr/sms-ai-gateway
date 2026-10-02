"""FastAPI application factory: wiring for config, DB, worker and routes."""

import asyncio
import logging
import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from . import __version__, db, logging_setup
from .config import ConfigManager
from .gateway.client import env_mock_enabled as gateway_mock_enabled
from .llm.gigachat import MAX_ATTEMPTS as MAX_LLM_ATTEMPTS, env_mock_enabled
from .pipeline import MessagePipeline
from .rate_limit import RateLimiter
from .routes import health, webhook
from .worker import DEFAULT_WATCHDOG_INTERVAL_S, Worker

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "config.yaml"

# Graceful-shutdown budget for the worker, derived from the LLM settings
# (see worker_shutdown_timeout_s): the client's worst case per completion is
# MAX_ATTEMPTS (3) x timeout_s plus backoff sleeps (1 s + 2 s), so a fixed
# 120 s would NOT cover an in-flight completion and a graceful shutdown
# would cancel it mid-request. Cancel-safety requirement from PLAN.md step 4.
SHUTDOWN_EXTRA_MARGIN_S = 45.0


def worker_shutdown_timeout_s(llm_timeout_s: float, max_attempts: int) -> float:
    """Shutdown budget covering one worst-case completion (with retries)."""
    # Backoff between the attempts: 1 s + 2 s (exponential, attempt-indexed).
    backoff_total = sum(min(2.0**i, 8.0) for i in range(max_attempts - 1))
    return max_attempts * llm_timeout_s + backoff_total + SHUTDOWN_EXTRA_MARGIN_S


def create_app(
    config_path: str | None = None,
    log_file: str | None = None,
) -> FastAPI:
    """Build the FastAPI application with lifespan-managed resources.

    :param config_path: explicit path to the YAML config; falls back to the
        ``GATEWAY_CONFIG`` env var, then to ``config.yaml`` in the CWD.
    :param log_file: optional override for the log file location (tests).
    """
    resolved_config_path = Path(
        config_path or os.environ.get("GATEWAY_CONFIG", DEFAULT_CONFIG_PATH)
    )
    config_manager = ConfigManager(resolved_config_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # ----- startup -----------------------------------------------------
        config = config_manager.load_initial()
        if not config.webhook.secret:
            # Fail fast at startup, not with 401s at request time: the
            # receiver cannot verify anything without the signing key.
            raise RuntimeError(
                "Webhook signing secret is not configured: set webhook.secret "
                "in the config file (e.g. webhook.secret: ${GATEWAY_HMAC_SECRET})"
            )
        llm_mock = env_mock_enabled(os.environ.get) or config.llm.mock
        if not config.llm.auth_key and not llm_mock:
            # Fail fast: without the key every completion would fail at the
            # OAuth step. Mock mode (MOCK_LLM=true / llm.mock) needs no key.
            raise RuntimeError(
                "GigaChat auth key is not configured: set llm.auth_key in the "
                "config file (e.g. llm.auth_key: ${GIGACHAT_AUTH_KEY}), or "
                "enable mock mode via MOCK_LLM=true"
            )
        if (
            not config.gateway.username or not config.gateway.password
        ) and not gateway_mock_enabled(os.environ.get) and not config.gateway.mock:
            # Fail fast: without the Basic credentials every reply send would
            # 401. Mock mode (MOCK_GATEWAY=true / gateway.mock) needs none.
            raise RuntimeError(
                "Gateway credentials are not configured: set gateway.username/"
                "gateway.password in the config file (e.g. via "
                "${GATEWAY_USERNAME}/${GATEWAY_PASSWORD}), or enable mock mode "
                "via MOCK_GATEWAY=true"
            )
        logging_setup.setup_logging(
            level=os.environ.get("LOG_LEVEL") or config.log_level,
            log_file=log_file,
        )
        logger.info(
            "Gateway starting: version=%s config=%s reload=%s",
            __version__,
            config_manager.path,
            config.config.reload,
        )
        # M1: loud startup banner for mock modes (dev/test convenience that
        # must never silently reach production). Also recorded as /healthz
        # counters so external monitoring sees it without log access.
        llm_mock = env_mock_enabled(os.environ.get) or config.llm.mock
        gw_mock = gateway_mock_enabled(os.environ.get) or config.gateway.mock
        if llm_mock:
            logger.warning(
                "MOCK MODE: LLM is an echo stub (MOCK_LLM env or llm.mock) - "
                "mock mode, do not use in production"
            )
        if gw_mock:
            logger.warning(
                "MOCK MODE: gateway sends are logged only (MOCK_GATEWAY env "
                "or gateway.mock) - mock mode, do not use in production"
            )
        if not config.whitelist:
            # V3: an empty whitelist is schema-valid, but the pipeline drops
            # EVERY incoming SMS silently (pipeline.py drops non-whitelisted
            # senders by design). Fail-fast is intentionally NOT used: the
            # operator may legitimately want the app up while filling the
            # whitelist via hot reload; the warning must be impossible to
            # miss (mirrors the M1 mock-mode banner below) and is also
            # visible externally via the "empty_whitelist" health counter
            # (incremented right after the mock counters).
            empty_whitelist = True
        else:
            empty_whitelist = False
        app.state.config_manager = config_manager
        app.state.started_at = time.monotonic()
        app.state.rate_limiter = RateLimiter(config.webhook.rate_limit_per_min)
        # M3: serializes the webhook dedup+enqueue section across concurrent
        # requests (the receiver connection runs explicit BEGIN/COMMIT in
        # db.accept_message; BEGIN/COMMIT are connection-global, so two
        # interleaved transactions could fail with OperationalError). Bound
        # to this app (hence this event loop) at startup.
        app.state.accept_lock = asyncio.Lock()

        # W3: separate SQLite connections per component. WAL allows one
        # writer + concurrent readers; sharing a single connection between
        # the request threads (webhook/health) and the worker thread risks
        # interleaved explicit BEGIN/COMMIT transactions. Migrations run
        # once on the receiver connection before anything else touches the
        # database.
        db_path = Path(config.db_path)
        if not db_path.is_absolute():
            # Relative db_path is resolved against the config file directory.
            db_path = config_manager.path.resolve().parent / db_path
        receiver_conn = db.open_db(str(db_path))
        schema_version = await asyncio.to_thread(db.run_migrations, receiver_conn)
        logger.info("Database ready: path=%s schema_version=%d", db_path, schema_version)
        worker_conn = db.open_db(str(db_path))
        health_conn = db.open_db(str(db_path))
        app.state.db_conn = receiver_conn
        app.state.health_conn = health_conn

        # M1: mock-mode counters go through the receiver connection (after
        # migrations, before any request can hit /healthz).
        if llm_mock:
            db.increment_counter(receiver_conn, "mock_llm")
        if gw_mock:
            db.increment_counter(receiver_conn, "mock_gateway")
        # V3: the empty-whitelist banner is logged here (logger lines above
        # are kept together) and exposed via a counter, same as mock modes.
        if empty_whitelist:
            logger.warning(
                "Empty whitelist - ALL incoming SMS will be dropped "
                "(silently, counted as dropped_whitelist). Add your number "
                "to the whitelist in the config file (E.164, e.g. "
                'whitelist: ["+79001234567"])'
            )
            db.increment_counter(receiver_conn, "empty_whitelist")

        pipeline = MessagePipeline(worker_conn, config_manager)
        worker = Worker(
            worker_conn,
            process_func=pipeline.process,
            sweep_func=pipeline.sweep,
            watchdog_func=lambda: pipeline.watchdog_tick(),
            watchdog_interval_s=DEFAULT_WATCHDOG_INTERVAL_S,
        )
        recovered = await asyncio.to_thread(worker.recover)
        if recovered:
            logger.info(
                "Recovered %d stuck 'processing' message(s) back to 'pending'", recovered
            )
        worker_task = asyncio.get_running_loop().create_task(worker.run())
        app.state.worker = worker
        app.state.worker_task = worker_task

        await config_manager.start()

        try:
            yield
        finally:
            # ----- shutdown ------------------------------------------------
            await config_manager.stop()
            worker.request_stop()  # graceful: finish the current batch
            # Budget must cover one worst-case LLM completion (retries
            # included) plus one worst-case reply send (3 x gateway.timeout_s
            # + backoff ~1.5 s = ~31.5 s) - hence the 45 s margin above.
            shutdown_timeout = worker_shutdown_timeout_s(
                config.llm.timeout_s, MAX_LLM_ATTEMPTS
            )
            logger.debug("Worker shutdown budget: %.0fs", shutdown_timeout)
            try:
                await asyncio.wait_for(worker_task, timeout=shutdown_timeout)
            except asyncio.TimeoutError:
                logger.warning("Worker did not stop in time, cancelling")
                worker_task.cancel()
                try:
                    # Await the cancelled task before closing the shared
                    # SQLite connections, otherwise the worker may still
                    # touch a closed connection.
                    await worker_task
                except asyncio.CancelledError:
                    pass
                except Exception:  # noqa: BLE001 - do not mask the shutdown
                    logger.exception("Worker task failed during shutdown")
            receiver_conn.close()
            worker_conn.close()
            health_conn.close()
            logger.info("Gateway stopped")

    app = FastAPI(
        title="SMS-to-GigaChat Gateway",
        version=__version__,
        lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(webhook.router)
    return app
