"""Configuration: YAML file + ${ENV} substitution + Pydantic v2 validation.

Features:
- Any string value in the YAML file may reference an environment variable via
  ``${VAR_NAME}`` (also inside larger strings, e.g. ``data/${DB_NAME}``).
  A reference to an unset variable is a configuration error.
- The resulting mapping is validated with Pydantic v2 models; unknown keys are
  rejected (``extra="forbid"``).
- Hot reload: when the ``config.reload`` flag is true the file is re-read
  periodically by :class:`ConfigManager` (and optionally on POSIX SIGHUP;
  signal-based reload is optional and simply skipped on Windows). If the new
  file is invalid, the previous last-good configuration is kept and the error
  is logged.
"""

import asyncio
import logging
import os
import re
import signal
import threading
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

logger = logging.getLogger(__name__)

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_E164_PATTERN = re.compile(r"^\+[1-9]\d{1,14}$")


class ConfigError(ValueError):
    """Raised when the configuration cannot be loaded, substituted or validated."""


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class LLMSettings(BaseModel):
    """GigaChat client settings (step 4).

    ``auth_key`` is the Basic auth key for the OAuth endpoint (Settings ->
    API keys -> "Authorization data" on developers.sber.ru), typically via
    an env reference: ``auth_key: ${GIGACHAT_AUTH_KEY}``. An empty key keeps
    the config schema-valid, but :func:`app.main.create_app` refuses to
    start the application unless mock mode is on (mirrors ``webhook.secret``).

    Endpoints default to the official production values: the chat API moved
    to ``https://api.giga.chat/v1`` (from 17.07.2026; the previous domain
    still works but is being decommissioned) and OAuth lives at
    ``https://ngw.devices.sberbank.ru:9443/api/v2/oauth``.

    Both endpoints present TLS certificates signed by the Russian trusted
    root CA (NUTS Mintsifry) - the deployment must have that root in the CA
    bundle (see README "TLS setup" / the Dockerfile); TLS verification is
    never disabled.

    ``mock`` (or the ``MOCK_LLM=true`` env override) turns the LLM call into
    an echo stub ``(mock) <text>`` for pipeline testing without any keys or
    network access.
    """

    model_config = ConfigDict(extra="forbid")

    model: str
    temperature: float
    max_tokens: int
    timeout_s: float
    base_url: str = "https://api.giga.chat/v1"
    oauth_url: str = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
    scope: str = "GIGACHAT_API_PERS"
    auth_key: str = ""
    mock: bool = False

    @field_validator("temperature")
    @classmethod
    def _check_temperature(cls, value: float) -> float:
        if not 0.0 <= value <= 2.0:
            raise ValueError("temperature must be within [0.0, 2.0]")
        return value

    @field_validator("max_tokens")
    @classmethod
    def _check_max_tokens(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("max_tokens must be positive")
        return value

    @field_validator("timeout_s")
    @classmethod
    def _check_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("timeout_s must be positive")
        return value

    @field_validator("base_url", "oauth_url")
    @classmethod
    def _check_urls(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("llm endpoint URLs must start with http:// or https://")
        return value.rstrip("/")


class LimitsSettings(BaseModel):
    """Operational limits: sessions, history size and reply budget."""

    model_config = ConfigDict(extra="forbid")

    max_sessions: int
    history_messages: int
    session_ttl_h: int
    max_reply_units: int

    @field_validator("max_sessions", "history_messages", "session_ttl_h", "max_reply_units")
    @classmethod
    def _check_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("limit values must be positive")
        return value


class GatewaySettings(BaseModel):
    """SMS gateway (Private Server) REST API settings (step 5).

    ``username``/``password`` are the phone app's web-server credentials
    (Settings -> Connectivity -> Web server credentials), sent as HTTP
    Basic auth to the Private Server on our VPS, typically via env
    references: ``username: ${GATEWAY_USERNAME}``. Empty credentials keep
    the config schema-valid, but :func:`app.main.create_app` refuses to
    start the application unless mock mode is on (mirrors ``webhook.secret``
    and ``llm.auth_key``).

    ``messages_path`` is configurable because the docs are inconsistent
    about the ``/api`` prefix on the messages endpoint (retrieved
    2026-09-17 the docs use ``/api/3rdparty/v1/messages``); the real path
    is verified against the live Private Server in step 6.

    ``mock`` (or the ``MOCK_GATEWAY=true`` env override) replaces the HTTP
    client with a log-only stub - no network, no credentials needed.
    """

    model_config = ConfigDict(extra="forbid")

    base_url: str
    username: str = ""
    password: str = ""
    messages_path: str = "/api/3rdparty/v1/messages"
    timeout_s: float = 10.0
    mock: bool = False

    @field_validator("base_url")
    @classmethod
    def _check_scheme(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("gateway.base_url must start with http:// or https://")
        return value.rstrip("/")

    @field_validator("messages_path")
    @classmethod
    def _check_messages_path(cls, value: str) -> str:
        if not value.startswith("/") or not value.strip():
            raise ValueError("gateway.messages_path must be an absolute path starting with '/'")
        return value

    @field_validator("timeout_s")
    @classmethod
    def _check_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError("gateway.timeout_s must be positive")
        return value


class NotifySettings(BaseModel):
    """Telegram alerting settings (used starting from step 5).

    Empty ``telegram_token``/``telegram_chat_id`` mean log-only mode: alerts
    are written to the log instead of being sent (dev/test fallback; the
    pipeline must keep working without Telegram).

    ``heartbeat_interval_min`` is the expected ``system:ping`` cadence of
    the phone app; the watchdog alerts after 3x this interval of silence
    (see :mod:`app.notify.watchdog`).
    """

    model_config = ConfigDict(extra="forbid")

    telegram_token: str = ""
    telegram_chat_id: str = ""
    heartbeat_interval_min: int = 15

    @field_validator("heartbeat_interval_min")
    @classmethod
    def _check_interval(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("notify.heartbeat_interval_min must be positive")
        return value


class RagSettings(BaseModel):
    """RAG subsystem switch (Phase 2)."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False


class WebhookSettings(BaseModel):
    """Inbound webhook receiver settings (step 2).

    ``secret`` must reference the gateway "Signing Key" (Settings -> Webhooks
    -> Signing Key), typically via an environment variable:
    ``webhook.secret: ${GATEWAY_HMAC_SECRET}``. An empty secret keeps the
    config schema-valid for tests, but :func:`app.main.create_app` refuses to
    start the application until a real secret is configured.

    ``dedup_ttl_hours`` (W8) is the dedup tombstone TTL; it must cover the
    GATEWAY'S retry horizon (the phone app retries failed deliveries with
    exponential backoff for ~45.5 h by default - README "First live run
    checklist"). 48 h is the safe default; a shorter value risks accepting
    a retried delivery as new (double processing).
    """

    model_config = ConfigDict(extra="forbid")

    secret: str = ""
    rate_limit_per_min: int = 120
    max_body_bytes: int = 65536
    timestamp_window_s: int = 300
    dedup_ttl_hours: int = 48

    @field_validator("rate_limit_per_min", "max_body_bytes", "timestamp_window_s", "dedup_ttl_hours")
    @classmethod
    def _check_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("webhook limits must be positive")
        return value


class ConfigSectionSettings(BaseModel):
    """The ``config`` section of the configuration file itself."""

    model_config = ConfigDict(extra="forbid")

    reload: bool = False


class AppConfig(BaseModel):
    """Root configuration model."""

    model_config = ConfigDict(extra="forbid")

    whitelist: list[str]
    system_prompt: str
    llm: LLMSettings
    limits: LimitsSettings
    sessions_named: dict[int, str]
    gateway: GatewaySettings
    webhook: WebhookSettings = WebhookSettings()
    notify: NotifySettings = NotifySettings()
    rag: RagSettings = RagSettings()
    config: ConfigSectionSettings = ConfigSectionSettings()
    db_path: str = "data/gateway.db"
    log_level: str = "INFO"

    @field_validator("whitelist")
    @classmethod
    def _check_e164(cls, value: list[str]) -> list[str]:
        for item in value:
            if not _E164_PATTERN.match(item):
                raise ValueError(
                    "whitelist entry '%s' is not a valid E.164 number "
                    "(must start with '+' followed by country code)" % item
                )
        return value

    @field_validator("sessions_named")
    @classmethod
    def _check_sessions_named(cls, value: dict[int, str]) -> dict[int, str]:
        for key, name in value.items():
            if not name:
                raise ValueError("sessions_named entry %d has an empty name" % key)
        return value


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def substitute_env(value: Any) -> Any:
    """Recursively substitute ``${VAR}`` references with environment values.

    Raises :class:`ConfigError` if a referenced variable is not set.
    """
    if isinstance(value, dict):
        return {key: substitute_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [substitute_env(item) for item in value]
    if isinstance(value, str):

        def _replace(match: re.Match[str]) -> str:
            var_name = match.group(1)
            env_value = os.environ.get(var_name)
            if env_value is None:
                raise ConfigError(
                    "Environment variable '%s' referenced in config is not set" % var_name
                )
            return env_value

        return _ENV_PATTERN.sub(_replace, value)
    return value


def load_raw(path: Path) -> dict[str, Any]:
    """Read the YAML file, apply ${ENV} substitution and return a raw mapping."""
    if not path.is_file():
        raise ConfigError("Config file not found: %s" % path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError("Invalid YAML in %s: %s" % (path, exc))
    if not isinstance(raw, dict):
        raise ConfigError("Config root must be a mapping, got: %s" % type(raw).__name__)
    return substitute_env(raw)


def load_config(path: os.PathLike[str] | str) -> AppConfig:
    """Load and validate the configuration file.

    Raises :class:`ConfigError` on missing file, broken YAML, missing env
    variables or schema validation errors.
    """
    config_path = Path(path)
    raw = load_raw(config_path)
    try:
        return AppConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError("Invalid configuration %s: %s" % (config_path, exc))


# ---------------------------------------------------------------------------
# Hot reload manager (last-good semantics)
# ---------------------------------------------------------------------------


class ConfigManager:
    """Holds the current (last-good) configuration and supports hot reload.

    Hot reload is opt-in: polling (and the POSIX SIGHUP handler) is only
    started when the loaded configuration sets ``config.reload: true``;
    otherwise :meth:`start` is a no-op. Reload triggers on a changed file
    fingerprint (mtime, size), polled every ``poll_interval_s`` seconds, and
    on POSIX SIGHUP when available. Windows development is fully supported
    without any signals.
    """

    def __init__(self, path: os.PathLike[str] | str, poll_interval_s: float = 5.0) -> None:
        self._path = Path(path)
        self._poll_interval_s = poll_interval_s
        self._config: AppConfig | None = None
        self._fingerprint: tuple[int, int] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._sighup_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None  # created inside the event loop
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def load_initial(self) -> AppConfig:
        """Load the configuration for the first time. Raises on any error."""
        config = load_config(self._path)
        with self._lock:
            self._config = config
            self._fingerprint = self._current_fingerprint()
        return config

    def get(self) -> AppConfig:
        """Return the current last-good configuration."""
        config = self._config
        if config is None:
            raise ConfigError("Configuration has not been loaded yet")
        return config

    def reload_now(self) -> bool:
        """Try to reload the configuration synchronously.

        Returns True if a new valid configuration was applied, False when the
        reload failed (the last-good configuration is kept in that case).
        """
        try:
            config = load_config(self._path)
        except Exception as exc:  # noqa: BLE001 - any failure keeps last-good
            logger.warning("Config reload failed, keeping last-good config: %s", exc)
            return False
        with self._lock:
            self._config = config
            self._fingerprint = self._current_fingerprint()
        logger.info("Configuration reloaded from %s", self._path)
        return True

    async def start(self) -> None:
        """Start hot reload if (and only if) enabled by the loaded config.

        The poll loop and the SIGHUP handler run only when the loaded
        configuration has ``config.reload: true``; with ``reload: false``
        this method does nothing (and logs it).
        """
        if self._config is None:
            raise ConfigError("Configuration has not been loaded yet")
        if not self._config.config.reload:
            logger.info("Config hot reload disabled (config.reload=false)")
            return
        self._stop_event = asyncio.Event()
        self._poll_task = asyncio.get_running_loop().create_task(self._poll_loop())
        try:
            asyncio.get_running_loop().add_signal_handler(signal.SIGHUP, self._handle_sighup)
        except (AttributeError, NotImplementedError, RuntimeError):
            # No SIGHUP on Windows (or not in the main thread): polling only.
            pass

    async def stop(self) -> None:
        """Stop the polling loop and release the signal handler."""
        if self._stop_event is not None:
            self._stop_event.set()
        for task in (self._poll_task, self._sighup_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    # Since Python 3.8 CancelledError inherits from
                    # BaseException, so it must be caught explicitly (not
                    # via `except Exception`).
                    pass
                except Exception:  # noqa: BLE001 - best-effort shutdown
                    pass
        self._poll_task = None
        self._sighup_task = None
        try:
            asyncio.get_running_loop().remove_signal_handler(signal.SIGHUP)
        except (AttributeError, NotImplementedError, RuntimeError):
            pass

    # -- internals ----------------------------------------------------------

    def _current_fingerprint(self) -> tuple[int, int] | None:
        try:
            stat = self._path.stat()
            return (stat.st_mtime_ns, stat.st_size)
        except OSError:
            return None

    def _file_changed(self) -> bool:
        return self._current_fingerprint() != self._fingerprint

    def _handle_sighup(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._sighup_task = loop.create_task(asyncio.to_thread(self.reload_now))

    async def _poll_loop(self) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval_s)
                break  # stop requested
            except asyncio.TimeoutError:
                pass
            if self._file_changed():
                await asyncio.to_thread(self.reload_now)
