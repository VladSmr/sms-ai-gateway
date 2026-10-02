"""Shared pytest fixtures: config files and an app test client."""

import copy
import sys
from pathlib import Path

import pytest
import yaml

# Make the "app" package importable regardless of pytest rootdir.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

VALID_CONFIG = {
    "whitelist": ["+79001234567"],
    "system_prompt": "Answer briefly, max 200 characters.",
    "llm": {
        "model": "GigaChat-2",
        "temperature": 0.4,
        "max_tokens": 500,
        "timeout_s": 90,
        # Tests never touch the real API: mock mode needs no auth_key
        # (the main.py fail-fast for an empty key is skipped in mock mode).
        "mock": True,
        "auth_key": "",
    },
    "limits": {
        "max_sessions": 10,
        "history_messages": 10,
        "session_ttl_h": 24,
        "max_reply_units": 201,
    },
    "sessions_named": {1: "default"},
    # Tests never touch the real gateway: mock mode needs no credentials
    # (the main.py fail-fast for empty credentials is skipped in mock mode).
    "gateway": {"base_url": "https://gw.example.com", "mock": True},
    "webhook": {
        # Test-only signing key: real deployments inject the gateway's
        # "Signing Key" via ${GATEWAY_HMAC_SECRET} from .env.
        "secret": "test-webhook-secret",
        "rate_limit_per_min": 120,
        "max_body_bytes": 65536,
        "timestamp_window_s": 300,
    },
    "notify": {"telegram_token": "", "telegram_chat_id": "", "heartbeat_interval_min": 15},
    "rag": {"enabled": False},
    "config": {"reload": False},
    "db_path": "data/gateway.db",
    "log_level": "INFO",
}


@pytest.fixture
def write_config(tmp_path):
    """Return a helper that writes a (possibly overridden) config YAML to tmp."""

    def _write(**overrides) -> Path:
        data = copy.deepcopy(VALID_CONFIG)
        for key, value in overrides.items():
            data[key] = value
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
        return path

    return _write


@pytest.fixture
def config_file(write_config) -> Path:
    """A plain valid config file inside tmp_path."""
    return write_config()


@pytest.fixture
def client(config_file, tmp_path):
    """TestClient with a fully started app (lifespan executed)."""
    from fastapi.testclient import TestClient

    from app.main import create_app

    app = create_app(config_path=str(config_file), log_file=str(tmp_path / "logs" / "app.log"))
    with TestClient(app) as test_client:
        yield test_client
