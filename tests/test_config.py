"""Tests for configuration loading: env substitution, validation, hot reload."""

import asyncio

import pytest
import yaml

from app.config import ConfigError, ConfigManager, load_config


# ---------------------------------------------------------------------------
# ${ENV} substitution
# ---------------------------------------------------------------------------


def test_env_substitution_full_value(write_config, monkeypatch):
    monkeypatch.setenv("GW_TEST_TOKEN", "secret-token-123")
    path = write_config(notify={"telegram_token": "${GW_TEST_TOKEN}", "telegram_chat_id": ""})
    config = load_config(path)
    assert config.notify.telegram_token == "secret-token-123"


def test_env_substitution_inside_string(write_config, monkeypatch):
    monkeypatch.setenv("GW_DB_NAME", "custom.db")
    path = write_config(db_path="data/${GW_DB_NAME}")
    config = load_config(path)
    assert config.db_path == "data/custom.db"


def test_missing_env_is_a_validation_error(write_config, monkeypatch):
    monkeypatch.delenv("GW_DEFINITELY_MISSING", raising=False)
    path = write_config(
        notify={"telegram_token": "${GW_DEFINITELY_MISSING}", "telegram_chat_id": ""}
    )
    with pytest.raises(ConfigError):
        load_config(path)


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def test_valid_config_loads(write_config):
    config = load_config(write_config())
    assert config.whitelist == ["+79001234567"]
    assert config.llm.model == "GigaChat-2"
    assert config.llm.temperature == 0.4
    assert config.limits.max_reply_units == 201
    assert config.sessions_named == {1: "default"}
    assert config.gateway.base_url == "https://gw.example.com"
    assert config.config.reload is False
    assert config.rag.enabled is False


def test_whitelist_must_be_e164(write_config):
    path = write_config(whitelist=["89161234567"])
    with pytest.raises(ConfigError):
        load_config(path)


def test_unknown_keys_are_rejected(write_config):
    path = write_config(bogus_section={"x": 1})
    with pytest.raises(ConfigError):
        load_config(path)


def test_broken_yaml_is_a_validation_error(tmp_path):
    path = tmp_path / "broken.yaml"
    path.write_text("whitelist: [", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(path)


# ---------------------------------------------------------------------------
# Hot reload with last-good semantics
# ---------------------------------------------------------------------------


def test_hot_reload_keeps_last_good_on_broken_yaml(config_file):
    manager = ConfigManager(config_file)
    original = manager.load_initial()

    config_file.write_text("whitelist: [", encoding="utf-8")  # broken YAML
    assert manager.reload_now() is False
    assert manager.get() is original  # the very same last-good object


def test_hot_reload_keeps_last_good_on_schema_error(config_file):
    manager = ConfigManager(config_file)
    original = manager.load_initial()

    invalid = {"whitelist": ["+79001234567"], "llm": {"model": "X"}}  # missing keys
    config_file.write_text(yaml.safe_dump(invalid), encoding="utf-8")
    assert manager.reload_now() is False
    assert manager.get() is original
    assert manager.get().llm.model == "GigaChat-2"


def test_hot_reload_applies_valid_changes(config_file):
    manager = ConfigManager(config_file)
    original = manager.load_initial()

    data = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    data["llm"]["temperature"] = 1.5
    data["limits"]["max_sessions"] = 5
    config_file.write_text(yaml.safe_dump(data), encoding="utf-8")

    assert manager.reload_now() is True
    assert manager.get() is not original
    assert manager.get().llm.temperature == 1.5
    assert manager.get().limits.max_sessions == 5


def test_hot_reload_of_missing_file_keeps_last_good(config_file, tmp_path):
    manager = ConfigManager(config_file)
    original = manager.load_initial()

    config_file.unlink()
    assert manager.reload_now() is False
    assert manager.get() is original


# ---------------------------------------------------------------------------
# Polling loop (asyncio) + config.reload flag
# ---------------------------------------------------------------------------


def test_poll_loop_applies_valid_changes_when_enabled(write_config):
    """With config.reload=true the poll loop picks up a valid new config.

    Regression test: the poll loop runs reload in a worker thread via
    asyncio.to_thread; a broken loop would never apply the new value.
    """
    config_path = write_config(config={"reload": True})

    async def scenario():
        manager = ConfigManager(config_path, poll_interval_s=0.05)
        manager.load_initial()
        assert manager.get().llm.temperature == 0.4

        await manager.start()
        try:
            data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            data["llm"]["temperature"] = 1.5
            config_path.write_text(yaml.safe_dump(data), encoding="utf-8")

            applied = False
            for _ in range(250):
                await asyncio.sleep(0.02)
                if manager.get().llm.temperature == 1.5:
                    applied = True
                    break
            assert applied, "poll loop did not apply the valid config change"
        finally:
            await manager.stop()

    asyncio.run(scenario())


def test_poll_loop_disabled_ignores_file_changes(config_file):
    """With config.reload=false the poll loop must not be started at all."""
    assert yaml.safe_load(config_file.read_text(encoding="utf-8"))["config"]["reload"] is False

    async def scenario():
        manager = ConfigManager(config_file, poll_interval_s=0.05)
        original = manager.load_initial()

        await manager.start()
        try:
            data = yaml.safe_load(config_file.read_text(encoding="utf-8"))
            data["llm"]["temperature"] = 1.5
            config_file.write_text(yaml.safe_dump(data), encoding="utf-8")

            await asyncio.sleep(0.3)  # several poll intervals

            # The last-good config is untouched: reload is disabled.
            assert manager.get() is original
            assert manager.get().llm.temperature == 0.4
        finally:
            await manager.stop()

    asyncio.run(scenario())
