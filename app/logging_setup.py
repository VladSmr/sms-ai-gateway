"""Logging setup: JSON records to stdout and a rotating file.

PRIVACY RULE (152-FZ / GDPR): NEVER log message text or payload contents.
Only metadata is allowed: timestamps, sender numbers, lengths, statuses,
session numbers, latencies, counters.

- File handler: RotatingFileHandler, 10 MB per file, 14 rotated files kept.
- Stdout handler: same JSON format.
- Level: ``LOG_LEVEL`` env var (default ``INFO``); invalid values fall back
  to ``INFO`` with a warning.
"""

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_LOG_FILE = "logs/app.log"
MAX_BYTES = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 14

# Standard LogRecord attributes; everything else is treated as caller-provided
# extra fields and embedded into the JSON entry.
_RESERVED_ATTRS = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRS and not key.startswith("_"):
                entry[key] = value
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def _resolve_level(level: str | None) -> int:
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO")
    resolved = getattr(logging, str(level).strip().upper(), None)
    if not isinstance(resolved, int):
        sys.stderr.write("Unknown log level '%s', falling back to INFO\n" % level)
        resolved = logging.INFO
    return resolved


def setup_logging(level: str | None = None, log_file: str | None = None) -> None:
    """Configure root logging (JSON to stdout + rotating file).

    Safe to call multiple times: previously installed handlers are closed and
    replaced (avoids duplicated lines and file-handle leaks on reload/tests).
    """
    resolved_level = _resolve_level(level)
    root = logging.getLogger()
    root.setLevel(resolved_level)
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # noqa: BLE001 - cleanup must not break setup
            pass

    formatter = JsonFormatter()

    file_path = Path(log_file or os.environ.get("LOG_FILE", DEFAULT_LOG_FILE))
    if file_path.parent and not file_path.parent.exists():
        file_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        str(file_path),
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)
