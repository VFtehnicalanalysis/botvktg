from __future__ import annotations

import logging
import time
from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Deque, Optional, Tuple

_RECENT_WINDOW_SECONDS = 3 * 60
_RECENT_MAX_RECORDS = 5000
_recent_handler: Optional["RecentLogsMemoryHandler"] = None


class RecentLogsMemoryHandler(logging.Handler):
    def __init__(self, max_records: int = _RECENT_MAX_RECORDS) -> None:
        super().__init__(level=logging.INFO)
        self._records: Deque[Tuple[float, str]] = deque(maxlen=max_records)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
        except Exception:  # noqa: BLE001
            return
        self._records.append((time.time(), line))

    def dump_recent(self, window_seconds: int = _RECENT_WINDOW_SECONDS) -> str:
        threshold = time.time() - max(1, int(window_seconds))
        lines = [line for ts, line in self._records if ts >= threshold]
        if lines:
            return "\n".join(lines)
        return ""


def get_recent_logs_text(window_seconds: int = _RECENT_WINDOW_SECONDS) -> str:
    if not _recent_handler:
        return ""
    return _recent_handler.dump_recent(window_seconds=window_seconds)


def setup_logging(log_dir: Path) -> None:
    global _recent_handler
    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    formatter = logging.Formatter(fmt)

    handlers = []
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_dir / "bot.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    except Exception:
        # В контейнере каталог может быть read-only; продолжаем с stdout-логированием.
        pass

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    if _recent_handler is None:
        _recent_handler = RecentLogsMemoryHandler()
        _recent_handler.setFormatter(formatter)
    handlers.append(_recent_handler)

    logging.basicConfig(level=logging.INFO, handlers=handlers)
