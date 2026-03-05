from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple


@dataclass
class Config:
    vk_group_id: int
    vk_token: str  # group token
    vk_user_token: str  # user token for read/upload methods unavailable for group token
    tg_bot_token: str
    tg_channel_id: str
    owner_id: int
    moderator_ids: Tuple[int, ...]
    moderation_mode: str  # "required" or "off"
    source_mode: str  # "vk", "site", "vk+site"
    vk_api_version: str = "5.199"
    longpoll_wait: int = 25
    log_dir: Path = Path("logs")
    state_path: Path = Path("state.json")
    dry_run: bool = False
    site_base_url: str = "https://www.econ.msu.ru"
    site_news_path: str = "/alumni/"
    site_poll_interval: int = 900  # seconds
    user_response_interval_seconds: float = 1.0
    monitor_min_interval_seconds: int = 60
    restart_backoff_seconds: int = 5
    monitor_inactivity_restart_seconds: int = 300
    restart_reason_path: Path = Path("restart_reason.txt")

    @property
    def moderation_required(self) -> bool:
        return self.moderation_mode.lower() == "required"

    @property
    def all_moderator_ids(self) -> Tuple[int, ...]:
        ordered: list[int] = []
        for user_id in (self.owner_id, *self.moderator_ids):
            if not user_id or user_id in ordered:
                continue
            ordered.append(user_id)
        return tuple(ordered)

    def is_owner(self, user_id: Optional[int]) -> bool:
        return bool(user_id) and int(user_id) == self.owner_id

    def is_moderator(self, user_id: Optional[int]) -> bool:
        if not user_id:
            return False
        return int(user_id) in self.all_moderator_ids


def _parse_kv_file(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    for line in path.read_text().splitlines():
        if not line.strip() or line.strip().startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.split("#", 1)[0].strip()
        data[key.strip()] = value.strip()
    return data


def load_config(password_file: Path = Path("password"), dry_run: bool = False) -> Config:
    """
    Загружает конфиг из файла password и окружения. Окружение имеет приоритет.
    Токены не выводятся в логи.
    """
    file_vars = _parse_kv_file(password_file)
    env = os.environ

    def get(name: str, default: Optional[str] = None) -> str:
        return env.get(name, file_vars.get(name, default))

    def get_int(name: str, default: int = 0) -> int:
        raw = str(get(name, str(default)) or "").strip()
        if not raw:
            return default
        try:
            return int(raw)
        except ValueError:
            return default

    def get_int_list(name: str, default: Tuple[int, ...] = ()) -> Tuple[int, ...]:
        raw = str(get(name, "") or "").strip()
        if not raw:
            return default
        parsed: list[int] = []
        for part in raw.replace(";", ",").split(","):
            piece = part.strip()
            if not piece:
                continue
            try:
                value = int(piece)
            except ValueError:
                continue
            if not value or value in parsed:
                continue
            parsed.append(value)
        return tuple(parsed)

    def get_float(name: str, default: float) -> float:
        raw = str(get(name, str(default)) or "").strip()
        if not raw:
            return default
        try:
            return float(raw)
        except ValueError:
            return default

    vk_group_id = get_int("VK_GROUP_ID", 0)
    vk_token = get("VK_GROUP_TOKEN", "")
    vk_user_token = get("VK_USER_TOKEN", "")
    tg_bot_token = get("TG_BOT_TOKEN", "")
    tg_channel_id = get("TG_CHANNEL_ID", "")
    owner_id = get_int("OWNER_ID", 0)
    moderator_ids = get_int_list("MODERATOR_IDS", ())
    moderation_mode = get("MODERATION_MODE", "required").lower()
    source_mode = get("SOURCE_MODE", "vk+site").lower()
    site_base_url = str(get("SITE_BASE_URL", "https://www.econ.msu.ru") or "").strip() or "https://www.econ.msu.ru"
    site_news_path = str(get("SITE_NEWS_PATH", "/alumni/") or "").strip() or "/alumni/"
    site_poll_interval = max(60, get_int("SITE_POLL_INTERVAL", 900))
    user_response_interval_seconds = max(0.0, get_float("USER_RESPONSE_INTERVAL_SECONDS", 1.0))
    monitor_min_interval_seconds = max(1, get_int("MONITOR_MIN_INTERVAL_SECONDS", 60))
    restart_backoff_seconds = max(1, get_int("RESTART_BACKOFF_SECONDS", 5))
    monitor_inactivity_restart_seconds = max(60, get_int("MONITOR_INACTIVITY_RESTART_SECONDS", 300))
    restart_reason_path = Path(str(get("RESTART_REASON_PATH", "restart_reason.txt") or "restart_reason.txt"))

    return Config(
        vk_group_id=vk_group_id,
        vk_token=vk_token,
        vk_user_token=vk_user_token,
        tg_bot_token=tg_bot_token,
        tg_channel_id=tg_channel_id,
        owner_id=owner_id,
        moderator_ids=moderator_ids,
        moderation_mode=moderation_mode,
        source_mode=source_mode,
        site_base_url=site_base_url.rstrip("/"),
        site_news_path=site_news_path if site_news_path.startswith("/") else f"/{site_news_path}",
        site_poll_interval=site_poll_interval,
        dry_run=dry_run,
        log_dir=Path("logs"),
        state_path=Path("state.json"),
        user_response_interval_seconds=user_response_interval_seconds,
        monitor_min_interval_seconds=monitor_min_interval_seconds,
        restart_backoff_seconds=restart_backoff_seconds,
        monitor_inactivity_restart_seconds=monitor_inactivity_restart_seconds,
        restart_reason_path=restart_reason_path,
    )
