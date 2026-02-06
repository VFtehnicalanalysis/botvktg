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

    vk_group_id = get_int("VK_GROUP_ID", 0)
    vk_token = get("VK_GROUP_TOKEN", "")
    vk_user_token = get("VK_USER_TOKEN", "")
    tg_bot_token = get("TG_BOT_TOKEN", "")
    tg_channel_id = get("TG_CHANNEL_ID", "")
    owner_id = get_int("OWNER_ID", 0)
    moderator_ids = get_int_list("MODERATOR_IDS", ())
    moderation_mode = get("MODERATION_MODE", "required").lower()
    source_mode = get("SOURCE_MODE", "vk+site").lower()

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
        dry_run=dry_run,
        log_dir=Path("logs"),
        state_path=Path("state.json"),
    )
