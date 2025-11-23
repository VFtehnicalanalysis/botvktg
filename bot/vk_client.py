from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx

from .config import Config

log = logging.getLogger(__name__)


class VKClient:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.api_url = "https://api.vk.com/method"
        self.client = httpx.AsyncClient(timeout=30.0)
        self.longpoll_server: Optional[str] = None
        self.longpoll_key: Optional[str] = None
        self.ts: Optional[str] = None

    async def close(self) -> None:
        await self.client.aclose()

    async def get_longpoll_server(self) -> None:
        params = {
            "group_id": self.config.vk_group_id,
            "access_token": self.config.vk_token,
            "v": self.config.vk_api_version,
        }
        resp = await self.client.get(f"{self.api_url}/groups.getLongPollServer", params=params)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"VK error: {data['error']}")
        server_info = data["response"]
        self.longpoll_server = server_info["server"]
        self.longpoll_key = server_info["key"]
        self.ts = server_info["ts"]
        log.info("LongPoll server updated: %s", self.longpoll_server)

    async def longpoll(self) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        if not self.longpoll_server or not self.longpoll_key or not self.ts:
            await self.get_longpoll_server()
        assert self.longpoll_server and self.longpoll_key and self.ts

        params = {
            "act": "a_check",
            "key": self.longpoll_key,
            "ts": self.ts,
            "wait": self.config.longpoll_wait,
        }
        if self.longpoll_server.startswith("http"):
            url = self.longpoll_server
        else:
            url = f"https://{self.longpoll_server}"
        if not self.longpoll_server:
            raise RuntimeError("LongPoll server is empty")
        try:
            resp = await self.client.get(url, params=params)
        except httpx.RequestError as exc:
            log.error("LongPoll request error (server=%s): %s", self.longpoll_server, exc)
            # Попробуем обновить сервер, чтобы получить новый host
            await self.get_longpoll_server()
            raise
        resp.raise_for_status()
        data = resp.json()
        failed = data.get("failed")
        if failed:
            if failed in (1, 2):
                self.ts = data.get("ts") or self.ts
            elif failed == 3:
                await self.get_longpoll_server()
            return self.ts, []
        self.ts = data.get("ts", self.ts)
        updates = data.get("updates", [])
        return self.ts, updates

    async def wall_get_recent(self, count: int = 5) -> List[Dict[str, Any]]:
        params = {
            "owner_id": -abs(self.config.vk_group_id),
            "count": count,
            "access_token": self.config.vk_token,
            "v": self.config.vk_api_version,
        }
        resp = await self.client.get(f"{self.api_url}/wall.get", params=params)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            log.error("VK wall.get error: %s", data["error"])
            return []
        return data.get("response", {}).get("items", [])
