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

    async def _api_call(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        params = {
            **params,
            "access_token": self.config.vk_token,
            "v": self.config.vk_api_version,
        }
        resp = await self.client.post(f"{self.api_url}/{method}", data=params)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"VK {method} error: {data['error']}")
        return data.get("response", data)

    async def get_longpoll_server(self) -> None:
        params = {"group_id": self.config.vk_group_id}
        server_info = await self._api_call("groups.getLongPollServer", params)
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
        params = {"owner_id": -abs(self.config.vk_group_id), "count": count}
        try:
            data = await self._api_call("wall.get", params)
        except Exception as exc:  # noqa: BLE001
            log.error("VK wall.get error: %s", exc)
            return []
        return data.get("items", [])

    async def wall_post(self, message: str, attachments: Optional[List[str]] = None) -> Optional[int]:
        if not self.config.vk_group_id or not self.config.vk_token:
            raise RuntimeError("VK credentials are not configured")
        params: Dict[str, Any] = {
            "owner_id": -abs(self.config.vk_group_id),
            "from_group": 1,
            "message": message,
        }
        if attachments:
            params["attachments"] = ",".join(attachments)
        data = await self._api_call("wall.post", params)
        return data.get("post_id")

    async def upload_wall_photos(self, urls: List[str], max_images: int = 10) -> List[str]:
        if not self.config.vk_group_id or not self.config.vk_token:
            raise RuntimeError("VK credentials are not configured")
        attachments: List[str] = []
        for url in urls[:max_images]:
            try:
                upload_url = await self._get_wall_upload_server()
                img_resp = await self.client.get(url, follow_redirects=True, timeout=20.0)
                img_resp.raise_for_status()
                filename = url.split("/")[-1] or "image.jpg"
                upload_resp = await self.client.post(upload_url, files={"photo": (filename, img_resp.content)})
                upload_resp.raise_for_status()
                upload_data = upload_resp.json()
                save_params = {
                    "group_id": self.config.vk_group_id,
                    "photo": upload_data.get("photo"),
                    "server": upload_data.get("server"),
                    "hash": upload_data.get("hash"),
                }
                saved = await self._api_call("photos.saveWallPhoto", save_params)
                if isinstance(saved, list) and saved:
                    photo = saved[0]
                elif isinstance(saved, dict) and saved.get("response"):
                    photo = saved["response"][0]
                else:
                    continue
                owner_id = photo.get("owner_id")
                photo_id = photo.get("id")
                if not owner_id or not photo_id:
                    continue
                access_key = photo.get("access_key")
                if access_key:
                    attachments.append(f"photo{owner_id}_{photo_id}_{access_key}")
                else:
                    attachments.append(f"photo{owner_id}_{photo_id}")
            except Exception as exc:  # noqa: BLE001
                log.warning("Skip image upload to VK: %s -> %s", url, exc)
        return attachments

    async def _get_wall_upload_server(self) -> str:
        params = {"group_id": self.config.vk_group_id}
        data = await self._api_call("photos.getWallUploadServer", params)
        upload_url = data.get("upload_url")
        if not upload_url:
            raise RuntimeError("VK upload_url not found")
        return upload_url
