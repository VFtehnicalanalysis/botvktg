from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple

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
        self._warned_wall_get_modes: Set[str] = set()
        self._warned_upload_fallback = False

    async def close(self) -> None:
        await self.client.aclose()

    async def _api_call(
        self,
        method: str,
        params: Dict[str, Any],
        access_token: Optional[str] = None,
        include_token: bool = True,
    ) -> Dict[str, Any]:
        payload = {**params, "v": self.config.vk_api_version}
        if include_token:
            token = (access_token or "").strip() or self.config.vk_token
            payload["access_token"] = token
        resp = await self.client.post(f"{self.api_url}/{method}", data=payload)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"VK {method} error: {data['error']}")
        return data.get("response", data)

    async def get_longpoll_server(self) -> None:
        params = {"group_id": self.config.vk_group_id}
        server_info = await self._api_call("groups.getLongPollServer", params, access_token=self.config.vk_token)
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
        except httpx.ReadTimeout:
            log.warning("LongPoll timeout (server=%s), continue polling", self.longpoll_server)
            return self.ts, []
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
        read_token = self._get_read_token()
        if not read_token:
            mode = "no_user_token"
            if mode not in self._warned_wall_get_modes:
                log.info(
                    "wall.get skipped (no VK_USER_TOKEN); "
                    "Latest VK post button uses cached post from LongPoll."
                )
                self._warned_wall_get_modes.add(mode)
            return []
        params = {"owner_id": -abs(self.config.vk_group_id), "count": count}
        try:
            data = await self._api_call(
                "wall.get",
                params,
                access_token=read_token,
                include_token=True,
            )
            items = data.get("items", [])
            if isinstance(items, list):
                return items
        except Exception as exc:  # noqa: BLE001
            mode = "user"
            if mode not in self._warned_wall_get_modes:
                log.warning("VK wall.get failed in %s mode: %s", mode, exc)
                self._warned_wall_get_modes.add(mode)
        return []

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
        try:
            data = await self._api_call("wall.post", params, access_token=self.config.vk_token)
            return data.get("post_id")
        except RuntimeError as exc:
            err_text = str(exc)
            # VK иногда отклоняет link-attachments (raw.php/страница) с ошибкой link_photo_sizing_rule.
            # В этом случае публикуем без attachments, чтобы не ломать поток модерации.
            if attachments and "link_photo_sizing_rule" in err_text:
                log.warning(
                    "VK rejected wall attachments (link_photo_sizing_rule); retrying wall.post without attachments."
                )
                retry_params: Dict[str, Any] = {
                    "owner_id": -abs(self.config.vk_group_id),
                    "from_group": 1,
                    "message": message,
                }
                data = await self._api_call("wall.post", retry_params, access_token=self.config.vk_token)
                return data.get("post_id")
            raise

    async def upload_wall_photos(self, urls: List[str], max_images: int = 10) -> List[str]:
        if not self.config.vk_group_id or not self.config.vk_token:
            raise RuntimeError("VK credentials are not configured")
        upload_token = self._get_upload_token()
        attachments: List[str] = []
        upload_api_blocked = False
        for url in urls[:max_images]:
            clean_url = str(url or "").strip()
            if not clean_url:
                continue
            if upload_api_blocked:
                continue
            try:
                upload_url = await self._get_wall_upload_server(access_token=upload_token)
                img_resp = await self.client.get(clean_url, follow_redirects=True, timeout=20.0)
                img_resp.raise_for_status()
                filename = clean_url.split("/")[-1] or "image.jpg"
                upload_resp = await self.client.post(upload_url, files={"photo": (filename, img_resp.content)})
                upload_resp.raise_for_status()
                upload_data = upload_resp.json()
                save_params = {
                    "group_id": self.config.vk_group_id,
                    "photo": upload_data.get("photo"),
                    "server": upload_data.get("server"),
                    "hash": upload_data.get("hash"),
                }
                saved = await self._api_call("photos.saveWallPhoto", save_params, access_token=upload_token)
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
                err = str(exc)
                if "method is unavailable with group auth" in err or "error_code': 27" in err:
                    upload_api_blocked = True
                    if not self._warned_upload_fallback:
                        log.warning(
                            "VK photos upload API unavailable for current token; "
                            "fallback to URL attachments."
                        )
                        self._warned_upload_fallback = True
                else:
                    log.warning("Skip image upload to VK via API: %s -> %s", clean_url, exc)
        return attachments

    async def _get_wall_upload_server(self, access_token: Optional[str] = None) -> str:
        params = {"group_id": self.config.vk_group_id}
        data = await self._api_call("photos.getWallUploadServer", params, access_token=access_token)
        upload_url = data.get("upload_url")
        if not upload_url:
            raise RuntimeError("VK upload_url not found")
        return upload_url

    def _get_read_token(self) -> str:
        return (self.config.vk_user_token or "").strip()

    def _get_upload_token(self) -> str:
        return (self.config.vk_user_token or "").strip() or self.config.vk_token
