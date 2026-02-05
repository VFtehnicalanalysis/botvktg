from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx

from .config import Config

log = logging.getLogger(__name__)


class TelegramClient:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.base_url = f"https://api.telegram.org/bot{self.config.tg_bot_token}"
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self.client.aclose()

    async def _request(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        while True:
            resp = await self.client.post(f"{self.base_url}/{method}", json=payload)
            if resp.status_code == 429:
                retry_after = resp.json().get("parameters", {}).get("retry_after", 1)
                await asyncio.sleep(int(retry_after) + 1)
                continue
            if not resp.is_success:
                log.error("Telegram %s failed: %s", method, resp.text)
            data = resp.json()
            if not data.get("ok"):
                log.error("Telegram %s error: %s", method, data)
            return data

    async def send_message(
        self,
        chat_id: int | str,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: Optional[Dict[str, Any]] = None,
        disable_web_page_preview: bool = True,
    ) -> Optional[int]:
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
            "disable_web_page_preview": disable_web_page_preview,
        }
        if reply_markup:
            payload["reply_markup"] = reply_markup
        if self.config.dry_run:
            log.info("[dry-run] sendMessage to %s: %s", chat_id, text[:200])
            return None
        data = await self._request("sendMessage", payload)
        return data.get("result", {}).get("message_id")

    async def send_media_group(
        self, chat_id: int | str, media: Sequence[Dict[str, Any]]
    ) -> List[int]:
        payload: Dict[str, Any] = {"chat_id": chat_id, "media": media}
        if self.config.dry_run:
            log.info("[dry-run] sendMediaGroup to %s: %s items", chat_id, len(media))
            return []
        data = await self._request("sendMediaGroup", payload)
        if not data.get("result"):
            return []
        return [msg["message_id"] for msg in data["result"]]

    async def send_photo(
        self,
        chat_id: int | str,
        photo: str,
        caption: Optional[str] = None,
        parse_mode: str = "HTML",
    ) -> Optional[int]:
        payload: Dict[str, Any] = {"chat_id": chat_id, "photo": photo}
        if caption:
            payload["caption"] = caption
            payload["parse_mode"] = parse_mode
        if self.config.dry_run:
            log.info("[dry-run] sendPhoto to %s", chat_id)
            return None
        data = await self._request("sendPhoto", payload)
        return data.get("result", {}).get("message_id")

    async def send_video(
        self,
        chat_id: int | str,
        video: str,
        caption: Optional[str] = None,
        parse_mode: str = "HTML",
    ) -> Optional[int]:
        payload: Dict[str, Any] = {"chat_id": chat_id, "video": video}
        if caption:
            payload["caption"] = caption
            payload["parse_mode"] = parse_mode
        if self.config.dry_run:
            log.info("[dry-run] sendVideo to %s", chat_id)
            return None
        data = await self._request("sendVideo", payload)
        return data.get("result", {}).get("message_id")

    async def send_poll(
        self,
        chat_id: int | str,
        question: str,
        options: Sequence[str],
        is_anonymous: bool = True,
    ) -> Optional[int]:
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "question": question,
            "options": list(options),
            "is_anonymous": is_anonymous,
        }
        if self.config.dry_run:
            log.info("[dry-run] sendPoll to %s", chat_id)
            return None
        data = await self._request("sendPoll", payload)
        return data.get("result", {}).get("message_id")

    async def notify_owner(self, text: str) -> None:
        await self.send_message(chat_id=self.config.owner_id, text=text)

    async def answer_callback_query(self, callback_id: str, text: Optional[str] = None) -> None:
        payload: Dict[str, Any] = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        if self.config.dry_run:
            log.info("[dry-run] answerCallbackQuery: %s", callback_id)
            return
        await self._request("answerCallbackQuery", payload)

    async def delete_message(self, chat_id: int | str, message_id: int) -> bool:
        payload: Dict[str, Any] = {"chat_id": chat_id, "message_id": message_id}
        if self.config.dry_run:
            log.info("[dry-run] deleteMessage in %s: %s", chat_id, message_id)
            return True
        data = await self._request("deleteMessage", payload)
        return bool(data.get("ok"))

    async def delete_messages(self, chat_id: int | str, message_ids: Sequence[int]) -> int:
        deleted = 0
        for message_id in message_ids:
            if await self.delete_message(chat_id=chat_id, message_id=message_id):
                deleted += 1
        return deleted

    async def get_updates(
        self,
        offset: Optional[int] = None,
        timeout: int = 20,
        allowed_updates: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {"timeout": timeout}
        if offset is not None:
            payload["offset"] = offset
        if allowed_updates:
            payload["allowed_updates"] = allowed_updates
        data = await self._request("getUpdates", payload)
        return data.get("result", [])
