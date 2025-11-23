from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .config import Config
from .state import StateStore
from .tg_client import TelegramClient

log = logging.getLogger(__name__)


def escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def normalize_vk_markup(text: str) -> str:
    """
    Конвертирует VK-формат ссылок [alias|text|url?] или [alias|text] в чистый URL без лишних скобок.
    Приоритет: url из третьей части, иначе alias, если он похож на url, иначе если label похож на url.
    Если урла нет, возвращает label.
    """
    import re

    def looks_like_url(value: str) -> bool:
        return value.startswith(("http://", "https://", "www."))

    def normalize_url(value: str) -> str:
        if value.startswith("www."):
            return f"https://{value}"
        return value

    def repl(match: re.Match[str]) -> str:
        alias, label, url = match.group(1), match.group(2), match.group(3)
        if url and looks_like_url(url):
            return normalize_url(url)
        if looks_like_url(alias):
            return normalize_url(alias)
        if looks_like_url(label):
            return normalize_url(label)
        return label

    return re.sub(r"\[([^|\]]+)\|([^|\]]+)(?:\|([^\]]+))?\]", repl, text)


def chunk_text(text: str, limit: int = 3500) -> List[str]:
    if len(text) <= limit:
        return [text]
    parts: List[str] = []
    start = 0
    while start < len(text):
        parts.append(text[start : start + limit])
        start += limit
    return parts


class Pipeline:
    def __init__(self, config: Config, state: StateStore, tg: TelegramClient) -> None:
        self.config = config
        self.state = state
        self.tg = tg
        self.pending_cache: Dict[int, Dict[str, Any]] = {}

    async def handle_vk_update(self, update: Dict[str, Any]) -> None:
        if update.get("type") == "wall_post_new":
            post = update.get("object") or update.get("wall_post") or {}
            await self.handle_post(post, source="longpoll")
        elif update.get("type") == "wall_post_edit":
            post = update.get("object") or update.get("wall_post") or {}
            await self.handle_post(post, source="edit")

    async def handle_post(self, post: Dict[str, Any], source: str) -> None:
        post_id = post.get("id") or post.get("post_id")
        if post_id is None:
            return
        post_type = post.get("post_type", "post")
        if post_type in {"suggest", "postpone"}:
            return
        normalized = self._normalize_post(post)
        content_hash = self._hash_payload(normalized)
        existing = await self.state.get_post_record(post_id) or {}
        existing_hash = existing.get("hash")
        status = existing.get("status")
        if existing_hash == content_hash:
            log.info("Skip post %s (duplicate hash)", post_id)
            return
        if status == "published":
            log.info("Post %s already published; skip edit to avoid duplicate", post_id)
            return
        if await self.state.should_skip(post_id, content_hash):
            log.info("Skip post %s (duplicate hash)", post_id)
            return
        token = str(uuid.uuid4()) if self.config.moderation_required else None
        await self.state.mark_pending(post_id, content_hash, token, payload=normalized)
        self.pending_cache[post_id] = normalized
        if self.config.moderation_required:
            await self._send_for_moderation(post_id, normalized, token)
        else:
            await self._publish(post_id, normalized)

    async def handle_callback(self, update: Dict[str, Any]) -> None:
        cb = update.get("callback_query")
        if not cb:
            return
        data = cb.get("data") or ""
        cb_id = cb.get("id")
        from_user = cb.get("from", {})
        user_id = from_user.get("id")
        if data.startswith("approve:") or data.startswith("reject:"):
            if user_id != self.config.owner_id:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Нет доступа")
                return
            action, token = data.split(":", 1)
            post_id = await self.state.get_post_by_token(token)
            if post_id is None:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Устарело")
                return
            payload = self.pending_cache.get(post_id) or await self.state.get_payload(post_id)
            if not payload:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Нет данных")
                return
            if action == "approve":
                await self.state.mark_approved(post_id)
                await self.state.invalidate_token(token)
                await self._publish(post_id, payload)
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Одобрено")
            elif action == "reject":
                await self.state.mark_rejected(post_id)
                await self.state.invalidate_token(token)
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Отклонено")

    def _normalize_post(self, post: Dict[str, Any]) -> Dict[str, Any]:
        text = post.get("text", "") or ""
        owner_id = post.get("owner_id") or 0
        post_id = post.get("id") or post.get("post_id") or 0
        vk_url = f"https://vk.com/wall{owner_id}_{post_id}"
        attachments = post.get("attachments", []) or []
        parsed_media: List[Dict[str, Any]] = []
        extra_lines: List[str] = []
        poll_obj: Optional[Dict[str, Any]] = None

        for att in attachments:
            att_type = att.get("type")
            if att_type == "photo":
                photo = att.get("photo") or {}
                sizes = photo.get("sizes") or []
                if sizes:
                    best = max(sizes, key=lambda s: s.get("width", 0) * s.get("height", 0))
                    parsed_media.append({"type": "photo", "url": best.get("url")})
            elif att_type == "video":
                video = att.get("video") or {}
                video_link = f"https://vk.com/video{video.get('owner_id')}_{video.get('id')}"
                extra_lines.append(f"Видео: {video_link}")
            elif att_type == "poll":
                poll = att.get("poll") or {}
                poll_obj = {
                    "question": poll.get("question", "")[:255],
                    "options": [ans.get("text", "")[:255] for ans in poll.get("answers", [])],
                    "is_anonymous": poll.get("anonymous", True),
                }
            else:
                link = att.get(att_type, {}).get("url")
                title = att.get(att_type, {}).get("title") or att_type
                if link:
                    extra_lines.append(f"{title}: {link}")

        if post.get("copy_history"):
            src = post["copy_history"][0]
            src_text = src.get("text") or ""
            if src_text:
                text = f"{text}\n\n[Перепост]:\n{src_text}"

        if extra_lines:
            extra_text = "\n".join(extra_lines)
            text = f"{text}\n\n{extra_text}" if text else extra_text

        text = normalize_vk_markup(text.strip())

        return {
            "post_id": post_id,
            "owner_id": owner_id,
            "text": text.strip(),
            "media": parsed_media,
            "poll": poll_obj,
            "vk_url": vk_url,
        }

    def _hash_payload(self, payload: Dict[str, Any]) -> str:
        dumped = json.dumps(payload, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()

    async def refresh_recent_posts(self, vk_client: "VKClient", count: int = 10) -> None:  # type: ignore
        log.info("Manual refresh of recent posts (count=%s)", count)
        items = await vk_client.wall_get_recent(count=count)
        for item in reversed(items):
            await self.handle_post(item, source="manual-refresh")

    async def _send_for_moderation(self, post_id: int, payload: Dict[str, Any], token: Optional[str]) -> None:
        log.info("Post %s pending moderation", post_id)
        text = escape_html(payload["text"]) if payload["text"] else "(без текста)"
        vk_link = escape_html(payload.get("vk_url", ""))
        header = f"Новый пост #{post_id} из ВК:\n{vk_link}"
        full_text = f"{header}\n\n{text}" if text else header
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "✅ Одобрить", "callback_data": f"approve:{token}"},
                    {"text": "🚫 Отклонить", "callback_data": f"reject:{token}"},
                ]
            ]
        }
        parts = chunk_text(full_text, limit=1000)
        await self.tg.send_message(chat_id=self.config.owner_id, text=parts[0], reply_markup=keyboard)
        for extra in parts[1:]:
            await self.tg.send_message(chat_id=self.config.owner_id, text=extra)
        media: List[Dict[str, Any]] = payload.get("media") or []
        if len(media) == 1:
            m = media[0]
            if m["type"] == "photo":
                await self.tg.send_photo(chat_id=self.config.owner_id, photo=m["url"])
        elif len(media) > 1:
            group = [{"type": m["type"], "media": m["url"]} for m in media if m.get("url")]
            if group:
                await self.tg.send_media_group(chat_id=self.config.owner_id, media=group)
        poll = payload.get("poll")
        if poll:
            await self.tg.send_poll(
                chat_id=self.config.owner_id,
                question=poll["question"],
                options=poll["options"],
                is_anonymous=poll.get("is_anonymous", True),
            )

    async def _publish(self, post_id: int, payload: Dict[str, Any]) -> None:
        text = payload.get("text", "") or ""
        media: List[Dict[str, Any]] = payload.get("media") or []
        poll = payload.get("poll")
        message_ids: List[int] = []

        escaped_text = escape_html(text)
        chunks = chunk_text(escaped_text, limit=3500)

        if media:
            if len(media) == 1:
                m = media[0]
                cap = chunks[0] if chunks else None
                msg_id = await self.tg.send_photo(
                    chat_id=self.config.tg_channel_id, photo=m["url"], caption=cap
                )
                if msg_id:
                    message_ids.append(msg_id)
                for extra in chunks[1:]:
                    mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=extra)
                    if mid:
                        message_ids.append(mid)
            else:
                caption = chunks[0] if chunks else ""
                group: List[Dict[str, Any]] = []
                for idx, m in enumerate(media):
                    entry = {"type": m["type"], "media": m["url"]}
                    if idx == 0 and caption:
                        entry["caption"] = caption[:1000]
                        entry["parse_mode"] = "HTML"
                    group.append(entry)
                mids = await self.tg.send_media_group(chat_id=self.config.tg_channel_id, media=group)
                message_ids.extend(mids)
                for extra in chunks[1:]:
                    mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=extra)
                    if mid:
                        message_ids.append(mid)
        else:
            for part in chunks:
                mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=part)
                if mid:
                    message_ids.append(mid)

        if poll:
            mid = await self.tg.send_poll(
                chat_id=self.config.tg_channel_id,
                question=poll["question"],
                options=poll["options"],
                is_anonymous=poll.get("is_anonymous", True),
            )
            if mid:
                message_ids.append(mid)

        await self.state.mark_published(post_id, message_ids)
        log.info("Published post %s to channel with %d messages", post_id, len(message_ids))
        if not self.config.dry_run:
            try:
                await self.tg.notify_owner(f"Пост {post_id} опубликован в канал, сообщений: {len(message_ids)}")
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to notify owner about publish: %s", exc)
