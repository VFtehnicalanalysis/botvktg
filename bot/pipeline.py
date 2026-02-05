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
from .site_client import SiteClient

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
    def __init__(
        self,
        config: Config,
        state: StateStore,
        tg: TelegramClient,
        site: Optional[SiteClient] = None,
        vk: Optional["VKClient"] = None,
    ) -> None:
        self.config = config
        self.state = state
        self.tg = tg
        self.site = site
        self.vk = vk
        self.pending_cache: Dict[int, Dict[str, Any]] = {}
        self.pending_news: Dict[str, Dict[str, Any]] = {}
        self.media_group_fail_fallback = True

    async def handle_vk_update(self, update: Dict[str, Any]) -> None:
        if update.get("type") == "wall_post_new":
            post = update.get("object") or update.get("wall_post") or {}
            await self.handle_post(post, source="longpoll")
        elif update.get("type") == "wall_post_edit":
            post = update.get("object") or update.get("wall_post") or {}
            await self.handle_post(post, source="edit")

    async def handle_post(self, post: Dict[str, Any], source: str, force: bool = False) -> None:
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
        already_published = str(status).startswith("published")
        if force:
            stale_ids = self._normalize_message_ids(existing.get("moderation_message_ids"))
            if stale_ids:
                deleted = await self.tg.delete_messages(self.config.owner_id, stale_ids)
                log.info(
                    "Deleted stale %s/%s post moderation messages before re-send for %s",
                    deleted,
                    len(stale_ids),
                    post_id,
                )
                await self.state.clear_moderation_message_ids(post_id)
        if not force and existing_hash == content_hash:
            log.info("Skip post %s (duplicate hash)", post_id)
            return
        if not force and status == "published":
            log.info("Post %s already published; skip edit to avoid duplicate", post_id)
            return
        if not force and await self.state.should_skip(post_id, content_hash):
            log.info("Skip post %s (duplicate hash)", post_id)
            return
        token = str(uuid.uuid4()) if self.config.moderation_required else None
        await self.state.mark_pending(post_id, content_hash, token, payload=normalized)
        self.pending_cache[post_id] = normalized
        if self.config.moderation_required:
            use_extended_actions = not (force and source == "manual-refresh")
            warn_duplicate = force and source == "manual-refresh" and already_published
            await self._send_for_moderation(
                post_id,
                normalized,
                token,
                use_extended_actions=use_extended_actions,
                warn_duplicate=warn_duplicate,
            )
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
        if data.startswith("post:"):
            if user_id != self.config.owner_id:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Нет доступа")
                return
            parts = data.split(":", 2)
            if len(parts) != 3:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Неверная команда")
                return
            _, action, token = parts
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
            valid_actions = {"vk", "tg", "both", "reject"}
            if action not in valid_actions:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Неизвестная команда")
                return
            if action == "reject":
                await self.state.mark_rejected(post_id)
                await self.state.invalidate_token(token)
                await self._delete_post_moderation_messages(post_id)
                await self._notify_post_result(
                    published=False,
                    source_link=payload.get("vk_url", ""),
                    publish_tg=False,
                    tg_message_ids=[],
                    publish_vk=False,
                    vk_link=None,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Отклонено")
                return
            await self.state.mark_approved(post_id)
            await self.state.invalidate_token(token)
            tg_message_ids: List[int] = []
            if action in {"tg", "both"}:
                tg_message_ids = await self._publish(post_id, payload, notify_owner=False)
            publish_vk = action in {"vk", "both"}
            vk_link = payload.get("vk_url", "") if publish_vk else None
            await self._delete_post_moderation_messages(post_id)
            await self._notify_post_result(
                published=True,
                source_link=payload.get("vk_url", ""),
                publish_tg=action in {"tg", "both"},
                tg_message_ids=tg_message_ids,
                publish_vk=publish_vk,
                vk_link=vk_link,
            )
            if cb_id:
                await self.tg.answer_callback_query(cb_id, text="Опубликовано")
        elif data.startswith("approve:") or data.startswith("reject:"):
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
                tg_message_ids = await self._publish(post_id, payload, notify_owner=False)
                await self._delete_post_moderation_messages(post_id)
                await self._notify_post_result(
                    published=True,
                    source_link=payload.get("vk_url", ""),
                    publish_tg=True,
                    tg_message_ids=tg_message_ids,
                    publish_vk=False,
                    vk_link=None,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Опубликовано в TG")
            elif action == "reject":
                await self.state.mark_rejected(post_id)
                await self.state.invalidate_token(token)
                await self._delete_post_moderation_messages(post_id)
                await self._notify_post_result(
                    published=False,
                    source_link=payload.get("vk_url", ""),
                    publish_tg=False,
                    tg_message_ids=[],
                    publish_vk=False,
                    vk_link=None,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Отклонено")
        elif data.startswith("approve_news:") or data.startswith("reject_news:") or data.startswith("news:"):
            if user_id != self.config.owner_id:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Нет доступа")
                return
            action = ""
            token = ""
            if data.startswith("news:"):
                parts = data.split(":", 2)
                if len(parts) == 3:
                    _, action, token = parts
            else:
                action, token = data.split(":", 1)
            url = await self.state.get_news_by_token(token)
            if not url:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Устарело")
                return
            payload = self.pending_news.get(url) or await self.state.get_news_payload(url)
            if not payload:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Нет данных")
                return
            valid_actions = {"approve_news", "reject_news", "reject", "vk", "tg", "both"}
            if action not in valid_actions:
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Неизвестная команда")
                return
            if action in {"reject_news", "reject"}:
                await self.state.mark_news_rejected(url)
                await self.state.invalidate_news_token(token)
                await self._delete_news_moderation_messages(url)
                await self._notify_news_result(
                    published=False,
                    source_link=url,
                    publish_tg=False,
                    tg_message_ids=[],
                    publish_vk=False,
                    vk_post_id=None,
                    is_digest=self._is_digest_payload(payload),
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Отклонено")
                return

            publish_vk = action in {"vk", "both"}
            publish_tg = action in {"tg", "both"} or action == "approve_news"
            await self.state.mark_news_approved(url)
            await self.state.invalidate_news_token(token)
            tg_message_ids, vk_post_id = await self._publish_news(
                url,
                payload,
                publish_vk=publish_vk,
                publish_tg=publish_tg,
                notify_owner=False,
            )
            await self._delete_news_moderation_messages(url)
            await self._notify_news_result(
                published=True,
                source_link=url,
                publish_tg=publish_tg,
                tg_message_ids=tg_message_ids,
                publish_vk=publish_vk,
                vk_post_id=vk_post_id,
                is_digest=self._is_digest_payload(payload),
            )
            if cb_id:
                await self.tg.answer_callback_query(cb_id, text="Опубликовано")

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

    def _is_digest_payload(self, payload: Dict[str, Any]) -> bool:
        if bool(payload.get("is_digest")):
            return True
        url_l = str(payload.get("url") or "").lower()
        title_l = str(payload.get("title") or "").lower()
        text_l = str(payload.get("text") or "").lower()
        if "/digest/" in url_l or "дайджест" in title_l or "digest" in title_l:
            return True
        digest_markers = (
            "юбилейные встречи выпускников",
            "ef msu alumni",
            "alumni@econ.msu.ru",
            "группы для нашего общения",
        )
        return any(marker in text_l for marker in digest_markers)

    def _is_digest_news(self, url: str = "", payload: Optional[Dict[str, Any]] = None) -> bool:
        payload_obj = payload or {}
        if self._is_digest_payload(payload_obj):
            return True
        source_url_l = str(url or "").lower()
        payload_url_l = str(payload_obj.get("url") or "").lower()
        title_l = str(payload_obj.get("title") or "").lower()
        return (
            "/digest/" in source_url_l
            or "/digest/" in payload_url_l
            or "дайджест" in title_l
            or "digest" in title_l
        )

    def _trim_digest_footer_text(self, text: str) -> str:
        if not text:
            return ""
        marker = "юбилейные встречи"
        idx = text.lower().find(marker)
        if idx != -1:
            return text[:idx].rstrip()
        return text.strip()

    def _normalize_message_ids(self, raw_value: Any) -> List[int]:
        if not isinstance(raw_value, list):
            return []
        normalized: List[int] = []
        for item in raw_value:
            try:
                message_id = int(item)
            except (TypeError, ValueError):
                continue
            normalized.append(message_id)
        return normalized

    def _split_photo_caption_and_chunks(
        self,
        chunks: Sequence[str],
        caption_limit: int = 1000,
    ) -> Tuple[Optional[str], List[str]]:
        if not chunks:
            return None, []
        first = chunks[0] or ""
        caption = first[:caption_limit] if first else ""
        tail = first[caption_limit:]
        extras = [part for part in chunks[1:] if part]
        if tail:
            extras.insert(0, tail)
        return (caption or None), extras

    def _format_news_text(
        self, payload: Dict[str, Any], html: bool = True, include_body: bool = True
    ) -> str:
        title = payload.get("title", "") or ""
        date = payload.get("date", "") or ""
        link = payload.get("url", "") or ""
        text_body = payload.get("text", "") or ""
        if html:
            title = escape_html(title)
            date = escape_html(date)
            link = escape_html(link)
            text_body = escape_html(text_body)
        lines: List[str] = []
        if title:
            lines.append(title)
        if date:
            lines.append(date)
        if link:
            lines.append(link)
        full_text = "\n".join(lines)
        if include_body and text_body:
            full_text = f"{full_text}\n\n{text_body}" if full_text else text_body
        return full_text.strip()

    async def refresh_recent_posts(self, vk_client: "VKClient", count: int = 10, force: bool = False) -> None:  # type: ignore
        log.info("Manual refresh of recent posts (count=%s, force=%s)", count, force)
        items = await vk_client.wall_get_recent(count=count)
        for item in reversed(items):
            await self.handle_post(item, source="manual-refresh", force=force)

    async def refresh_latest_news(self, force: bool = False) -> None:
        if not self.site:
            return
        latest = await self.site.fetch_latest_news()
        if not latest:
            return
        await self.handle_news(latest, force=force)

    async def _send_for_moderation(
        self,
        post_id: int,
        payload: Dict[str, Any],
        token: Optional[str],
        use_extended_actions: bool = True,
        warn_duplicate: bool = False,
    ) -> None:
        log.info("Post %s pending moderation", post_id)
        text = escape_html(payload["text"]) if payload["text"] else "(без текста)"
        vk_link = escape_html(payload.get("vk_url", ""))
        header = f"Новый пост #{post_id} из ВК:\n{vk_link}"
        if warn_duplicate:
            header = f"{header}\n⚠️ Найден дубликат, уверены ли вы, что хотите продолжить?"
        full_text = f"{header}\n\n{text}" if text else header
        if use_extended_actions:
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "📢 ВК", "callback_data": f"post:vk:{token}"},
                        {"text": "✈️ TG", "callback_data": f"post:tg:{token}"},
                    ],
                    [
                        {"text": "📢+✈️ ВК+TG", "callback_data": f"post:both:{token}"},
                        {"text": "🚫 Отклонить", "callback_data": f"post:reject:{token}"},
                    ],
                ]
            }
        else:
            keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "✅ Опубликовать в TG", "callback_data": f"approve:{token}"},
                        {"text": "🚫 Отклонить", "callback_data": f"reject:{token}"},
                    ]
                ]
            }
        parts = chunk_text(full_text, limit=1000)
        message_ids: List[int] = []
        first_id = await self.tg.send_message(
            chat_id=self.config.owner_id, text=parts[0], reply_markup=keyboard
        )
        if first_id:
            message_ids.append(first_id)
        for extra in parts[1:]:
            mid = await self.tg.send_message(chat_id=self.config.owner_id, text=extra)
            if mid:
                message_ids.append(mid)
        media: List[Dict[str, Any]] = payload.get("media") or []
        if len(media) == 1:
            m = media[0]
            if m["type"] == "photo":
                mid = await self.tg.send_photo(chat_id=self.config.owner_id, photo=m["url"])
                if mid:
                    message_ids.append(mid)
        elif len(media) > 1:
            group = [{"type": m["type"], "media": m["url"]} for m in media if m.get("url")]
            if group:
                mids = await self.tg.send_media_group(chat_id=self.config.owner_id, media=group)
                message_ids.extend(mids)
        poll = payload.get("poll")
        if poll:
            mid = await self.tg.send_poll(
                chat_id=self.config.owner_id,
                question=poll["question"],
                options=poll["options"],
                is_anonymous=poll.get("is_anonymous", True),
            )
            if mid:
                message_ids.append(mid)
        await self.state.set_moderation_message_ids(post_id, message_ids)

    async def _publish(
        self,
        post_id: int,
        payload: Dict[str, Any],
        notify_owner: bool = True,
    ) -> List[int]:
        text = payload.get("text", "") or ""
        media: List[Dict[str, Any]] = payload.get("media") or []
        poll = payload.get("poll")
        message_ids: List[int] = []

        escaped_text = escape_html(text)
        chunks = chunk_text(escaped_text, limit=3500)

        if media:
            if len(media) == 1:
                m = media[0]
                cap, extra_chunks = self._split_photo_caption_and_chunks(chunks, caption_limit=1000)
                msg_id = await self.tg.send_photo(
                    chat_id=self.config.tg_channel_id, photo=m["url"], caption=cap
                )
                if msg_id:
                    message_ids.append(msg_id)
                for extra in extra_chunks:
                    mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=extra)
                    if mid:
                        message_ids.append(mid)
            else:
                caption, extra_chunks = self._split_photo_caption_and_chunks(chunks, caption_limit=1000)
                group: List[Dict[str, Any]] = []
                for idx, m in enumerate(media):
                    entry = {"type": m["type"], "media": m["url"]}
                    if idx == 0 and caption:
                        entry["caption"] = caption
                        entry["parse_mode"] = "HTML"
                    group.append(entry)
                mids = await self._send_media_group_safe(self.config.tg_channel_id, group)
                message_ids.extend(mids)
                for extra in extra_chunks:
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
        if notify_owner and not self.config.dry_run:
            try:
                lines = [f"Пост {post_id} опубликован в канал, сообщений: {len(message_ids)}."]
                tg_link = self._build_tg_message_link(message_ids)
                if tg_link:
                    lines.append(f"Ссылка TG: {tg_link}")
                vk_source = payload.get("vk_url", "") or ""
                if vk_source:
                    lines.append(f"Исходный пост ВК: {vk_source}")
                await self.tg.notify_owner("\n".join(lines))
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to notify owner about publish: %s", exc)
        return message_ids

    async def handle_news(self, news: Dict[str, Any], force: bool = False) -> None:
        url = news["url"]
        existing = await self.state.get_news_record(url)
        if force and existing:
            stale_ids = self._normalize_message_ids(existing.get("moderation_message_ids"))
            if stale_ids:
                deleted = await self.tg.delete_messages(self.config.owner_id, stale_ids)
                log.info(
                    "Deleted stale %s/%s news moderation messages before re-send for %s",
                    deleted,
                    len(stale_ids),
                    url,
                )
                await self.state.clear_news_moderation_message_ids(url)
        if existing and not force:
            status = existing.get("status")
            if status in {"approved", "pending", "rejected"} or str(status).startswith("published"):
                log.info("Skip news (status=%s): %s", status, url)
                return
        detail = (
            await self.site.fetch_news_detail(url, title=news.get("title"))
            if self.site
            else {"text": "", "images": []}
        )
        canonical_url = str(detail.get("canonical_url") or url)
        title = news.get("title") or detail.get("title", "")
        date = news.get("date") or detail.get("date", "")
        payload = {
            "url": canonical_url,
            "title": title or "",
            "date": date or "",
            "text": detail.get("text", ""),
            "images": detail.get("images", []),
        }
        is_digest = (
            bool(news.get("is_digest"))
            or bool(detail.get("is_digest"))
            or "/digest/" in canonical_url.lower()
            or self._is_digest_payload(payload)
        )
        if is_digest:
            payload["text"] = self._trim_digest_footer_text(str(payload.get("text") or ""))
            payload["images"] = payload.get("images", [])[:1]
            payload["is_digest"] = True
        else:
            payload["is_digest"] = False
        content_hash = self._hash_payload(payload)
        if not force and await self.state.news_should_skip(url, content_hash):
            log.info("Skip news (hash): %s", url)
            return
        token = str(uuid.uuid4()) if self.config.moderation_required else None
        await self.state.mark_news_pending(url, content_hash, token, payload=payload)
        self.pending_news[url] = payload
        if self.config.moderation_required:
            await self._send_news_for_moderation(url, payload, token)
        else:
            await self._publish_news(url, payload)

    async def _send_news_for_moderation(self, url: str, payload: Dict[str, Any], token: Optional[str]) -> None:
        text_body = escape_html(payload.get("text", "")) or "(без текста)"
        is_digest = self._is_digest_news(url=url, payload=payload)
        log.info(
            "News moderation type digest=%s source_url=%s payload_url=%s payload_is_digest=%s",
            is_digest,
            url,
            payload.get("url", ""),
            payload.get("is_digest"),
        )
        header_prefix = "Новый дайджест на сайте" if is_digest else "Новая новость на сайте"
        header = f"{header_prefix}:\n{self._format_news_text(payload, html=True, include_body=False)}"
        chunks = chunk_text(f"{header}\n\n{text_body}", limit=1000)
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "📢 ВК", "callback_data": f"news:vk:{token}"},
                    {"text": "✈️ TG", "callback_data": f"news:tg:{token}"},
                ],
                [
                    {"text": "📢+✈️ ВК+TG", "callback_data": f"news:both:{token}"},
                    {"text": "🚫 Отклонить", "callback_data": f"news:reject:{token}"},
                ],
            ]
        }
        message_ids: List[int] = []
        first_id = await self.tg.send_message(
            chat_id=self.config.owner_id, text=chunks[0], reply_markup=keyboard
        )
        if first_id:
            message_ids.append(first_id)
        for extra in chunks[1:]:
            mid = await self.tg.send_message(chat_id=self.config.owner_id, text=extra)
            if mid:
                message_ids.append(mid)
        all_images: List[str] = payload.get("images", []) or []
        images: List[str] = all_images[:1] if is_digest else all_images[:10]
        if len(images) == 1:
            mid = await self.tg.send_photo(chat_id=self.config.owner_id, photo=images[0])
            if mid:
                message_ids.append(mid)
        elif len(images) > 1:
            media = [{"type": "photo", "media": img} for img in images]
            mids = await self._send_media_group_safe(self.config.owner_id, media)
            message_ids.extend(mids)
        await self.state.set_news_moderation_message_ids(url, message_ids)

    async def _publish_news(
        self,
        url: str,
        payload: Dict[str, Any],
        publish_vk: bool = False,
        publish_tg: bool = True,
        notify_owner: bool = True,
    ) -> Tuple[List[int], Optional[int]]:
        message_ids: Optional[List[int]] = None
        vk_post_id: Optional[int] = None
        if publish_tg:
            message_ids = await self._publish_news_tg(payload)
            log.info("Published news to Telegram: %s", url)
        if publish_vk:
            vk_post_id = await self._publish_news_vk(payload)
            log.info("Published news to VK: %s", url)
        published_to = "tg" if publish_tg else None
        if publish_vk and publish_tg:
            published_to = "both"
        elif publish_vk:
            published_to = "vk"
        await self.state.mark_news_published(
            url,
            tg_message_ids=message_ids,
            vk_post_id=vk_post_id,
            published_to=published_to,
        )
        if notify_owner and not self.config.dry_run:
            try:
                entity_name = "Дайджест" if self._is_digest_news(url=url, payload=payload) else "Новость"
                await self.tg.notify_owner(f"{entity_name} опубликован: {url} ({published_to})")
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to notify owner about news publish: %s", exc)
        return message_ids or [], vk_post_id

    async def _publish_news_tg(self, payload: Dict[str, Any]) -> List[int]:
        full_text = self._format_news_text(payload, html=True)
        message_ids: List[int] = []
        chunks = chunk_text(full_text, limit=3500)
        all_images: List[str] = payload.get("images", []) or []
        images: List[str] = all_images[:1] if self._is_digest_news(payload=payload) else all_images[:10]

        if images:
            if len(images) == 1:
                caption, extra_chunks = self._split_photo_caption_and_chunks(chunks, caption_limit=1000)
                mid = await self.tg.send_photo(
                    chat_id=self.config.tg_channel_id,
                    photo=images[0],
                    caption=caption,
                )
                if mid:
                    message_ids.append(mid)
                for extra in extra_chunks:
                    mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=extra)
                    if mid:
                        message_ids.append(mid)
            else:
                caption, extra_chunks = self._split_photo_caption_and_chunks(chunks, caption_limit=1000)
                group = []
                for idx, img in enumerate(images):
                    entry = {"type": "photo", "media": img}
                    if idx == 0 and caption:
                        entry["caption"] = caption
                        entry["parse_mode"] = "HTML"
                    group.append(entry)
                mids = await self._send_media_group_safe(self.config.tg_channel_id, group)
                message_ids.extend(mids)
                for extra in extra_chunks:
                    mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=extra)
                    if mid:
                        message_ids.append(mid)
        else:
            for part in chunks:
                mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=part)
                if mid:
                    message_ids.append(mid)
        return message_ids

    async def _publish_news_vk(self, payload: Dict[str, Any]) -> Optional[int]:
        if not self.vk:
            raise RuntimeError("VK client is not configured")
        text = self._format_news_text(payload, html=False)
        if len(text) > 6000:
            text = text[:6000] + "…"
        attachments: List[str] = []
        all_images: List[str] = payload.get("images", []) or []
        is_digest = self._is_digest_news(payload=payload)
        images: List[str] = all_images[:1] if is_digest else all_images[:10]
        if images and not self.config.dry_run:
            max_images = 1 if is_digest else 10
            attachments = await self.vk.upload_wall_photos(images, max_images=max_images)
        if self.config.dry_run:
            log.info("[dry-run] VK wall.post: %s", text[:200])
            return None
        return await self.vk.wall_post(message=text, attachments=attachments)

    def _build_tg_message_link(self, message_ids: Sequence[int]) -> Optional[str]:
        if not message_ids:
            return None
        message_id = message_ids[0]
        chat = str(self.config.tg_channel_id).strip()
        if chat.startswith("https://t.me/"):
            return f"{chat.rstrip('/')}/{message_id}"
        if chat.startswith("@"):
            username = chat[1:]
            return f"https://t.me/{username}/{message_id}" if username else None
        if chat.startswith("-100") and chat[4:].isdigit():
            return f"https://t.me/c/{chat[4:]}/{message_id}"
        if chat and not chat.startswith("-"):
            return f"https://t.me/{chat}/{message_id}"
        return None

    def _build_vk_post_link(self, vk_post_id: Optional[int]) -> Optional[str]:
        if not vk_post_id:
            return None
        return f"https://vk.com/wall-{abs(self.config.vk_group_id)}_{vk_post_id}"

    async def _notify_post_result(
        self,
        published: bool,
        source_link: str,
        publish_tg: bool,
        tg_message_ids: Sequence[int],
        publish_vk: bool,
        vk_link: Optional[str],
    ) -> None:
        tg_link = self._build_tg_message_link(tg_message_ids) if publish_tg else None
        lines = ["Публикация поста отклонена."]
        if published:
            lines[0] = "Пост опубликован."
            published_links: List[str] = []
            if tg_link:
                published_links.append(f"TG: {tg_link}")
            if publish_vk and vk_link:
                published_links.append(f"ВК: {vk_link}")
            if published_links:
                lines.append(f"Публикация: {', '.join(published_links)}")
        lines.append(f"Исходный пост ВК: {source_link}")
        try:
            await self.tg.notify_owner("\n".join(lines))
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to notify owner about post moderation result: %s", exc)

    async def _notify_news_result(
        self,
        published: bool,
        source_link: str,
        publish_tg: bool,
        tg_message_ids: Sequence[int],
        publish_vk: bool,
        vk_post_id: Optional[int],
        is_digest: bool = False,
    ) -> None:
        tg_link = self._build_tg_message_link(tg_message_ids) if publish_tg else None
        vk_link = self._build_vk_post_link(vk_post_id) if publish_vk else None
        lines = ["Публикация новости отклонена."]
        if is_digest:
            lines = ["Публикация дайджеста отклонена."]
        if published:
            lines[0] = "Новость опубликована."
            if is_digest:
                lines[0] = "Дайджест опубликован."
            published_links: List[str] = []
            if tg_link:
                published_links.append(f"TG: {tg_link}")
            if vk_link:
                published_links.append(f"ВК: {vk_link}")
            if published_links:
                lines.append(f"Публикация: {', '.join(published_links)}")
        lines.append(f"Исходная новость: {source_link}")
        try:
            await self.tg.notify_owner("\n".join(lines))
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to notify owner about news moderation result: %s", exc)

    async def _delete_post_moderation_messages(self, post_id: int) -> None:
        message_ids = await self.state.get_moderation_message_ids(post_id)
        if not message_ids:
            return
        deleted = await self.tg.delete_messages(self.config.owner_id, message_ids)
        log.info("Deleted %s/%s post moderation messages for %s", deleted, len(message_ids), post_id)
        await self.state.clear_moderation_message_ids(post_id)

    async def _delete_news_moderation_messages(self, url: str) -> None:
        message_ids = await self.state.get_news_moderation_message_ids(url)
        if not message_ids:
            return
        deleted = await self.tg.delete_messages(self.config.owner_id, message_ids)
        log.info("Deleted %s/%s news moderation messages for %s", deleted, len(message_ids), url)
        await self.state.clear_news_moderation_message_ids(url)

    async def _send_media_group_safe(self, chat_id: int | str, group: List[Dict[str, Any]]) -> List[int]:
        if not group:
            return []
        mids = await self.tg.send_media_group(chat_id=chat_id, media=group)
        if len(mids) == len(group):
            return mids
        log.warning("Media group failed or partial (sent %s of %s), falling back to single sends", len(mids), len(group))
        fallback_ids: List[int] = []
        for item in group:
            caption = item.get("caption")
            parse_mode = item.get("parse_mode", "HTML")
            mid = await self.tg.send_photo(chat_id=chat_id, photo=item["media"], caption=caption, parse_mode=parse_mode)
            if mid:
                fallback_ids.append(mid)
        return fallback_ids
