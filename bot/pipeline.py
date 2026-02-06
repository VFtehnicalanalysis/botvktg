from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
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


def _split_block_by_words(text: str, limit: int) -> List[str]:
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    tokens = re.findall(r"\S+|\s+", text)
    parts: List[str] = []
    current = ""
    for token in tokens:
        if len(token) > limit:
            if current.strip():
                parts.append(current.rstrip())
                current = ""
            start = 0
            while start < len(token):
                piece = token[start : start + limit]
                if len(piece) == limit:
                    parts.append(piece.rstrip())
                else:
                    current = piece
                start += limit
            continue
        if len(current) + len(token) <= limit:
            current += token
            continue
        if current.strip():
            parts.append(current.rstrip())
        current = token.lstrip() if token.isspace() else token
    if current.strip():
        parts.append(current.rstrip())
    return parts


def chunk_text(text: str, limit: int = 3500) -> List[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return [""]
    if len(cleaned) <= limit:
        return [cleaned]
    paragraphs = [p.strip() for p in cleaned.split("\n\n") if p.strip()]
    if not paragraphs:
        return _split_block_by_words(cleaned, limit)
    chunks: List[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(paragraph) <= limit:
            current = paragraph
            continue
        paragraph_parts = _split_block_by_words(paragraph, limit)
        if not paragraph_parts:
            continue
        chunks.extend(paragraph_parts[:-1])
        current = paragraph_parts[-1]
    if current:
        chunks.append(current)
    return chunks or [cleaned]


def _split_block_with_more_markers(text: str, limit: int) -> List[str]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return []
    atoms: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.fullmatch(r"\[\[MORE:[^\]]+\]\]", line):
            if atoms:
                atoms[-1] = f"{atoms[-1]}\n{line}"
            else:
                atoms.append(line)
            i += 1
            continue
        atom = line
        i += 1
        while i < len(lines) and re.fullmatch(r"\[\[MORE:[^\]]+\]\]", lines[i]):
            atom = f"{atom}\n{lines[i]}"
            i += 1
        atoms.append(atom)

    parts: List[str] = []
    current = ""
    for atom in atoms:
        candidate = atom if not current else f"{current}\n{atom}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            parts.append(current)
            current = ""
        if len(atom) <= limit:
            current = atom
            continue
        split_atom = _split_block_by_words(atom, limit)
        if not split_atom:
            continue
        parts.extend(split_atom[:-1])
        current = split_atom[-1]
    if current:
        parts.append(current)
    return parts


def chunk_text_preserving_more_markers(text: str, limit: int = 3500) -> List[str]:
    cleaned = (text or "").strip()
    if not cleaned:
        return [""]
    if "[[MORE:" not in cleaned:
        return chunk_text(cleaned, limit=limit)

    pair_re = re.compile(r"(?s)(.*?)(\[\[MORE:[^\]]+\]\])")
    blocks: List[str] = []
    pos = 0
    for match in pair_re.finditer(cleaned):
        before = (match.group(1) or "").strip()
        marker = (match.group(2) or "").strip()
        if before:
            blocks.append(f"{before}\n{marker}")
        elif marker:
            if blocks:
                blocks[-1] = f"{blocks[-1]}\n{marker}"
            else:
                blocks.append(marker)
        pos = match.end()
    tail = cleaned[pos:].strip()
    if tail:
        blocks.append(tail)
    if not blocks:
        return chunk_text(cleaned, limit=limit)

    chunks: List[str] = []
    current = ""
    for block in blocks:
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(block) <= limit:
            current = block
            continue
        split_block = (
            _split_block_with_more_markers(block, limit)
            if "[[MORE:" in block
            else _split_block_by_words(block, limit)
        )
        if not split_block:
            continue
        chunks.extend(split_block[:-1])
        current = split_block[-1]
    if current:
        chunks.append(current)
    return chunks or [cleaned]


def chunk_news_text(text: str, limit: int = 3500) -> List[str]:
    """
    Единый чанкер для новостей (events/articles/digest):
    - минимизирует число сообщений за счет плотной упаковки до limit;
    - не режет слова, кроме технически неизбежного случая очень длинного токена.
    """
    return chunk_text_preserving_more_markers(text, limit=limit)


def truncate_text_without_word_cut(text: str, limit: int) -> str:
    cleaned = (text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    parts = _split_block_by_words(cleaned, limit)
    if not parts:
        return cleaned[:limit].rstrip() + "…"
    return parts[0].rstrip() + "…"


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
            await self._delete_post_moderation_messages(post_id)
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
        actor = self._format_telegram_actor(from_user)
        actor_for_owner = actor if not self.config.is_owner(user_id) else None
        if data.startswith("post:"):
            if not self.config.is_moderator(user_id):
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
                    actor=actor_for_owner,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Отклонено")
                return
            try:
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
                    actor=actor_for_owner,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Опубликовано")
            except Exception as exc:  # noqa: BLE001
                log.exception("Post publish failed: %s", exc)
                try:
                    await self._delete_post_moderation_messages(post_id)
                except Exception as del_exc:  # noqa: BLE001
                    log.warning("Failed to delete post moderation messages after error: %s", del_exc)
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Ошибка публикации")
                await self._notify_publish_error(
                    f"Ошибка публикации поста: {exc}",
                    actor=actor_for_owner,
                )
                return
        elif data.startswith("approve:") or data.startswith("reject:"):
            if not self.config.is_moderator(user_id):
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
                try:
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
                        actor=actor_for_owner,
                    )
                    if cb_id:
                        await self.tg.answer_callback_query(cb_id, text="Опубликовано в TG")
                except Exception as exc:  # noqa: BLE001
                    log.exception("Post publish (TG) failed: %s", exc)
                    try:
                        await self._delete_post_moderation_messages(post_id)
                    except Exception as del_exc:  # noqa: BLE001
                        log.warning("Failed to delete post moderation messages after error: %s", del_exc)
                    if cb_id:
                        await self.tg.answer_callback_query(cb_id, text="Ошибка публикации")
                    await self._notify_publish_error(
                        f"Ошибка публикации поста: {exc}",
                        actor=actor_for_owner,
                    )
                    return
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
                    actor=actor_for_owner,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Отклонено")
        elif data.startswith("approve_news:") or data.startswith("reject_news:") or data.startswith("news:"):
            if not self.config.is_moderator(user_id):
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
                await self._delete_news_moderation_messages(url, payload=payload)
                await self._notify_news_result(
                    published=False,
                    source_link=url,
                    publish_tg=False,
                    tg_message_ids=[],
                    publish_vk=False,
                    vk_post_id=None,
                    is_digest=self._is_digest_payload(payload),
                    actor=actor_for_owner,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Отклонено")
                return

            publish_vk = action in {"vk", "both"}
            publish_tg = action in {"tg", "both"} or action == "approve_news"
            try:
                await self.state.mark_news_approved(url)
                await self.state.invalidate_news_token(token)
                tg_message_ids, vk_post_id = await self._publish_news(
                    url,
                    payload,
                    publish_vk=publish_vk,
                    publish_tg=publish_tg,
                    notify_owner=False,
                )
                await self._delete_news_moderation_messages(url, payload=payload)
                await self._notify_news_result(
                    published=True,
                    source_link=url,
                    publish_tg=publish_tg,
                    tg_message_ids=tg_message_ids,
                    publish_vk=publish_vk,
                    vk_post_id=vk_post_id,
                    is_digest=self._is_digest_payload(payload),
                    actor=actor_for_owner,
                )
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Опубликовано")
            except Exception as exc:  # noqa: BLE001
                log.exception("News publish failed: %s", exc)
                try:
                    await self._delete_news_moderation_messages(url, payload=payload)
                except Exception as del_exc:  # noqa: BLE001
                    log.warning("Failed to delete news moderation messages after error: %s", del_exc)
                if cb_id:
                    await self.tg.answer_callback_query(cb_id, text="Ошибка публикации")
                await self._notify_publish_error(
                    f"Ошибка публикации новости: {exc}",
                    actor=actor_for_owner,
                )
                return

    def _normalize_post(self, post: Dict[str, Any]) -> Dict[str, Any]:
        text = post.get("text", "") or ""
        owner_id = post.get("owner_id") or 0
        post_id = post.get("id") or post.get("post_id") or 0
        vk_url = f"https://vk.com/wall{owner_id}_{post_id}"
        parsed_media: List[Dict[str, Any]] = []
        extra_lines: List[str] = []
        poll_obj: Optional[Dict[str, Any]] = None

        def _extract_best_photo_url(photo_obj: Any) -> Optional[str]:
            if not isinstance(photo_obj, dict):
                return None
            best_url: Optional[str] = None
            best_score = -1

            sizes = photo_obj.get("sizes") or []
            if isinstance(sizes, list):
                size_type_rank = {
                    "w": 12,
                    "z": 11,
                    "y": 10,
                    "x": 9,
                    "r": 8,
                    "q": 7,
                    "p": 6,
                    "o": 5,
                    "m": 4,
                    "s": 3,
                }
                for size in sizes:
                    if not isinstance(size, dict):
                        continue
                    url = str(size.get("url") or size.get("src") or "").strip()
                    if not url:
                        continue
                    width = int(size.get("width") or 0)
                    height = int(size.get("height") or 0)
                    score = width * height
                    if score <= 0:
                        score = size_type_rank.get(str(size.get("type") or "").lower(), 0)
                    if score > best_score:
                        best_score = score
                        best_url = url

            orig_photo = photo_obj.get("orig_photo")
            if isinstance(orig_photo, dict):
                url = str(orig_photo.get("url") or "").strip()
                if url:
                    width = int(orig_photo.get("width") or 0)
                    height = int(orig_photo.get("height") or 0)
                    score = width * height if width and height else 10_000_000
                    if score > best_score:
                        best_score = score
                        best_url = url

            flat_key_rank = {
                "src_xxbig": 9_500_000,
                "src_xbig": 9_400_000,
                "src_big": 9_300_000,
                "src": 9_200_000,
                "url": 9_100_000,
            }
            for key, score in flat_key_rank.items():
                url = str(photo_obj.get(key) or "").strip()
                if url and score > best_score:
                    best_score = score
                    best_url = url

            for key, value in photo_obj.items():
                if not isinstance(value, str):
                    continue
                url = value.strip()
                if not url:
                    continue
                match = re.fullmatch(r"photo_(\d+)", str(key))
                if not match:
                    continue
                score = int(match.group(1))
                if score > best_score:
                    best_score = score
                    best_url = url

            return best_url

        def _append_photo(photo_obj: Any) -> None:
            best_url = _extract_best_photo_url(photo_obj)
            if not best_url:
                return
            if any(item.get("url") == best_url for item in parsed_media):
                return
            parsed_media.append({"type": "photo", "url": best_url})

        def _parse_attachments(raw_attachments: Any) -> None:
            nonlocal poll_obj
            for att in (raw_attachments or []):
                if not isinstance(att, dict):
                    continue
                att_type = att.get("type")
                if att_type == "photo":
                    _append_photo(att.get("photo") or {})
                    continue
                if att_type == "video":
                    video = att.get("video") or {}
                    if video.get("owner_id") is not None and video.get("id") is not None:
                        video_link = f"https://vk.com/video{video.get('owner_id')}_{video.get('id')}"
                        extra_lines.append(f"Видео: {video_link}")
                    continue
                if att_type == "link":
                    link_obj = att.get("link") or {}
                    _append_photo(link_obj.get("photo") or {})
                    link_url = link_obj.get("url")
                    if link_url:
                        extra_lines.append(f"Ссылка: {link_url}")
                    continue
                if att_type == "posted_photo":
                    _append_photo(att.get("posted_photo") or att)
                    continue
                if att_type == "doc":
                    doc = att.get("doc") or {}
                    preview_photo = ((doc.get("preview") or {}).get("photo") or {})
                    _append_photo(preview_photo)
                    doc_url = doc.get("url")
                    if doc_url:
                        extra_lines.append(f"Документ: {doc_url}")
                    continue
                if att_type == "poll":
                    poll = att.get("poll") or {}
                    poll_obj = {
                        "question": poll.get("question", "")[:255],
                        "options": [ans.get("text", "")[:255] for ans in poll.get("answers", [])],
                        "is_anonymous": poll.get("anonymous", True),
                    }
                    continue
                link = att.get(att_type, {}).get("url") if att_type else None
                title = att.get(att_type, {}).get("title") if att_type else None
                if link:
                    extra_lines.append(f"{title or att_type}: {link}")

        _parse_attachments(post.get("attachments", []) or [])

        for idx, src in enumerate(post.get("copy_history", []) or []):
            if not isinstance(src, dict):
                continue
            src_text = src.get("text") or ""
            if src_text:
                prefix = "[Перепост]:" if idx == 0 else f"[Перепост #{idx + 1}]:"
                text = f"{text}\n\n{prefix}\n{src_text}" if text else f"{prefix}\n{src_text}"
            _parse_attachments(src.get("attachments", []) or [])

        if extra_lines:
            # Дедуплицируем повторы ссылок/видео, которые могут приходить одновременно из post и copy_history
            seen_lines = set()
            deduped_lines: List[str] = []
            for line in extra_lines:
                if line in seen_lines:
                    continue
                seen_lines.add(line)
                deduped_lines.append(line)
            extra_text = "\n".join(deduped_lines)
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

    def _is_events_news(self, url: str = "", payload: Optional[Dict[str, Any]] = None) -> bool:
        payload_obj = payload or {}
        if bool(payload_obj.get("is_event")):
            return True
        source_url_l = str(url or "").lower()
        payload_url_l = str(payload_obj.get("url") or "").lower()
        return (
            "/events." in source_url_l
            or "/events/" in source_url_l
            or "/events." in payload_url_l
            or "/events/" in payload_url_l
        )

    def _bold_event_fields_html(self, text: str) -> str:
        if not text:
            return ""
        label = r"(целевая аудитория|начало|окончание|спикеры)"
        rendered = re.sub(
            rf"(?im)^(\s*){label}(\s*:?\s*)$",
            r"\1<b>\2</b>\3",
            text,
        )
        rendered = re.sub(
            rf"(?im)^(\s*){label}(\s*[:\-]\s*)(?=\S)",
            r"\1<b>\2</b>\3",
            rendered,
        )
        rendered = re.sub(
            rf"(?im)^(\s*){label}(\s+)(?=\S)",
            r"\1<b>\2</b>\3",
            rendered,
        )
        return rendered

    def _trim_digest_footer_text(self, text: str) -> str:
        if not text:
            return ""
        marker = "юбилейные встречи"
        idx = text.lower().find(marker)
        if idx != -1:
            return text[:idx].rstrip()
        return text.strip()

    def _sanitize_news_date(self, date: str) -> str:
        cleaned = (date or "").replace("\xa0", " ").strip()
        if re.fullmatch(r"0\s*auto;?", cleaned.lower()):
            return ""
        return cleaned

    def _news_url_variants(self, url: str) -> List[str]:
        base = (url or "").strip()
        if not base:
            return []
        variants = {base}
        if base.endswith("/"):
            variants.add(base.rstrip("/"))
        else:
            variants.add(f"{base}/")
        return [v for v in variants if v]

    def _moderation_target_ids(self) -> Tuple[int, ...]:
        targets = self.config.all_moderator_ids
        if targets:
            return targets
        if self.config.owner_id:
            return (self.config.owner_id,)
        return ()

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

    def _resolve_moderation_chat_id(self, raw_chat_id: Any) -> Optional[int]:
        try:
            return int(raw_chat_id)
        except (TypeError, ValueError):
            return self.config.owner_id or None

    def _format_telegram_actor(self, user: Dict[str, Any]) -> str:
        user_id = user.get("id")
        username = str(user.get("username") or "").strip()
        first_name = str(user.get("first_name") or "").strip()
        last_name = str(user.get("last_name") or "").strip()
        if username:
            label = f"@{username}"
        else:
            full_name = " ".join(part for part in [first_name, last_name] if part).strip()
            label = full_name or "неизвестный пользователь"
        if user_id:
            return f"{label} (id={user_id})"
        return label

    def _split_photo_caption_and_chunks(
        self,
        chunks: Sequence[str],
        caption_limit: int = 1000,
        body_limit: int = 3500,
        preserve_more_markers: bool = False,
        fill_caption_from_body: bool = False,
    ) -> Tuple[Optional[str], List[str]]:
        if not chunks:
            return None, []
        first = chunks[0] or ""
        if not first:
            extras = [part for part in chunks[1:] if part]
            return None, extras

        chunker = chunk_text_preserving_more_markers if preserve_more_markers else chunk_text
        caption_parts = chunker(first, limit=caption_limit)
        if not caption_parts:
            caption_parts = _split_block_by_words(first, caption_limit) or [first[:caption_limit]]
        caption = caption_parts[0]

        if first.startswith(caption):
            tail = first[len(caption) :].lstrip()
        else:
            tail = ""
            if len(caption_parts) > 1:
                tail = "\n\n".join(part for part in caption_parts[1:] if part).strip()

        remainder_blocks: List[str] = []
        if tail:
            remainder_blocks.append(tail)
        remainder_blocks.extend(part for part in chunks[1:] if part)

        if (
            fill_caption_from_body
            and caption
            and len(caption) < caption_limit
            and remainder_blocks
            and not (preserve_more_markers and "[[MORE:" in remainder_blocks[0])
        ):
            sep = "\n\n" if caption else ""
            available = caption_limit - len(caption) - len(sep)
            if available > 40:
                first_block = remainder_blocks[0]
                donor_parts = _split_block_by_words(first_block, available)
                donor = donor_parts[0].strip() if donor_parts else ""
                if donor:
                    caption = f"{caption}{sep}{donor}" if caption else donor
                    if first_block.startswith(donor):
                        first_rest = first_block[len(donor) :].lstrip()
                    else:
                        first_rest = ""
                    if first_rest:
                        remainder_blocks[0] = first_rest
                    else:
                        remainder_blocks = remainder_blocks[1:]

        remainder_text = "\n\n".join(block for block in remainder_blocks if block.strip())
        extras = chunker(remainder_text, limit=body_limit) if remainder_text else []
        return (caption or None), extras

    def _render_digest_more_links(self, text: str, html: bool) -> str:
        if not text:
            return ""

        def repl(match: re.Match[str]) -> str:
            url = (match.group(1) or "").strip()
            if not url:
                return ""
            if html:
                return f'<a href="{url}">&gt;&gt;</a>'
            return f">> {url}"

        rendered = re.sub(r"\[\[MORE:([^\]]+)\]\]", repl, text)
        rendered = re.sub(r"[ \t]+\n", "\n", rendered)
        rendered = re.sub(r"\n{3,}", "\n\n", rendered)
        return rendered.strip()

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
            if self._is_events_news(payload=payload):
                text_body = self._bold_event_fields_html(text_body)
        else:
            text_body = self._render_digest_more_links(text_body, html=False)
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

    async def refresh_recent_posts(self, vk_client: "VKClient", count: int = 10, force: bool = False) -> int:  # type: ignore
        log.info("Manual refresh of recent posts (count=%s, force=%s)", count, force)
        items = await vk_client.wall_get_recent(count=count)
        if not items:
            log.warning("No VK posts returned for refresh (count=%s, force=%s)", count, force)
            if force and count == 1:
                latest_entry = await self.state.get_latest_post_entry()
                if latest_entry:
                    post_id, payload, status = latest_entry
                    await self._delete_post_moderation_messages(post_id)
                    content_hash = self._hash_payload(payload)
                    token = str(uuid.uuid4()) if self.config.moderation_required else None
                    await self.state.mark_pending(post_id, content_hash, token, payload=payload)
                    self.pending_cache[post_id] = payload
                    if self.config.moderation_required:
                        await self._send_for_moderation(
                            post_id,
                            payload,
                            token,
                            use_extended_actions=False,
                            warn_duplicate=str(status).startswith("published"),
                        )
                    else:
                        await self._publish(post_id, payload)
                    log.info("Sent latest cached VK post for moderation: %s", post_id)
                    return 1
            return 0
        for item in reversed(items):
            await self.handle_post(item, source="manual-refresh", force=force)
        return len(items)

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
        media: List[Dict[str, Any]] = payload.get("media") or []
        poll = payload.get("poll")
        message_ids_by_chat: Dict[int, List[int]] = {}
        for moderator_id in self._moderation_target_ids():
            message_ids: List[int] = []
            try:
                first_id = await self.tg.send_message(
                    chat_id=moderator_id, text=parts[0], reply_markup=keyboard
                )
                if first_id:
                    message_ids.append(first_id)
                for extra in parts[1:]:
                    mid = await self.tg.send_message(chat_id=moderator_id, text=extra)
                    if mid:
                        message_ids.append(mid)
                if len(media) == 1:
                    m = media[0]
                    if m["type"] == "photo":
                        mid = await self.tg.send_photo(chat_id=moderator_id, photo=m["url"])
                        if mid:
                            message_ids.append(mid)
                elif len(media) > 1:
                    group = [{"type": m["type"], "media": m["url"]} for m in media if m.get("url")]
                    if group:
                        mids = await self.tg.send_media_group(chat_id=moderator_id, media=group)
                        message_ids.extend(mids)
                if poll:
                    mid = await self.tg.send_poll(
                        chat_id=moderator_id,
                        question=poll["question"],
                        options=poll["options"],
                        is_anonymous=poll.get("is_anonymous", True),
                    )
                    if mid:
                        message_ids.append(mid)
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to send post moderation preview to %s: %s", moderator_id, exc)
                continue
            if message_ids:
                message_ids_by_chat[moderator_id] = message_ids
        await self.state.set_moderation_message_ids(post_id, message_ids_by_chat)

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
                cap, extra_chunks = self._split_photo_caption_and_chunks(
                    chunks,
                    caption_limit=1000,
                    body_limit=3500,
                    preserve_more_markers=False,
                )
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
                caption, extra_chunks = self._split_photo_caption_and_chunks(
                    chunks,
                    caption_limit=1000,
                    body_limit=3500,
                    preserve_more_markers=False,
                )
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

        if not message_ids:
            raise RuntimeError(
                "Telegram publish failed: no messages sent. "
                "Check TG_CHANNEL_ID and bot rights in target chat/channel."
            )

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
            await self._delete_news_moderation_messages(url, payload=existing.get("payload"))
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
        date = self._sanitize_news_date(str(date or ""))
        payload = {
            "url": canonical_url,
            "title": title or "",
            "date": date,
            "text": detail.get("text", ""),
            "images": detail.get("images", []),
            "is_event": bool(detail.get("is_event"))
            or "/events." in canonical_url.lower()
            or "/events/" in canonical_url.lower(),
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
        if self._is_events_news(url=url, payload=payload):
            text_body = self._bold_event_fields_html(text_body)
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
        chunks = chunk_news_text(f"{header}\n\n{text_body}", limit=3500)
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
        first_text = self._render_digest_more_links(chunks[0], html=True)
        all_images: List[str] = payload.get("images", []) or []
        images: List[str] = all_images[:1] if is_digest else all_images[:10]
        message_ids_by_chat: Dict[int, List[int]] = {}
        for moderator_id in self._moderation_target_ids():
            message_ids: List[int] = []
            try:
                first_id = await self.tg.send_message(
                    chat_id=moderator_id, text=first_text, reply_markup=keyboard
                )
                if first_id:
                    message_ids.append(first_id)
                for extra in chunks[1:]:
                    extra_text = self._render_digest_more_links(extra, html=True)
                    mid = await self.tg.send_message(chat_id=moderator_id, text=extra_text)
                    if mid:
                        message_ids.append(mid)
                if len(images) == 1:
                    mid = await self.tg.send_photo(chat_id=moderator_id, photo=images[0])
                    if mid:
                        message_ids.append(mid)
                elif len(images) > 1:
                    media = [{"type": "photo", "media": img} for img in images]
                    mids = await self._send_media_group_safe(moderator_id, media)
                    message_ids.extend(mids)
            except Exception as exc:  # noqa: BLE001
                log.warning("Failed to send news moderation preview to %s: %s", moderator_id, exc)
                continue
            if message_ids:
                message_ids_by_chat[moderator_id] = message_ids
        await self.state.set_news_moderation_message_ids(url, message_ids_by_chat)

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
            if not message_ids:
                raise RuntimeError(
                    "Telegram news publish failed: no messages sent. "
                    "Check TG_CHANNEL_ID and bot rights in target chat/channel."
                )
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
        chunks = chunk_news_text(full_text, limit=3500)
        all_images: List[str] = payload.get("images", []) or []
        images: List[str] = all_images[:1] if self._is_digest_news(payload=payload) else all_images[:10]

        if images:
            if len(images) == 1:
                caption, extra_chunks = self._split_photo_caption_and_chunks(
                    chunks,
                    caption_limit=1000,
                    body_limit=3500,
                    preserve_more_markers=True,
                    fill_caption_from_body=True,
                )
                rendered_caption = self._render_digest_more_links(caption or "", html=True) or None
                if rendered_caption and len(rendered_caption) > 1000:
                    first_parts = chunk_news_text(caption or "", limit=900)
                    caption = first_parts[0] if first_parts else ""
                    rendered_caption = self._render_digest_more_links(caption, html=True) or None
                    extra_chunks = [part for part in first_parts[1:] if part] + extra_chunks
                mid = await self.tg.send_photo(
                    chat_id=self.config.tg_channel_id,
                    photo=images[0],
                    caption=rendered_caption,
                )
                if mid:
                    message_ids.append(mid)
                for extra in extra_chunks:
                    extra_text = self._render_digest_more_links(extra, html=True)
                    mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=extra_text)
                    if mid:
                        message_ids.append(mid)
            else:
                caption, extra_chunks = self._split_photo_caption_and_chunks(
                    chunks,
                    caption_limit=1000,
                    body_limit=3500,
                    preserve_more_markers=True,
                    fill_caption_from_body=True,
                )
                rendered_caption = self._render_digest_more_links(caption or "", html=True) or None
                if rendered_caption and len(rendered_caption) > 1000:
                    first_parts = chunk_news_text(caption or "", limit=900)
                    caption = first_parts[0] if first_parts else ""
                    rendered_caption = self._render_digest_more_links(caption, html=True) or None
                    extra_chunks = [part for part in first_parts[1:] if part] + extra_chunks
                group = []
                for idx, img in enumerate(images):
                    entry = {"type": "photo", "media": img}
                    if idx == 0 and rendered_caption:
                        entry["caption"] = rendered_caption
                        entry["parse_mode"] = "HTML"
                    group.append(entry)
                mids = await self._send_media_group_safe(self.config.tg_channel_id, group)
                message_ids.extend(mids)
                for extra in extra_chunks:
                    extra_text = self._render_digest_more_links(extra, html=True)
                    mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=extra_text)
                    if mid:
                        message_ids.append(mid)
        else:
            for part in chunks:
                part_text = self._render_digest_more_links(part, html=True)
                mid = await self.tg.send_message(chat_id=self.config.tg_channel_id, text=part_text)
                if mid:
                    message_ids.append(mid)
        return message_ids

    async def _publish_news_vk(self, payload: Dict[str, Any]) -> Optional[int]:
        if not self.vk:
            raise RuntimeError("VK client is not configured")
        text = self._format_news_text(payload, html=False)
        source_link = str(payload.get("url") or "").strip()
        if source_link and source_link not in text:
            text = f"{source_link}\n\n{text}" if text else source_link
        if len(text) > 6000:
            text = truncate_text_without_word_cut(text, limit=6000)
        attachments: List[str] = []
        all_images: List[str] = payload.get("images", []) or []
        is_digest = self._is_digest_news(payload=payload)
        images: List[str] = all_images[:1] if is_digest else all_images[:10]
        if images and not self.config.dry_run:
            max_images = 1 if is_digest else 10
            attachments = await self.vk.upload_wall_photos(images, max_images=max_images)
            if not attachments:
                # Для group-token upload API недоступен; публикуем без attachments.
                # Превью строится по ссылке на новость внутри текста поста.
                attachments = []
                if source_link:
                    log.warning(
                        "VK photo upload returned no attachments; "
                        "posting without attachments and relying on link preview: %s",
                        source_link,
                    )
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

    def _owner_menu_keyboard(self) -> Dict[str, Any]:
        return {
            "inline_keyboard": [
                [{"text": "🔄 Обновить посты", "callback_data": "refresh_posts"}],
                [{"text": "📌 Крайний пост VK", "callback_data": "latest_vk"}],
                [{"text": "📰 Крайняя новость сайта", "callback_data": "latest_site"}],
                [{"text": "🔗 Новость по ссылке", "callback_data": "news_by_link"}],
            ]
        }

    async def _notify_owner_with_menu(self, text: str) -> None:
        await self.tg.send_message(
            chat_id=self.config.owner_id,
            text=text,
            reply_markup=self._owner_menu_keyboard(),
        )

    async def _notify_publish_error(self, text: str, actor: Optional[str] = None) -> None:
        if actor:
            text = f"{text}\nДействие модератора: {actor}"
        try:
            await self._notify_owner_with_menu(text)
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to notify owner about publish error: %s", exc)

    async def _notify_post_result(
        self,
        published: bool,
        source_link: str,
        publish_tg: bool,
        tg_message_ids: Sequence[int],
        publish_vk: bool,
        vk_link: Optional[str],
        actor: Optional[str] = None,
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
        if actor:
            lines.append(f"Действие модератора: {actor}")
        try:
            await self._notify_owner_with_menu("\n".join(lines))
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
        actor: Optional[str] = None,
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
        if actor:
            lines.append(f"Действие модератора: {actor}")
        try:
            await self._notify_owner_with_menu("\n".join(lines))
        except Exception as exc:  # noqa: BLE001
            log.warning("Failed to notify owner about news moderation result: %s", exc)

    async def _delete_post_moderation_messages(self, post_id: int) -> None:
        message_map = await self.state.get_moderation_message_id_map(post_id)
        if not message_map:
            return
        total_deleted = 0
        total_messages = 0
        for raw_chat_id, message_ids in message_map.items():
            chat_id = self._resolve_moderation_chat_id(raw_chat_id)
            normalized_ids = self._normalize_message_ids(message_ids)
            if chat_id is None or not normalized_ids:
                continue
            total_deleted += await self.tg.delete_messages(chat_id, normalized_ids)
            total_messages += len(normalized_ids)
        if total_messages:
            log.info(
                "Deleted %s/%s post moderation messages for %s",
                total_deleted,
                total_messages,
                post_id,
            )
        await self.state.clear_moderation_message_ids(post_id)

    async def _delete_news_moderation_messages(
        self,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        alias_urls = set(self._news_url_variants(url))
        payload_url = str((payload or {}).get("url") or "")
        if payload_url:
            alias_urls.update(self._news_url_variants(payload_url))

        ids_by_chat: Dict[int, List[int]] = {}
        urls_with_ids: List[str] = []
        for alias in alias_urls:
            message_map = await self.state.get_news_moderation_message_id_map(alias)
            if not message_map:
                continue
            alias_has_ids = False
            for raw_chat_id, message_ids in message_map.items():
                chat_id = self._resolve_moderation_chat_id(raw_chat_id)
                normalized = self._normalize_message_ids(message_ids)
                if chat_id is None or not normalized:
                    continue
                ids_by_chat.setdefault(chat_id, []).extend(normalized)
                alias_has_ids = True
            if alias_has_ids:
                urls_with_ids.append(alias)

        if not ids_by_chat and not urls_with_ids:
            return

        deleted = 0
        total_ids = 0
        for chat_id, message_ids in ids_by_chat.items():
            unique_ids = sorted(set(message_ids))
            if not unique_ids:
                continue
            total_ids += len(unique_ids)
            deleted += await self.tg.delete_messages(chat_id, unique_ids)
        for alias in urls_with_ids:
            await self.state.clear_news_moderation_message_ids(alias)
        log.info(
            "Deleted %s/%s news moderation messages for %s (aliases=%s)",
            deleted,
            total_ids,
            url,
            ",".join(sorted(urls_with_ids)),
        )

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
