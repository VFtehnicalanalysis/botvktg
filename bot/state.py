from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Dict, List, Optional


class StateStore:
    def __init__(self, path: Path, max_processed: int = 300) -> None:
        self.path = path
        self.max_processed = max_processed
        self._lock = asyncio.Lock()
        self._data: Dict[str, object] = {
            "last_ts": None,
            "last_post_id": None,
            "processed_ids": [],
            "posts": {},
            "moderation_tokens": {},
        }

    async def load(self) -> None:
        if not self.path.exists():
            return
        try:
            self._data = json.loads(self.path.read_text())
        except json.JSONDecodeError:
            # Corrupted state; start fresh but keep file as backup.
            backup = self.path.with_suffix(".bak")
            self.path.rename(backup)
            self._data = {
                "last_ts": None,
                "last_post_id": None,
                "processed_ids": [],
                "posts": {},
                "moderation_tokens": {},
            }

    async def save(self) -> None:
        self.path.write_text(json.dumps(self._data, ensure_ascii=True, indent=2))

    async def _with_lock(self, func, *args, **kwargs):
        async with self._lock:
            return await func(*args, **kwargs)

    async def get_last_ts(self) -> Optional[str]:
        return self._data.get("last_ts")  # type: ignore

    async def set_last_ts(self, ts: str) -> None:
        async with self._lock:
            self._data["last_ts"] = ts
            await self.save()

    async def should_skip(self, post_id: int, content_hash: str) -> bool:
        post = self._data.get("posts", {}).get(str(post_id))
        if not post:
            return False
        stored_hash = post.get("hash")
        status = post.get("status")
        if stored_hash == content_hash and status in {"pending", "approved", "published", "rejected"}:
            return True
        return False

    async def mark_pending(
        self,
        post_id: int,
        content_hash: str,
        token: Optional[str],
        payload: Optional[Dict[str, object]] = None,
    ) -> None:
        async with self._lock:
            now = int(time.time())
            existing = self._data.get("posts", {}).get(str(post_id))
            if existing and existing.get("token"):
                old_token = existing["token"]
                self._data.get("moderation_tokens", {}).pop(old_token, None)
            post = {
                "hash": content_hash,
                "status": "pending" if token else "auto",
                "token": token,
                "tg_message_ids": [],
                "updated_at": now,
                "payload": payload or {},
            }
            self._data.setdefault("posts", {})[str(post_id)] = post
            if token:
                self._data.setdefault("moderation_tokens", {})[token] = post_id
            self._data["last_post_id"] = post_id
            self._append_processed(post_id)
            await self.save()

    async def mark_rejected(self, post_id: int) -> None:
        async with self._lock:
            post = self._data.get("posts", {}).get(str(post_id))
            if post:
                post["status"] = "rejected"
                post["updated_at"] = int(time.time())
                if post.get("token"):
                    self._data.get("moderation_tokens", {}).pop(post["token"], None)
            await self.save()

    async def mark_approved(self, post_id: int) -> None:
        async with self._lock:
            post = self._data.get("posts", {}).get(str(post_id))
            if post:
                post["status"] = "approved"
                post["updated_at"] = int(time.time())
                if post.get("token"):
                    self._data.get("moderation_tokens", {}).pop(post["token"], None)
            await self.save()

    async def mark_published(self, post_id: int, tg_message_ids: List[int]) -> None:
        async with self._lock:
            post = self._data.get("posts", {}).get(str(post_id)) or {}
            post["status"] = "published"
            post["tg_message_ids"] = tg_message_ids
            post["updated_at"] = int(time.time())
            self._data.setdefault("posts", {})[str(post_id)] = post
            self._append_processed(post_id)
            await self.save()

    async def get_post_by_token(self, token: str) -> Optional[int]:
        return self._data.get("moderation_tokens", {}).get(token)  # type: ignore

    async def invalidate_token(self, token: str) -> None:
        async with self._lock:
            self._data.get("moderation_tokens", {}).pop(token, None)
            await self.save()

    async def get_post_record(self, post_id: int) -> Optional[Dict[str, object]]:
        return self._data.get("posts", {}).get(str(post_id))  # type: ignore

    async def get_payload(self, post_id: int) -> Optional[Dict[str, object]]:
        post = self._data.get("posts", {}).get(str(post_id))  # type: ignore
        if not post:
            return None
        return post.get("payload")  # type: ignore

    def _append_processed(self, post_id: int) -> None:
        processed: List[int] = self._data.setdefault("processed_ids", [])  # type: ignore
        if post_id in processed:
            return
        processed.append(post_id)
        if len(processed) > self.max_processed:
            del processed[0 : len(processed) - self.max_processed]
