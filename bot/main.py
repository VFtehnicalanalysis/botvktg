from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Any, Dict, List, Optional

from .config import load_config
from .logging_setup import setup_logging
from .pipeline import Pipeline
from .state import StateStore
from .tg_client import TelegramClient
from .vk_client import VKClient
from .site_client import SiteClient

log = logging.getLogger(__name__)


async def vk_loop(vk: VKClient, pipeline: Pipeline, state: StateStore, tg: TelegramClient) -> None:
    same_error: Optional[str] = None
    error_count = 0
    while True:
        try:
            ts, updates = await vk.longpoll()
            if ts:
                await state.set_last_ts(ts)
            for upd in updates:
                await pipeline.handle_vk_update(upd)
            # сбрасываем счётчик при успешном запросе
            same_error = None
            error_count = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("VK loop error: %s", exc)
            msg = str(exc)
            if same_error == msg:
                error_count += 1
            else:
                same_error = msg
                error_count = 1
            if error_count >= 5:
                warn = f"VK loop stopped after 5 identical errors: {msg}"
                log.error(warn)
                try:
                    await tg.notify_owner(warn)
                finally:
                    raise SystemExit(warn) from exc
            await asyncio.sleep(5)


async def vk_fallback_loop(vk: VKClient, pipeline: Pipeline, every_seconds: int = 300) -> None:
    while True:
        try:
            items = await vk.wall_get_recent(count=3)
            for item in reversed(items):
                await pipeline.handle_post(item, source="fallback")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("VK fallback error: %s", exc)
        await asyncio.sleep(every_seconds)


async def tg_callback_loop(tg: TelegramClient, pipeline: Pipeline, vk: "VKClient") -> None:  # type: ignore
    offset: Optional[int] = None
    while True:
        try:
            updates = await tg.get_updates(
                offset=offset, allowed_updates=["callback_query", "message"]
            )
            for upd in updates:
                offset = upd["update_id"] + 1
                if "callback_query" in upd:
                    cb = upd["callback_query"]
                    data = cb.get("data") or ""
                    user_id = cb.get("from", {}).get("id")
                    if data == "refresh_posts" and user_id == tg.config.owner_id:
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Обновление запущено")
                        if tg.config.source_mode in ("vk", "vk+site"):
                            await pipeline.refresh_recent_posts(vk)
                        if tg.config.source_mode in ("site", "vk+site"):
                            await pipeline.refresh_latest_news()
                        await tg.notify_owner("Ручное обновление завершено (без дублей).")
                    elif data == "latest_vk" and user_id == tg.config.owner_id:
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Берем крайний пост VK")
                        if tg.config.source_mode in ("vk", "vk+site"):
                            await pipeline.refresh_recent_posts(vk, count=1, force=True)
                    elif data == "latest_site" and user_id == tg.config.owner_id:
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Берем крайнюю новость сайта")
                        if tg.config.source_mode in ("site", "vk+site"):
                            await pipeline.refresh_latest_news(force=True)
                    else:
                        await pipeline.handle_callback(upd)
                elif "message" in upd:
                    msg = upd["message"]
                    text = msg.get("text") or ""
                    from_id = msg.get("from", {}).get("id")
                    if from_id == tg.config.owner_id and text.strip().lower() in {"/refresh", "обновить посты"}:
                        if tg.config.source_mode in ("vk", "vk+site"):
                            await pipeline.refresh_recent_posts(vk)
                        if tg.config.source_mode in ("site", "vk+site"):
                            await pipeline.refresh_latest_news()
                        await tg.notify_owner("Ручное обновление завершено (без дублей).")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("TG callback loop error: %s", exc)
            await asyncio.sleep(3)


async def run(dry_run: bool = False) -> None:
    config = load_config(dry_run=dry_run)
    setup_logging(config.log_dir)
    log.info("Starting bot (dry-run=%s) with moderation=%s", dry_run, config.moderation_mode)

    state = StateStore(config.state_path)
    await state.load()

    tg = TelegramClient(config)
    vk = VKClient(config)
    site = SiteClient(config)
    pipeline = Pipeline(config, state, tg, site=site, vk=vk)

    try:
        keyboard = {
            "inline_keyboard": [
                [{"text": "🔄 Обновить посты", "callback_data": "refresh_posts"}],
                [{"text": "📌 Крайний пост VK", "callback_data": "latest_vk"}],
                [{"text": "📰 Крайняя новость сайта", "callback_data": "latest_site"}],
            ]
        }
        await tg.send_message(
            chat_id=config.owner_id,
            text=f"Бот запущен. moderation={config.moderation_mode}, dry_run={dry_run}.",
            reply_markup=keyboard,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to notify owner on startup: %s", exc)

    tasks = []
    if config.source_mode in ("vk", "vk+site"):
        tasks.append(asyncio.create_task(vk_loop(vk, pipeline, state, tg), name="vk_loop"))
        tasks.append(asyncio.create_task(vk_fallback_loop(vk, pipeline), name="vk_fallback"))
    tasks.append(asyncio.create_task(tg_callback_loop(tg, pipeline, vk), name="tg_callback"))
    if config.source_mode in ("site", "vk+site"):
        async def site_worker():
            same_error: Optional[str] = None
            err_count = 0
            while True:
                try:
                    await pipeline.refresh_latest_news()
                    same_error = None
                    err_count = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.exception("Site worker error: %s", exc)
                    msg = str(exc)
                    if same_error == msg:
                        err_count += 1
                    else:
                        same_error = msg
                        err_count = 1
                    if err_count >= 5:
                        warn = f"Site worker stopped after 5 identical errors: {msg}"
                        log.error(warn)
                        try:
                            await tg.notify_owner(warn)
                        finally:
                            raise SystemExit(warn) from exc
                await asyncio.sleep(config.site_poll_interval)
        tasks.append(asyncio.create_task(site_worker(), name="site_worker"))
    try:
        await asyncio.gather(*tasks)
    except SystemExit as exc:
        log.error("Bot stopped: %s", exc)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return
    except Exception as exc:  # noqa: BLE001
        log.exception("Fatal error, stopping bot: %s", exc)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        await tg.close()
        await vk.close()
        await site.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VK to Telegram bot")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without sending to Telegram")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
