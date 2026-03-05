from __future__ import annotations

import argparse
import asyncio
import logging
import re
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import load_config
from .logging_setup import get_recent_logs_text, setup_logging
from .pipeline import Pipeline
from .state import StateStore
from .tg_client import TelegramClient
from .vk_client import VKClient
from .site_client import SiteClient

log = logging.getLogger(__name__)
PLANNED_RESTART_HOUR = 9
PLANNED_RESTART_MINUTE = 30


class ActivityTracker:
    def __init__(self) -> None:
        self.last_ts = time.monotonic()
        self.last_source = "startup"

    def touch(self, source: str) -> None:
        self.last_ts = time.monotonic()
        self.last_source = source

    def age_seconds(self) -> float:
        return max(0.0, time.monotonic() - self.last_ts)


def _format_telegram_actor(user: Dict[str, Any]) -> str:
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


async def _notify_owner_about_moderator_action(
    tg: TelegramClient,
    user_id: Optional[int],
    user: Dict[str, Any],
    action: str,
) -> None:
    if tg.config.is_owner(user_id):
        return
    actor = _format_telegram_actor(user)
    try:
        await tg.notify_owner(f"Модератор {actor}: {action}")
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to notify owner about moderator action: %s", exc)


def _format_restart_timestamp() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def _consume_restart_reason(path: Path) -> str:
    try:
        if not path.exists():
            return ""
        reason = path.read_text(encoding="utf-8").strip()
        path.unlink(missing_ok=True)
        return reason
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to consume restart reason from %s: %s", path, exc)
        return ""


def _write_restart_reason(path: Path, reason: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"{reason.strip()}\n", encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to write restart reason to %s: %s", path, exc)


def _build_crash_report_text(exc: Exception) -> str:
    ts = _format_restart_timestamp()
    recent = get_recent_logs_text(window_seconds=3 * 60).strip()
    if not recent:
        recent = "(Нет логов за последние 3 минуты)"
    return (
        f"Crash timestamp: {ts}\n"
        f"Exception: {type(exc).__name__}: {exc}\n"
        "\n=== Recent logs (last 3 minutes) ===\n"
        f"{recent}\n"
    )


def _build_recent_logs_report_text(event_label: str) -> str:
    ts = _format_restart_timestamp()
    recent = get_recent_logs_text(window_seconds=3 * 60).strip()
    if not recent:
        recent = "(Нет логов за последние 3 минуты)"
    return (
        f"Event: {event_label}\n"
        f"Timestamp: {ts}\n"
        "\n=== Recent logs (last 3 minutes) ===\n"
        f"{recent}\n"
    )


async def _send_owner_recent_logs_report(
    tg: TelegramClient,
    event_label: str,
    caption: str,
) -> None:
    if int(getattr(tg.config, "owner_id", 0) or 0) <= 0:
        return
    report_text = _build_recent_logs_report_text(event_label=event_label)
    report_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".txt",
            prefix="botvktg_logs_",
            delete=False,
            encoding="utf-8",
        ) as fh:
            fh.write(report_text)
            report_path = Path(fh.name)
        if report_path:
            await tg.send_document(
                chat_id=tg.config.owner_id,
                file_path=report_path,
                caption=caption,
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to send recent logs report to owner: %s", exc)
    finally:
        if report_path and report_path.exists():
            try:
                report_path.unlink()
            except Exception:  # noqa: BLE001
                pass


async def _send_owner_crash_report(config, exc: Exception) -> None:
    if int(getattr(config, "owner_id", 0) or 0) <= 0:
        return
    report_text = _build_crash_report_text(exc)
    report_path: Optional[Path] = None
    tg_report: Optional[TelegramClient] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".txt",
            prefix="botvktg_crash_",
            delete=False,
            encoding="utf-8",
        ) as fh:
            fh.write(report_text)
            report_path = Path(fh.name)
        tg_report = TelegramClient(config)
        await tg_report.send_message(
            chat_id=config.owner_id,
            text="Бот аварийно перезапущен. Отправляю лог за последние 3 минуты.",
        )
        if report_path:
            await tg_report.send_document(
                chat_id=config.owner_id,
                file_path=report_path,
                caption="Логи перед падением (3 минуты)",
            )
    except Exception as report_exc:  # noqa: BLE001
        log.warning("Failed to send crash report to owner: %s", report_exc)
    finally:
        if tg_report is not None:
            try:
                await tg_report.close()
            except Exception:  # noqa: BLE001
                pass
        if report_path and report_path.exists():
            try:
                report_path.unlink()
            except Exception:  # noqa: BLE001
                pass


async def _notify_owner_startup(
    tg: TelegramClient,
    keyboard: Dict[str, Any],
    dry_run: bool,
    restart_count: int,
    restart_reason: str = "",
) -> None:
    if tg.config.owner_id <= 0:
        return
    ts = _format_restart_timestamp()
    reason_l = (restart_reason or "").lower()
    if reason_l.startswith("planned_daily"):
        text = (
            f"Бот успешно планово перезагружен.\n"
            f"Время перезапуска: {ts}."
        )
    elif reason_l.startswith("unplanned_inactivity"):
        text = (
            f"Бот успешно ВНЕпланово перезапущен.\n"
            f"Причина: не было активности мониторинга более 5 минут.\n"
            f"Время перезапуска: {ts}."
        )
    elif restart_count <= 0:
        text = (
            f"Бот запущен. Время запуска: {ts}\n"
            f"moderation={tg.config.moderation_mode}, dry_run={dry_run}."
        )
    else:
        text = (
            f"Бот перезапущен. Время перезапуска: {ts}\n"
            f"Попытка #{restart_count}. moderation={tg.config.moderation_mode}, dry_run={dry_run}."
        )
    try:
        await tg.send_message(
            chat_id=tg.config.owner_id,
            text=text,
            reply_markup=keyboard,
        )
        if reason_l.startswith("planned_daily"):
            await _send_owner_recent_logs_report(
                tg=tg,
                event_label="planned_daily_restart",
                caption="Логи до плановой перезагрузки (3 минуты)",
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("Failed to notify owner on startup/restart: %s", exc)


def _seconds_until_next_planned_restart(hour: int, minute: int) -> float:
    now = datetime.now().astimezone()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


async def planned_daily_restart_loop(config) -> None:
    while True:
        wait_seconds = _seconds_until_next_planned_restart(
            hour=PLANNED_RESTART_HOUR,
            minute=PLANNED_RESTART_MINUTE,
        )
        log.info(
            "Next planned restart at %02d:%02d (in %.0f seconds).",
            PLANNED_RESTART_HOUR,
            PLANNED_RESTART_MINUTE,
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)
        reason = f"planned_daily:{PLANNED_RESTART_HOUR:02d}:{PLANNED_RESTART_MINUTE:02d}"
        _write_restart_reason(config.restart_reason_path, reason)
        raise SystemExit(
            f"Planned daily restart at {PLANNED_RESTART_HOUR:02d}:{PLANNED_RESTART_MINUTE:02d}"
        )


async def vk_loop(
    vk: VKClient,
    pipeline: Pipeline,
    state: StateStore,
    tg: TelegramClient,
    min_cycle_seconds: int,
    activity: Optional[ActivityTracker] = None,
) -> None:
    same_error: Optional[str] = None
    error_count = 0
    while True:
        cycle_started = time.monotonic()
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
        if activity:
            activity.touch("vk_longpoll")
        wait = max(0.0, float(min_cycle_seconds) - (time.monotonic() - cycle_started))
        if wait:
            await asyncio.sleep(wait)


async def vk_fallback_loop(
    vk: VKClient,
    pipeline: Pipeline,
    every_seconds: int = 300,
    min_cycle_seconds: int = 60,
    activity: Optional[ActivityTracker] = None,
) -> None:
    interval = max(int(every_seconds), int(min_cycle_seconds))
    while True:
        cycle_started = time.monotonic()
        try:
            items = await vk.wall_get_recent(count=3)
            for item in reversed(items):
                await pipeline.handle_post(item, source="fallback")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("VK fallback error: %s", exc)
        if activity:
            activity.touch("vk_fallback")
        wait = max(0.0, float(interval) - (time.monotonic() - cycle_started))
        if wait:
            await asyncio.sleep(wait)


async def monitor_activity_watchdog(
    config,
    activity: ActivityTracker,
) -> None:
    inactivity_limit = max(60, int(config.monitor_inactivity_restart_seconds))
    check_interval = max(5, min(30, inactivity_limit // 6))
    while True:
        await asyncio.sleep(check_interval)
        idle_seconds = int(activity.age_seconds())
        if idle_seconds < inactivity_limit:
            continue
        reason = f"unplanned_inactivity:{idle_seconds}:{activity.last_source}"
        log.error(
            "Monitoring inactivity detected: %ss without checks (last_source=%s). Restarting.",
            idle_seconds,
            activity.last_source,
        )
        _write_restart_reason(config.restart_reason_path, reason)
        raise SystemExit(
            f"Restarting bot: monitoring inactivity {idle_seconds}s "
            f"(last_source={activity.last_source})"
        )


async def tg_callback_loop(tg: TelegramClient, pipeline: Pipeline, vk: "VKClient") -> None:  # type: ignore
    offset: Optional[int] = None
    min_response_interval = max(0.0, float(tg.config.user_response_interval_seconds))
    last_response_ts = 0.0
    while True:
        try:
            updates = await tg.get_updates(
                offset=offset, allowed_updates=["callback_query", "message"]
            )
            for upd in updates:
                now_ts = time.monotonic()
                wait = (last_response_ts + min_response_interval) - now_ts
                if wait > 0:
                    await asyncio.sleep(wait)
                last_response_ts = time.monotonic()
                offset = upd["update_id"] + 1
                if "callback_query" in upd:
                    cb = upd["callback_query"]
                    data = cb.get("data") or ""
                    from_user = cb.get("from", {}) or {}
                    user_id = from_user.get("id")
                    is_moderator = tg.config.is_moderator(user_id)
                    if data == "refresh_posts":
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Функция отключена")
                        continue
                    if data in {"latest_vk", "latest_site", "news_by_link"} and not is_moderator:
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Нет доступа")
                        continue
                    if data == "latest_vk" and is_moderator:
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Берем крайний пост VK")
                        await _notify_owner_about_moderator_action(
                            tg, user_id, from_user, "запросил крайний пост VK."
                        )
                        if tg.config.source_mode in ("vk", "vk+site"):
                            vk_count = await pipeline.refresh_recent_posts(vk, count=1, force=True)
                            if vk_count == 0:
                                await tg.notify_owner(
                                    "Не удалось получить крайний пост VK из кеша авто-потока."
                                )
                    elif data == "latest_site" and is_moderator:
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Берем крайнюю новость сайта")
                        await _notify_owner_about_moderator_action(
                            tg, user_id, from_user, "запросил крайнюю новость сайта."
                        )
                        if tg.config.source_mode in ("site", "vk+site"):
                            await pipeline.refresh_latest_news(force=True)
                    elif data == "news_by_link" and is_moderator:
                        if cb.get("id"):
                            await tg.answer_callback_query(cb["id"], text="Пришлите ссылку в чат")
                        await _notify_owner_about_moderator_action(
                            tg, user_id, from_user, "открыл режим публикации новости по ссылке."
                        )
                        await tg.notify_owner(
                            "Пришлите ссылку на новость/дайджест с сайта одним сообщением."
                        )
                    else:
                        await pipeline.handle_callback(upd)
                elif "message" in upd:
                    msg = upd["message"]
                    text = msg.get("text") or ""
                    from_user = msg.get("from", {}) or {}
                    from_id = from_user.get("id")
                    if tg.config.is_moderator(from_id):
                        lowered = text.strip().lower()
                        url = _extract_first_url(text)
                        if url and pipeline.site and pipeline.site.is_supported_news_url(url):
                            await _notify_owner_about_moderator_action(
                                tg, from_id, from_user, f"передал ссылку для публикации: {url}"
                            )
                            await pipeline.handle_news({"url": url, "title": "", "date": ""}, force=True)
                    elif from_id:
                        chat_id = (msg.get("chat") or {}).get("id") or from_id
                        await tg.send_message(
                            chat_id=chat_id,
                            text="Это приватный бот, он недоступен всем пользователям",
                        )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("TG callback loop error: %s", exc)
            await asyncio.sleep(3)


async def run(dry_run: bool = False, restart_count: int = 0) -> None:
    config = load_config(dry_run=dry_run)
    setup_logging(config.log_dir)
    log.info("Starting bot (dry-run=%s) with moderation=%s", dry_run, config.moderation_mode)

    state = StateStore(config.state_path)
    await state.load()

    tg = TelegramClient(config)
    vk = VKClient(config)
    site = SiteClient(config)
    pipeline = Pipeline(config, state, tg, site=site, vk=vk)
    activity = ActivityTracker()
    restart_reason = _consume_restart_reason(config.restart_reason_path)

    keyboard = {
        "inline_keyboard": [
            [{"text": "📌 Крайний пост VK", "callback_data": "latest_vk"}],
            [{"text": "📰 Крайняя новость сайта", "callback_data": "latest_site"}],
            [{"text": "🔗 Новость по ссылке", "callback_data": "news_by_link"}],
        ]
    }
    await _notify_owner_startup(
        tg=tg,
        keyboard=keyboard,
        dry_run=dry_run,
        restart_count=restart_count,
        restart_reason=restart_reason,
    )

    tasks = []
    min_monitor_interval = max(60, int(config.monitor_min_interval_seconds))
    if config.source_mode in ("vk", "vk+site"):
        tasks.append(
            asyncio.create_task(
                vk_loop(
                    vk,
                    pipeline,
                    state,
                    tg,
                    min_cycle_seconds=min_monitor_interval,
                    activity=activity,
                ),
                name="vk_loop",
            )
        )
        tasks.append(
            asyncio.create_task(
                vk_fallback_loop(
                    vk,
                    pipeline,
                    min_cycle_seconds=min_monitor_interval,
                    activity=activity,
                ),
                name="vk_fallback",
            )
        )
    tasks.append(asyncio.create_task(tg_callback_loop(tg, pipeline, vk), name="tg_callback"))
    tasks.append(
        asyncio.create_task(
            monitor_activity_watchdog(config=config, activity=activity),
            name="monitor_activity_watchdog",
        )
    )
    tasks.append(
        asyncio.create_task(
            planned_daily_restart_loop(config=config),
            name="planned_daily_restart",
        )
    )
    if config.source_mode in ("site", "vk+site"):
        async def site_worker():
            same_error: Optional[str] = None
            err_count = 0
            interval = max(60, int(config.monitor_min_interval_seconds), int(config.site_poll_interval))
            while True:
                cycle_started = time.monotonic()
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
                        warn = (
                            "Site worker has 5 identical errors подряд; "
                            "продолжаем работу VK/TG и пробуем сайт дальше. "
                            f"Последняя ошибка: {msg}"
                        )
                        log.error(warn)
                        try:
                            await tg.notify_owner(warn)
                        except Exception as notify_exc:  # noqa: BLE001
                            log.warning("Failed to notify owner about site worker errors: %s", notify_exc)
                        same_error = None
                        err_count = 0
                activity.touch("site_poll")
                wait = max(0.0, float(interval) - (time.monotonic() - cycle_started))
                if wait:
                    await asyncio.sleep(wait)
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


async def run_forever(dry_run: bool = False) -> None:
    boot_config = load_config(dry_run=dry_run)
    setup_logging(boot_config.log_dir)
    restart_count = 0
    while True:
        config = load_config(dry_run=dry_run)
        try:
            await run(dry_run=dry_run, restart_count=restart_count)
            log.warning(
                "Bot run exited, restarting in %s seconds",
                config.restart_backoff_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.exception("Bot crashed, restarting in %s seconds: %s", config.restart_backoff_seconds, exc)
            await _send_owner_crash_report(config, exc)
        restart_count += 1
        await asyncio.sleep(max(1, int(config.restart_backoff_seconds)))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VK to Telegram bot")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without sending to Telegram")
    return parser.parse_args()


def _extract_first_url(text: str) -> Optional[str]:
    match = re.search(r"(https?://\S+|www\.\S+)", text)
    if not match:
        return None
    url = match.group(0).strip()
    while url and url[-1] in ").,;:!?]>\"'":
        url = url[:-1]
    if url.startswith("www."):
        url = f"https://{url}"
    return url


def main() -> None:
    args = parse_args()
    asyncio.run(run_forever(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
