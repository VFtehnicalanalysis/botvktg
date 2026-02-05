from __future__ import annotations

import logging
import re
from html.parser import HTMLParser
from html import unescape
from typing import Dict, List, Optional

import httpx

from .config import Config

log = logging.getLogger(__name__)


def _abs_url(base: str, url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("//"):
        return f"https:{url}"
    if url.startswith("/"):
        return f"{base}{url}"
    return f"{base}/{url}"


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: List[str] = []
        self._skip = False
        self._skip_stack: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "nav", "header", "footer"):
            self._skip = True
            self._skip_stack.append(tag)

    def handle_endtag(self, tag):
        if self._skip_stack and tag == self._skip_stack[-1]:
            self._skip_stack.pop()
            self._skip = bool(self._skip_stack)
        if tag in ("p", "br", "div", "h1", "h2", "h3"):
            self._chunks.append("\n")

    def handle_data(self, data):
        if self._skip:
            return
        text = data.strip()
        if text:
            self._chunks.append(text + " ")

    def text(self) -> str:
        joined = "".join(self._chunks)
        # collapse spaces/newlines
        return re.sub(r"\s+\n", "\n", re.sub(r"[ \t]+", " ", joined)).strip()


DATE_RE = re.compile(r"\b\d{1,2}\s+[A-Za-zА-Яа-яЁё]+\b(?:\s+\d{4})?", re.I)


def _is_news_link(href: str) -> bool:
    href_l = href.lower()
    return any(token in href_l for token in ("news.", "article.", "/news/", "/article/"))


class FeedParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.items: List[Dict[str, str]] = []
        self._in_feed = False
        self._feed_done = False
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []
        self._last_date: str = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self._current_href = href
                self._current_text = []

    def handle_endtag(self, tag):
        if tag == "a" and self._current_href:
            if self._in_feed and not self._feed_done:
                title = " ".join(self._current_text).strip()
                if title and _is_news_link(self._current_href):
                    self.items.append(
                        {"url": self._current_href, "title": title, "date": self._last_date}
                    )
            self._current_href = None
            self._current_text = []

    def handle_data(self, data):
        text = data.strip()
        if not text:
            return
        if not self._in_feed and "Лента событий" in text:
            self._in_feed = True
            return
        if self._in_feed and not self._feed_done:
            lower = text.lower()
            if "показать" in lower and "еще" in lower:
                self._feed_done = True
                return
            if lower == "контакты":
                self._feed_done = True
                return
            m = DATE_RE.search(text)
            if m:
                self._last_date = m.group(0).strip()
        if self._current_href and self._in_feed and not self._feed_done:
            self._current_text.append(text)


class SiteClient:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self.client.aclose()

    async def fetch_latest_news(self) -> Optional[Dict[str, str]]:
        url = f"{self.config.site_base_url}{self.config.site_news_path}"
        resp = await self.client.get(url)
        resp.raise_for_status()
        html = unescape(resp.text)
        # Пытаемся вытащить свежую запись из "Ленты событий" и поддерживаем Article/News ссылки
        parser = FeedParser()
        parser.feed(html)
        if parser.items:
            item = parser.items[0]
            full_url = _abs_url(self.config.site_base_url, item["url"])
            return {"url": full_url, "title": item.get("title", ""), "date": item.get("date", "")}

        # Fallback на старую вёрстку
        block_match = re.search(
            r'<div class="w33 headline status-Published"[^>]*>(.*?)</div>\s*</div>',
            html,
            re.S,
        )
        if not block_match:
            log.warning("No news block found on alumni page")
            return None
        block_html = block_match.group(1)
        link_match = re.search(
            r'<h3 class="news_text">\s*<a href="([^"]+)"[^>]*>(.*?)</a>',
            block_html,
            re.S,
        )
        date_block = re.search(r'<p class="title_text">(.*?)</p>', block_html, re.S)
        if not link_match:
            log.warning("No link found inside news block")
            return None
        link = link_match.group(1).strip()
        if not _is_news_link(link):
            log.warning("Latest link is not Article/News: %s", link)
            return None
        title = re.sub(r"<[^>]+>", "", link_match.group(2)).strip()
        if date_block:
            date = re.sub(r"<[^>]+>", " ", date_block.group(1))
            date = re.sub(r"\s+", " ", date).strip()
        else:
            date = ""
        full_url = _abs_url(self.config.site_base_url, link)
        return {"url": full_url, "title": title, "date": date}

    async def fetch_news_detail(self, url: str, title: Optional[str] = None) -> Dict[str, object]:
        resp = await self.client.get(url)
        resp.raise_for_status()
        html = unescape(resp.text)
        content_html = self._extract_main_block(html)
        # текст
        is_digest = self._is_digest(title)
        parser = TextExtractor()
        parser.feed(content_html)
        text = self._clean_text(parser.text(), title, is_digest=is_digest)
        # изображения: только с нашего домена, допустимые расширения или raw.php
        imgs = re.findall(r'<img[^>]+src="([^"]+)"', content_html)
        images: List[str] = []
        for src in imgs:
            if not self._is_candidate_image(src):
                continue
            images.append(_abs_url(self.config.site_base_url, src))
        raws = re.findall(r'href="([^"]+raw\.php[^"]*)"', content_html)
        for r in raws:
            if self._is_candidate_image(r):
                images.append(_abs_url(self.config.site_base_url, r))
        # уникализируем и фильтруем запросом (до 10 валидных)
        seen = set()
        uniq_images: List[str] = []
        for img in images:
            if img not in seen:
                uniq_images.append(img)
                seen.add(img)
        filtered = await self._filter_images(uniq_images, max_results=10)
        if is_digest and filtered:
            filtered = filtered[:1]
        return {"text": text, "images": filtered, "is_digest": is_digest}

    def _is_candidate_image(self, src: str) -> bool:
        allowed_ext = (".jpg", ".jpeg", ".png", ".webp", ".gif")
        if "raw.php" in src:
            return True
        if src.lower().endswith(allowed_ext):
            pass
        else:
            return False
        if src.startswith(("http://", "https://")):
            return src.startswith(self.config.site_base_url)
        return True  # относительные ссылки на том же домене

    async def _filter_images(self, urls: List[str], max_results: int = 10) -> List[str]:
        good: List[str] = []
        checks = 0
        for url in urls:
            if len(good) >= max_results:
                break
            if checks >= max_results * 2:
                break
            checks += 1
            try:
                r = await self.client.get(url, follow_redirects=True, timeout=10.0)
                ctype = r.headers.get("content-type", "")
                if r.is_success and ctype.startswith("image"):
                    good.append(url)
                else:
                    log.warning("Skip image (status/content-type): %s %s", r.status_code, url)
            except httpx.RequestError as exc:
                log.warning("Skip image (request error): %s -> %s", url, exc)
        return good

    def _extract_main_block(self, html: str) -> str:
        """
        Пытаемся изолировать главный контент новости, чтобы не тянуть иконки/меню.
        """
        html = unescape(html)
        patterns = [
            r'<section class="container content"[^>]*>(.*?)</section>',
            r'<div class="main_col"[^>]*>(.*?)<div class="clear">',
            r'<div class="content"[^>]*>(.*?)<div class="clear">',
            r'<div class="right_col"[^>]*>(.*?)<div class="clear">',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.S)
            if m:
                return m.group(1)
        return html

    def _is_digest(self, title: Optional[str]) -> bool:
        if not title:
            return False
        return "дайджест" in title.lower()

    def _clean_text(self, text: str, title: Optional[str], is_digest: bool = False) -> str:
        if is_digest:
            text = re.sub(
                r"Юбилейные встречи выпускников.*?Группы для нашего общения",
                "",
                text,
                flags=re.S | re.I,
            )
        lines = [ln.strip() for ln in text.splitlines()]
        cleaned: List[str] = []
        title_norm = (title or "").replace("\xa0", " ").strip().lower()
        stop_phrases = {
            title_norm,
            "алumni анкетирование на выпуске выпускники",
            "фурасов владислав дмитриевич",
        }
        digest_skip = {
            "юбилейные встречи выпускников",
            "организовать встречу выпуска",
            "ef msu alumni",
            "alumni@econ.msu.ru",
            "самые свежие новости факультета",
            "экономический факультет всегда рад",
            "ваши предложения, вопросы и пожелания",
            "группы для нашего общения",
        }
        for ln in lines:
            if not ln:
                continue
            ln_norm = ln.replace("\xa0", " ").strip().lower()
            if ln_norm in stop_phrases or (title_norm and ln_norm == title_norm):
                continue
            if is_digest and any(skip in ln_norm for skip in digest_skip):
                continue
            # отбрасываем строки вида "17 ноября 2025"
            if re.fullmatch(r"\d{1,2}\s+\w+\s+\d{4}", ln_norm):
                continue
            cleaned.append(ln.replace("\xa0", " ").strip())
        return "\n".join(cleaned)
