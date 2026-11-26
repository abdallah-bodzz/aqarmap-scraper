#!/usr/bin/env python3
"""
aqarmap_scraper.py - resumable, polite scraper for Aqarmap listing pages.

Pipeline
    probe    fetch page 1 (+ one detail page), save the raw HTML under ./samples/,
             print what the parser extracted. Run this first.
    crawl    phase 1: walk the paginated result pages, store one row per listing.
    details  phase 2 (optional): open each listing page for the extra fields.
    reparse  re-run the parsers over HTML stored in the DB (no network).
    export   write CSV / XLSX / JSONL from the DB.
    status   show progress and data-quality numbers.
    areas    list sub-area URLs found on a results page (fallback if pagination is capped).

Design notes
    * SQLite (WAL) is the source of truth; every page commits, so Ctrl-C / crashes lose nothing.
    * robots.txt is fetched and enforced (wildcards, Allow/Disallow, Crawl-delay). If the
      crawl URLs are disallowed the run aborts. There is deliberately no override flag.
    * Rate limiting is global, jittered and adaptive (slows down on 429/5xx/blocks, recovers
      slowly). Repeated blocks stop the run instead of hammering the site.
    * Raw card HTML is stored (zlib), so parser fixes never need a re-crawl: use `reparse`.
    * Phone numbers are hidden behind registration on the site and are not collected.

Requires Python 3.10+.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import random
import re
import sqlite3
import sys
import time
import zlib
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser
from selectolax.lexbor import LexborNode as Node

__version__ = "1.0.0"

DEFAULT_URL = "https://aqarmap.com.eg/en/for-rent/property-type/alexandria/?location=alexandria"
DEFAULT_DB = "aqarmap_alexandria_rent.db"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}

log = logging.getLogger("aqarmap")


# --------------------------------------------------------------------------- errors
class BlockedError(RuntimeError):
    """The site is refusing us (403 / challenge page) repeatedly."""


class FetchError(RuntimeError):
    """A URL could not be fetched after all retries."""


class RobotsDisallowed(RuntimeError):
    """robots.txt forbids the URLs we were asked to crawl."""


class TransportFailure(Exception):
    """Network-level failure (DNS, TLS, timeout, reset, browser crash)."""


# --------------------------------------------------------------------------- small utils
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


_AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩٬٫", "0123456789,.")


def norm_digits(s: str) -> str:
    """Eastern Arabic digits / separators -> ASCII."""
    return s.translate(_AR_DIGITS)


def to_num(s: str | None) -> int | float | None:
    if s is None:
        return None
    s = norm_digits(str(s)).replace(",", "").strip(" .")
    try:
        v = float(s)
    except ValueError:
        return None
    return int(v) if v.is_integer() else v


def first_number(s: str | None) -> int | float | None:
    if not s:
        return None
    m = re.search(r"\d[\d,]*(?:\.\d+)?", norm_digits(s))
    return to_num(m.group(0)) if m else None


def squash(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def fmt_eta(seconds: float) -> str:
    if not math.isfinite(seconds) or seconds < 0:
        return "?"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


def safe_text(v):
    """Neutralise spreadsheet formula injection for text that came from the web."""
    if isinstance(v, str) and v[:1] in ("=", "+", "-", "@"):
        return "'" + v
    return v


def page_url(base: str, n: int) -> str:
    """Site pattern: page 1 = base, page N = ?page=N&<other params>."""
    p = urlparse(base)
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k != "page"]
    if n > 1:
        q.insert(0, ("page", str(n)))
    return urlunparse(p._replace(query=urlencode(q), fragment=""))


def source_key(url: str) -> str:
    return page_url(url, 1)


# --------------------------------------------------------------------------- robots.txt
class RobotsRules:
    """Minimal robots.txt matcher with Google-style wildcards and longest-match wins.
    (urllib.robotparser ignores '*' and '$' inside paths, which is why this exists.)"""

    def __init__(self, text: str, agent: str = "*"):
        groups: list[tuple[list[str], list[tuple[bool, str]], float | None]] = []
        agents: list[str] = []
        rules: list[tuple[bool, str]] = []
        delay: float | None = None
        last_was_agent = False
        for raw in text.splitlines():
            line = raw.split("#", 1)[0].strip()
            if ":" not in line:
                continue
            k, v = (x.strip() for x in line.split(":", 1))
            k = k.lower()
            if k == "user-agent":
                if agents and not last_was_agent:
                    groups.append((agents, rules, delay))
                    agents, rules, delay = [], [], None
                agents.append(v.lower())
                last_was_agent = True
                continue
            last_was_agent = False
            if k in ("allow", "disallow"):
                rules.append((k == "allow", v))
            elif k == "crawl-delay":
                try:
                    delay = float(v)
                except ValueError:
                    pass
        if agents:
            groups.append((agents, rules, delay))

        chosen = [g for g in groups if agent.lower() in g[0]] or [g for g in groups if "*" in g[0]]
        self.crawl_delay: float | None = None
        self.raw: list[str] = []
        self._rules: list[tuple[int, bool, re.Pattern]] = []
        for _, grp_rules, grp_delay in chosen:
            if grp_delay is not None:
                self.crawl_delay = max(self.crawl_delay or 0.0, grp_delay)
            for is_allow, pattern in grp_rules:
                if not pattern:  # "Disallow:" (empty) allows everything
                    continue
                self.raw.append(f"{'Allow' if is_allow else 'Disallow'}: {pattern}")
                end = pattern.endswith("$")
                body = pattern[:-1] if end else pattern
                rx = "^" + re.escape(body).replace(r"\*", ".*") + ("$" if end else "")
                self._rules.append((len(pattern), is_allow, re.compile(rx)))

    def allowed(self, url: str) -> bool:
        p = urlparse(url)
        target = p.path + (f"?{p.query}" if p.query else "")
        best: tuple[int, bool] | None = None
        for length, is_allow, rx in self._rules:
            if rx.match(target) and (best is None or length > best[0] or (length == best[0] and is_allow)):
                best = (length, is_allow)
        return True if best is None else best[1]


# --------------------------------------------------------------------------- parsing: result pages
LISTING_HREF_RE = re.compile(r"/listing/(\d+)")
PRICE_RE = re.compile(r"(?:EGP|جنيه|ج\.م)\W{0,4}([\d,\.]+\d)|([\d,\.]*\d)\W{0,4}(?:EGP|جنيه|ج\.م)", re.I)
AREA_RE = re.compile(r"([\d,\.]*\d)\s*(?:m²|m2|sqm|sq\.?\s?m|م²|م2|متر)", re.I)
BEDS_RE = re.compile(r"(\d{1,2})\s*(?:beds?|bedrooms?|br\b|غرف|غرفة)", re.I)
BATHS_RE = re.compile(r"(\d{1,2})\s*(?:baths?|bathrooms?|حمام)", re.I)
DAILY_RE = re.compile(r"daily|per\s*day|/\s*day|\bnight|يومي|ليلة", re.I)
TOTAL_RE = re.compile(r"([\d,]{2,})\s+(?:Propert(?:y|ies)|عقار)", re.I)
MAX_CARD_CHARS = 2500


@dataclass
class Card:
    id: int
    url: str
    url_slug: str | None
    title: str | None
    property_type: str | None
    price: float | None
    daily_price: float | None
    area_sqm: float | None
    beds: int | None
    baths: int | None
    location: str | None
    image_url: str | None
    card_text: str
    card_html: str


@dataclass
class PageResult:
    cards: list[Card]
    total_results: int | None


def _listing_id(href: str | None) -> int | None:
    m = LISTING_HREF_RE.search(href or "")
    return int(m.group(1)) if m else None


def _anchors_in(node: Node) -> list[Node]:
    out = node.css("a")
    if node.tag == "a":
        out = [node] + out
    return out


def _ids_in(node: Node) -> set[int]:
    return {i for a in _anchors_in(node) if (i := _listing_id(a.attributes.get("href"))) is not None}


def _find_attr_number(card: Node, keyword: str) -> int | None:
    """Look for an element whose class/alt/title/aria-* mentions `keyword` (e.g. 'bed') and
    read the small number sitting in it, next to it, or in its parent (icon + number)."""
    for el in card.css("*"):
        attrs = " ".join(str(v) for v in el.attributes.values() if v).lower()
        if keyword not in attrs:
            continue
        cands = [el.text(strip=True)]
        sib = el.next
        for _ in range(3):
            if sib is None:
                break
            cands.append(sib.text(strip=True))
            sib = sib.next
        if el.parent is not None:
            cands.append(el.parent.text(strip=True))
        for c in cands:
            m = re.fullmatch(r"\D{0,12}(\d{1,2})\D{0,12}", norm_digits(c or ""))
            if m:
                return int(m.group(1))
    return None


def _prices(text: str) -> tuple[float | None, float | None]:
    matches = list(PRICE_RE.finditer(text))
    monthly = daily = None
    for i, m in enumerate(matches):
        amount = to_num((m.group(1) or m.group(2)))
        if amount is None:
            continue
        prev_end = matches[i - 1].end() if i else 0
        next_start = matches[i + 1].start() if i + 1 < len(matches) else m.end() + 30
        window = text[max(prev_end, m.start() - 15):m.start()] + " " + text[m.end():min(next_start, m.end() + 30)]
        if DAILY_RE.search(window):
            daily = daily if daily is not None else float(amount)
        else:
            monthly = monthly if monthly is not None else float(amount)
    return monthly, daily


def parse_card(node: Node, listing_id: int, base_url: str) -> Card:
    anchors = _anchors_in(node)
    listing_anchors = [a for a in anchors if _listing_id(a.attributes.get("href")) == listing_id]
    href = listing_anchors[0].attributes.get("href") if listing_anchors else f"/en/listing/{listing_id}/"
    url = urlunparse(urlparse(urljoin(base_url, href))._replace(query="", fragment=""))
    slug_m = re.search(r"/listing/\d+-(?:for-rent|for-sale)-?(.*?)/?$", urlparse(url).path)
    url_slug = slug_m.group(1) or None if slug_m else None

    text_spaced = squash(node.text(separator=" ", strip=True))
    text_piped = node.text(separator=" | ", strip=True)
    text_norm = norm_digits(text_spaced)

    title = None
    for h in node.css("h1,h2,h3,h4"):
        t = squash(h.text(strip=True))
        if t:
            title = t
            break
    if not title:
        for a in listing_anchors:
            t = squash(a.attributes.get("title") or a.attributes.get("aria-label") or "")
            if t:
                title = t
                break
    if not title:
        for a in listing_anchors:
            t = squash(a.text(strip=True))
            if len(t) > 10:
                title = t
                break
    ptype = None
    if title:
        m = re.match(r"\s*(.+?)\s+for\s+(?:rent|sale)\b", title, re.I)
        ptype = m.group(1) if m else None

    monthly, daily = _prices(text_norm)
    am = AREA_RE.search(text_norm)
    area = float(to_num(am.group(1))) if am and to_num(am.group(1)) is not None else None

    beds = _find_attr_number(node, "bed")
    baths = _find_attr_number(node, "bath")
    if beds is None and (m := BEDS_RE.search(text_norm)):
        beds = int(m.group(1))
    if baths is None and (m := BATHS_RE.search(text_norm)):
        baths = int(m.group(1))

    crumbs = []
    for a in anchors:
        h = a.attributes.get("href") or ""
        if "/for-rent/" in h and "/listing/" not in h:
            t = squash(a.text(strip=True))
            if t and t not in crumbs:
                crumbs.append(t)
    location = " > ".join(crumbs) or None

    img = node.css_first("img")
    image_url = None
    if img is not None:
        src = img.attributes.get("data-src") or img.attributes.get("src") or ""
        if src and not src.startswith("data:"):
            image_url = urljoin(base_url, src)

    return Card(
        id=listing_id, url=url, url_slug=url_slug, title=title, property_type=ptype,
        price=monthly, daily_price=daily, area_sqm=area,
        beds=beds, baths=baths, location=location, image_url=image_url,
        card_text=text_piped, card_html=node.html or "",
    )


def parse_listing_page(html: str, page_url_: str) -> PageResult:
    doc = HTMLParser(html)
    seen: dict[int, Node] = {}
    for a in doc.css("a[href*='/listing/']"):
        lid = _listing_id(a.attributes.get("href"))
        if lid is None or lid in seen:
            continue
        node = a
        while node.parent is not None and node.parent.tag not in ("body", "html", "-undef"):
            parent = node.parent
            if len(_ids_in(parent)) > 1 or len(parent.text(strip=True)) > MAX_CARD_CHARS:
                break
            node = parent
        seen[lid] = node
    cards = [parse_card(node, lid, page_url_) for lid, node in seen.items()]
    total = None
    body_text = norm_digits(doc.body.text(separator=" ", strip=True)) if doc.body else ""
    for m in TOTAL_RE.finditer(body_text[:20000]):
        v = to_num(m.group(1))
        if v and v >= 1:
            total = int(v)
            break
    return PageResult(cards=cards, total_results=total)


def parse_card_fragment(card_html: str, listing_id: int, base_url: str) -> Card | None:
    doc = HTMLParser(card_html)
    root = doc.body.child if doc.body else None
    while root is not None and root.tag == "-text":
        root = root.next
    return parse_card(root, listing_id, base_url) if root is not None else None


def looks_like_challenge(text: str) -> bool:
    if len(text) > 60_000:
        return False
    t = text[:8000].lower()
    markers = ("just a moment", "cf-browser-verification", "/cdn-cgi/challenge-platform",
               "attention required! | cloudflare", "px-captcha", "enable javascript and cookies",
               "access denied", "verify you are human", "checking your browser")
    return any(m in t for m in markers)


# --------------------------------------------------------------------------- parsing: detail pages
DETAIL_LABELS: dict[str, list[str]] = {
    "posted_date": ["date posted", "posted", "posted on", "listed on", "تاريخ النشر"],
    "updated_date": ["date updated", "updated", "last updated", "تاريخ التحديث"],
    "area_sqm": ["built-up area", "built up area", "area", "size", "المساحة"],
    "beds": ["bedrooms", "bed rooms", "beds", "غرف النوم", "غرف"],
    "baths": ["bathrooms", "baths", "bath", "الحمامات"],
    "floor": ["floor", "الدور"],
    "view": ["view", "الإطلالة"],
    "year_built": ["year built", "built year", "سنة البناء"],
    "finishing": ["finishing", "finish", "التشطيب"],
    "furnished": ["furnished", "furnishing", "مفروش"],
    "seller_type": ["seller type", "seller", "advertiser type", "نوع المعلن"],
    "payment": ["payment method", "payment terms", "payment", "طريقة الدفع"],
}
_ALL_LABELS = {s for syns in DETAIL_LABELS.values() for s in syns} | {"reception", "reception rooms", "listing id", "property id"}
_DATE_FORMATS = ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y")


def norm_label(s: str) -> str:
    return re.sub(r"[^a-z0-9؀-ۿ \-]", "", s.lower()).strip()


def parse_date(s: str | None) -> str | None:
    if not s:
        return None
    s = squash(norm_digits(s))
    for f in _DATE_FORMATS:
        try:
            return datetime.strptime(s, f).date().isoformat()
        except ValueError:
            continue
    return s  # keep the raw string rather than lose it


def pairs_from_lines(lines: list[str]) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for i, line in enumerate(lines):
        if ":" in line and len(line) < 140:
            label, val = (x.strip() for x in line.split(":", 1))
            if val and 0 < len(label) < 40:
                pairs.setdefault(norm_label(label), val)
        nl = norm_label(line)
        if nl in _ALL_LABELS and i + 1 < len(lines):
            nxt = lines[i + 1]
            if norm_label(nxt) not in _ALL_LABELS and len(nxt) < 200:
                pairs.setdefault(nl, nxt)
    return pairs


def _walk(obj, keys: set[str]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and not isinstance(v, (dict, list)):
                yield k, v
            else:
                yield from _walk(v, keys)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v, keys)


def parse_detail(html: str, url: str) -> dict:
    doc = HTMLParser(html)
    out: dict = {}

    h1 = doc.css_first("h1")
    full_title = squash(h1.text(strip=True)) if h1 is not None else None
    if not full_title:
        t = doc.css_first("title")
        full_title = squash(t.text(strip=True)) if t is not None else None
    out["full_title"] = full_title

    # coordinates: JSON-LD first, then any inline "latitude"/"longitude" pair
    lat = lng = None
    for s in doc.css('script[type="application/ld+json"]'):
        try:
            data = json.loads(s.text())
        except (ValueError, TypeError):
            continue
        for k, v in _walk(data, {"latitude", "longitude"}):
            num = to_num(v)
            if k == "latitude" and lat is None:
                lat = num
            elif k == "longitude" and lng is None:
                lng = num
    if lat is None or lng is None:
        mlat = re.search(r'"lat(?:itude)?"\s*:\s*"?(-?\d{1,3}\.\d+)', html)
        mlng = re.search(r'"(?:lng|lon|longitude)"\s*:\s*"?(-?\d{1,3}\.\d+)', html)
        if mlat and mlng:
            lat, lng = to_num(mlat.group(1)), to_num(mlng.group(1))
    out["lat"], out["lng"] = lat, lng

    # images
    imgs: list[str] = []
    og = doc.css_first('meta[property="og:image"]')
    if og is not None and og.attributes.get("content"):
        imgs.append(urljoin(url, og.attributes["content"]))
    for img in doc.css("img"):
        src = img.attributes.get("data-src") or img.attributes.get("src") or ""
        if not src or src.startswith("data:"):
            continue
        low = src.lower()
        if any(x in low for x in ("logo", "avatar", "icon", "sprite", "placeholder")):
            continue
        imgs.append(urljoin(url, src))
    imgs = list(dict.fromkeys(imgs))[:40]
    out["images"] = json.dumps(imgs) if imgs else None
    out["image_count"] = len(imgs)

    # key/value pairs from tables, definition lists, and plain text lines
    pairs: dict[str, str] = {}
    for tr in doc.css("tr"):
        cells = [squash(c.text(strip=True)) for c in tr.css("th,td")]
        if len(cells) == 2 and cells[0] and cells[1]:
            pairs.setdefault(norm_label(cells[0]), cells[1])
    for dt in doc.css("dt"):
        dd = dt.next
        while dd is not None and dd.tag != "dd":
            dd = dd.next
        if dd is not None:
            k, v = squash(dt.text(strip=True)), squash(dd.text(strip=True))
            if k and v:
                pairs.setdefault(norm_label(k), v)
    for n in doc.css("script,style,noscript,svg,nav,footer"):
        n.decompose()
    text = doc.body.text(separator="\n", strip=True) if doc.body else ""
    lines = [squash(x) for x in text.split("\n") if x.strip()]
    for k, v in pairs_from_lines(lines).items():
        pairs.setdefault(k, v)

    out.update(map_detail_pairs(pairs))
    out["attrs_json"] = json.dumps(pairs, ensure_ascii=False) if pairs else None
    out["detail_text"] = "\n".join(lines)[:60_000]
    return out


def map_detail_pairs(pairs: dict[str, str]) -> dict:
    out: dict = {}
    for col, syns in DETAIL_LABELS.items():
        val = next((pairs[s] for s in syns if s in pairs), None)
        if val is None:
            continue
        if col in ("area_sqm",):
            out[col] = first_number(val)
        elif col in ("beds", "baths"):
            n = first_number(val)
            out[col] = int(n) if n is not None else None
        elif col in ("posted_date", "updated_date"):
            out[col] = parse_date(val)
        else:
            out[col] = val[:200]
    return out


def reparse_detail_from_text(detail_text: str) -> dict:
    lines = [x for x in detail_text.split("\n") if x.strip()]
    return map_detail_pairs(pairs_from_lines(lines))


# --------------------------------------------------------------------------- storage
SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
  id INTEGER PRIMARY KEY,
  url TEXT NOT NULL, url_slug TEXT,
  title TEXT, property_type TEXT,
  price REAL, daily_price REAL, currency TEXT DEFAULT 'EGP',
  area_sqm REAL, beds INTEGER, baths INTEGER,
  location TEXT, image_url TEXT,
  source TEXT, page INTEGER,
  first_seen TEXT, last_seen TEXT,
  card_text TEXT, card_html BLOB,
  detail_status TEXT, detail_fetched_at TEXT,
  full_title TEXT, posted_date TEXT, updated_date TEXT, floor TEXT, view TEXT,
  year_built TEXT, finishing TEXT, furnished TEXT, seller_type TEXT, payment TEXT,
  lat REAL, lng REAL, image_count INTEGER, images TEXT,
  attrs_json TEXT, detail_text BLOB
);
CREATE INDEX IF NOT EXISTS idx_listings_detail ON listings(detail_status);
CREATE TABLE IF NOT EXISTS pages (
  source TEXT NOT NULL, page INTEGER NOT NULL, status TEXT NOT NULL,
  n_cards INTEGER, ids TEXT, fetched_at TEXT, error TEXT,
  PRIMARY KEY (source, page)
);
CREATE TABLE IF NOT EXISTS price_history (id INTEGER NOT NULL, seen_at TEXT NOT NULL, price REAL NOT NULL);
CREATE INDEX IF NOT EXISTS idx_price_history_id ON price_history(id);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""

UPSERT_CARD = """
INSERT INTO listings (id,url,url_slug,title,property_type,price,daily_price,area_sqm,beds,baths,
                      location,image_url,source,page,first_seen,last_seen,card_text,card_html)
VALUES (:id,:url,:url_slug,:title,:property_type,:price,:daily_price,:area_sqm,:beds,:baths,
        :location,:image_url,:source,:page,:ts,:ts,:card_text,:card_html)
ON CONFLICT(id) DO UPDATE SET
  url=excluded.url,
  url_slug=COALESCE(excluded.url_slug,url_slug),
  title=COALESCE(excluded.title,title),
  property_type=COALESCE(excluded.property_type,property_type),
  price=COALESCE(excluded.price,price),
  daily_price=COALESCE(excluded.daily_price,daily_price),
  area_sqm=COALESCE(excluded.area_sqm,area_sqm),
  beds=COALESCE(excluded.beds,beds),
  baths=COALESCE(excluded.baths,baths),
  location=COALESCE(excluded.location,location),
  image_url=COALESCE(excluded.image_url,image_url),
  source=excluded.source, page=excluded.page, last_seen=excluded.last_seen,
  card_text=excluded.card_text, card_html=excluded.card_html
"""

DETAIL_COLS = ["full_title", "posted_date", "updated_date", "area_sqm", "beds", "baths", "floor", "view",
               "year_built", "finishing", "furnished", "seller_type", "payment", "lat", "lng",
               "image_count", "images", "attrs_json"]


class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self):
        self.conn.commit()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    # -- meta
    def meta_get(self, key: str) -> str | None:
        r = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

    def meta_set(self, key: str, value) -> None:
        self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                          (key, str(value)))

    # -- pages
    def page_row(self, source: str, page: int):
        return self.conn.execute("SELECT * FROM pages WHERE source=? AND page=?", (source, page)).fetchone()

    def record_page(self, source: str, page: int, status: str, n_cards: int, ids: list[int] | None, error: str | None = None):
        self.conn.execute(
            "INSERT INTO pages(source,page,status,n_cards,ids,fetched_at,error) VALUES(?,?,?,?,?,?,?) "
            "ON CONFLICT(source,page) DO UPDATE SET status=excluded.status,n_cards=excluded.n_cards,"
            "ids=excluded.ids,fetched_at=excluded.fetched_at,error=excluded.error",
            (source, page, status, n_cards, json.dumps(ids) if ids is not None else None, now_iso(), error))

    # -- listings
    def upsert_cards(self, source: str, page: int, cards: list[Card]) -> int:
        ts = now_iso()
        new = 0
        for c in cards:
            row = self.conn.execute("SELECT price FROM listings WHERE id=?", (c.id,)).fetchone()
            if row is None:
                new += 1
                if c.price is not None:
                    self.conn.execute("INSERT INTO price_history VALUES(?,?,?)", (c.id, ts, c.price))
            elif c.price is not None and row["price"] != c.price:
                self.conn.execute("INSERT INTO price_history VALUES(?,?,?)", (c.id, ts, c.price))
            d = asdict(c)
            d["card_html"] = zlib.compress(c.card_html.encode("utf-8"), 6)
            d.update(source=source, page=page, ts=ts)
            self.conn.execute(UPSERT_CARD, d)
        return new

    def pending_detail_ids(self, limit: int | None, retry_errors: bool, redo: bool) -> list[tuple[int, str]]:
        if redo:
            cond = "1=1"
        elif retry_errors:
            cond = "detail_status IS NULL OR detail_status='error'"
        else:
            cond = "detail_status IS NULL"
        sql = f"SELECT id,url FROM listings WHERE {cond} ORDER BY id DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [(r["id"], r["url"]) for r in self.conn.execute(sql)]

    def mark_detail(self, lid: int, status: str):
        self.conn.execute("UPDATE listings SET detail_status=?, detail_fetched_at=? WHERE id=?", (status, now_iso(), lid))

    def save_detail(self, lid: int, d: dict):
        sets = ["detail_status='ok'", "detail_fetched_at=?", "detail_text=?"]
        vals: list = [now_iso(), zlib.compress((d.get("detail_text") or "").encode("utf-8"), 6)]
        for col in DETAIL_COLS:
            if col in d and d[col] is not None:
                sets.append(f"{col}=?")
                vals.append(d[col])
        vals.append(lid)
        self.conn.execute(f"UPDATE listings SET {', '.join(sets)} WHERE id=?", vals)

    def stats(self) -> dict:
        q = lambda sql: self.conn.execute(sql).fetchone()[0]  # noqa: E731
        total = q("SELECT COUNT(*) FROM listings")
        return {
            "listings": total,
            "with_price": q("SELECT COUNT(*) FROM listings WHERE price IS NOT NULL"),
            "with_area": q("SELECT COUNT(*) FROM listings WHERE area_sqm IS NOT NULL"),
            "with_beds": q("SELECT COUNT(*) FROM listings WHERE beds IS NOT NULL"),
            "with_baths": q("SELECT COUNT(*) FROM listings WHERE baths IS NOT NULL"),
            "with_location": q("SELECT COUNT(*) FROM listings WHERE location IS NOT NULL"),
            "with_title": q("SELECT COUNT(*) FROM listings WHERE title IS NOT NULL"),
            "detail_ok": q("SELECT COUNT(*) FROM listings WHERE detail_status='ok'"),
            "detail_gone": q("SELECT COUNT(*) FROM listings WHERE detail_status='gone'"),
            "detail_error": q("SELECT COUNT(*) FROM listings WHERE detail_status='error'"),
            "detail_pending": q("SELECT COUNT(*) FROM listings WHERE detail_status IS NULL"),
            "pages_ok": q("SELECT COUNT(*) FROM pages WHERE status='ok'"),
            "pages_error": q("SELECT COUNT(*) FROM pages WHERE status='error'"),
            "price_changes": q("SELECT COUNT(*) FROM (SELECT id FROM price_history GROUP BY id HAVING COUNT(*)>1)"),
        }


# --------------------------------------------------------------------------- networking
@dataclass
class Fetched:
    status: int
    text: str
    url: str
    retry_after: float | None = None


class RateLimiter:
    """Global request pacing with jitter. Slows down when penalised, recovers slowly."""

    def __init__(self, rps: float):
        self.base = 1.0 / max(rps, 0.01)
        self.interval = self.base
        self.max_interval = max(self.base * 16, 30.0)
        self._next = 0.0
        self._lock = asyncio.Lock()

    def raise_floor(self, seconds: float):
        self.base = max(self.base, seconds)
        self.interval = max(self.interval, self.base)
        self.max_interval = max(self.max_interval, self.base * 16)

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + self.interval * random.uniform(0.85, 1.35)
        if start > now:
            await asyncio.sleep(start - now)

    def penalize(self):
        self.interval = min(self.interval * 2, self.max_interval)

    def reward(self):
        self.interval = max(self.base, self.interval * 0.95)


class HttpxFetcher:
    def __init__(self, args):
        kw = dict(
            headers={
                "User-Agent": args.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            },
            timeout=httpx.Timeout(args.timeout, connect=15.0),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=args.concurrency + 2, max_keepalive_connections=args.concurrency + 2),
        )
        if args.proxy:
            kw["proxy"] = args.proxy
        try:
            import h2  # noqa: F401
            kw["http2"] = True
        except ImportError:
            pass
        self.client = httpx.AsyncClient(**kw)

    async def get(self, url: str) -> Fetched:
        try:
            r = await self.client.get(url)
        except httpx.TransportError as e:
            raise TransportFailure(f"{type(e).__name__}: {e}") from e
        ra = None
        h = r.headers.get("retry-after")
        if h:
            try:
                ra = float(h)
            except ValueError:
                ra = None
        return Fetched(r.status_code, r.text, str(r.url), ra)

    async def close(self):
        await self.client.aclose()


class PlaywrightFetcher:
    """Optional fallback if plain HTTP is blocked: `pip install playwright && playwright install chromium`.
    NOTE: written to the Playwright API but not exercised in the author's test environment."""

    def __init__(self, args):
        self.args = args
        self._pw = self._browser = self._ctx = None

    async def _start(self):
        if self._ctx is not None:
            return
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise SystemExit("Playwright not installed: pip install playwright && playwright install chromium") from e
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=True)
        opts = {"user_agent": self.args.user_agent, "locale": "en-US"}
        if self.args.proxy:
            opts["proxy"] = {"server": self.args.proxy}
        self._ctx = await self._browser.new_context(**opts)

    async def get(self, url: str) -> Fetched:
        await self._start()
        page = await self._ctx.new_page()
        try:
            resp = await page.goto(url, wait_until="domcontentloaded", timeout=int(self.args.timeout * 1000))
            await page.wait_for_timeout(1200)
            return Fetched(resp.status if resp else 0, await page.content(), page.url)
        except Exception as e:  # noqa: BLE001 - playwright raises its own hierarchy
            raise TransportFailure(f"{type(e).__name__}: {e}") from e
        finally:
            await page.close()

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()


class Engine:
    def __init__(self, args, db: DB):
        self.args = args
        self.db = db
        self.fetcher = PlaywrightFetcher(args) if args.fetcher == "playwright" else HttpxFetcher(args)
        self.limiter = RateLimiter(args.rps)
        self.consec_blocks = 0
        self.stop = False

    async def close(self):
        await self.fetcher.close()

    async def _backoff(self, attempt: int, retry_after: float | None, why: str, url: str, base: float | None = None):
        base = base if base is not None else self.args.backoff_base
        delay = retry_after if retry_after is not None else min(180.0, base * 2 ** attempt) * random.uniform(0.7, 1.3)
        delay = min(delay, 300.0)
        log.warning("retry %d/%d in %.1fs: %s  (%s)", attempt + 1, self.args.max_retries, delay, why, url)
        await asyncio.sleep(delay)

    async def fetch(self, url: str) -> Fetched:
        last = "n/a"
        for attempt in range(self.args.max_retries + 1):
            await self.limiter.wait()
            try:
                r = await self.fetcher.get(url)
            except (TransportFailure, asyncio.TimeoutError) as e:
                last = str(e) or type(e).__name__
                self.limiter.penalize()
                if attempt < self.args.max_retries:
                    await self._backoff(attempt, None, last, url)
                continue
            blocked = r.status in (401, 403) or (r.status in (200, 429, 503) and looks_like_challenge(r.text))
            if blocked:
                self.consec_blocks += 1
                self.limiter.penalize()
                last = f"blocked (HTTP {r.status})"
                if self.consec_blocks >= self.args.block_limit:
                    raise BlockedError(
                        f"{self.consec_blocks} consecutive blocked responses (last: HTTP {r.status}) at {url}")
                if attempt < self.args.max_retries:
                    await self._backoff(attempt, r.retry_after, last, url, base=self.args.backoff_base * 5)
                continue
            if r.status in RETRY_STATUS:
                last = f"HTTP {r.status}"
                self.limiter.penalize()
                if attempt < self.args.max_retries:
                    await self._backoff(attempt, r.retry_after, last, url)
                continue
            self.consec_blocks = 0
            if r.status == 200:
                self.limiter.reward()
            return r
        raise FetchError(f"{url}: gave up after {self.args.max_retries + 1} attempts ({last})")

    async def ensure_robots(self, urls: list[str]) -> None:
        origin = "{0.scheme}://{0.netloc}".format(urlparse(urls[0]))
        r = await self.fetch(origin + "/robots.txt")
        if r.status == 404:
            log.info("robots.txt: none (404), nothing to enforce")
            return
        if r.status != 200 or "<html" in r.text[:500].lower():
            log.warning("robots.txt could not be read (HTTP %s). Check %s/robots.txt manually before a full run.",
                        r.status, origin)
            return
        rules = RobotsRules(r.text)
        if rules.crawl_delay:
            log.info("robots.txt Crawl-delay: %.1fs, honouring it", rules.crawl_delay)
            self.limiter.raise_floor(rules.crawl_delay)
        bad = [u for u in urls if not rules.allowed(u)]
        if bad:
            log.error("robots.txt disallows:\n  %s", "\n  ".join(bad))
            log.error("Applicable rules:\n  %s", "\n  ".join(rules.raw) or "(none)")
            raise RobotsDisallowed("robots.txt forbids the requested URLs; not crawling.")
        log.info("robots.txt OK for %d checked URL pattern(s) (%d rule lines applied)", len(urls), len(rules.raw))


# --------------------------------------------------------------------------- crawl (phase 1)
class Crawler:
    def __init__(self, eng: Engine):
        self.eng = eng
        self.db = eng.db
        self.args = eng.args

    async def crawl_source(self, source: str) -> dict:
        db, args = self.db, self.args
        total = int(db.meta_get(f"total:{source}") or 0) or None
        per_page = int(db.meta_get(f"per_page:{source}") or 0) or None
        first_row = db.page_row(source, 1)
        first_ids = set(json.loads(first_row["ids"])) if first_row and first_row["ids"] else None
        n, new_total, pages_done, errors_in_row = 1, 0, 0, 0
        started = time.monotonic()
        stopped_reason = "reached max pages"

        while True:
            limit = args.max_pages
            if total and per_page:
                limit = min(limit, math.ceil(total / per_page) + 5)
            if n > limit:
                stopped_reason = "reached expected last page" if total else "reached --max-pages"
                break
            row = db.page_row(source, n)
            if row and row["status"] == "ok" and not args.refresh:
                n += 1
                continue

            url = page_url(source, n)
            res = await self.eng.fetch(url)
            if res.status in (404, 410) and n > 1:
                db.record_page(source, n, "empty", 0, [], f"http {res.status}")
                stopped_reason = f"HTTP {res.status} at page {n} (end of results)"
                break
            if res.status != 200:
                db.record_page(source, n, "error", 0, None, f"http {res.status}")
                db.commit()
                errors_in_row += 1
                log.error("page %d: HTTP %d", n, res.status)
                if errors_in_row >= 3:
                    stopped_reason = "3 consecutive page errors"
                    break
                n += 1
                continue

            pr = parse_listing_page(res.text, url)
            if n == 1:
                if pr.total_results:
                    total = pr.total_results
                    db.meta_set(f"total:{source}", total)
                if pr.cards:
                    per_page = len(pr.cards)
                    db.meta_set(f"per_page:{source}", per_page)

            if not pr.cards:
                expected_more = bool(total and per_page and n <= math.ceil(total / per_page))
                if expected_more:
                    for k in range(2):  # maybe a transient empty render; retry before concluding
                        log.warning("page %d empty but ~%s expected; re-checking (%d/2)", n, math.ceil(total / per_page), k + 1)
                        await asyncio.sleep(args.backoff_base * 1.5)
                        res = await self.eng.fetch(url)
                        pr = parse_listing_page(res.text, url)
                        if pr.cards:
                            break
                if not pr.cards:
                    db.record_page(source, n, "empty", 0, [])
                    db.commit()
                    if n == 1:
                        raise FetchError(
                            "page 1 parsed to 0 listings. Run `probe` and check samples/page1.html "
                            "(layout change, or a block page that looks like HTTP 200).")
                    if expected_more:
                        log.warning("results ended at page %d but ~%d pages were expected -> probable pagination cap. "
                                    "Use `areas` + `crawl --url-file` to shard by area.", n, math.ceil(total / per_page))
                    stopped_reason = f"empty page {n}"
                    break

            ids = [c.id for c in pr.cards]
            if n > 1 and first_ids and set(ids) == first_ids:
                db.record_page(source, n, "wrap", len(ids), ids)
                db.commit()
                log.warning("page %d returned the same listings as page 1 (pagination wraps).", n)
                stopped_reason = f"pagination wraps at page {n}"
                break

            new = db.upsert_cards(source, n, pr.cards)
            db.record_page(source, n, "ok", len(ids), ids)
            db.commit()
            if n == 1:
                first_ids = set(ids)
            errors_in_row = 0
            new_total += new
            pages_done += 1
            elapsed = time.monotonic() - started
            rate = pages_done / elapsed if elapsed else 0
            remaining = (limit - n) / rate if rate and limit != args.max_pages else float("nan")
            log.info("page %d/%s  cards=%d new=%d db=%d  eta=%s", n, limit if limit != args.max_pages else "?",
                     len(ids), new, db.stats()["listings"], fmt_eta(remaining))
            n += 1

        found = db.conn.execute("SELECT COUNT(*) FROM listings WHERE source=?", (source,)).fetchone()[0]
        return {"source": source, "stopped": stopped_reason, "new": new_total, "in_db": found, "site_total": total}


# --------------------------------------------------------------------------- details (phase 2)
class DetailRunner:
    def __init__(self, eng: Engine):
        self.eng = eng
        self.db = eng.db
        self.args = eng.args

    async def run(self) -> dict:
        todo = self.db.pending_detail_ids(self.args.limit, self.args.retry_errors, self.args.redo)
        stats = {"ok": 0, "gone": 0, "error": 0, "todo": len(todo)}
        if not todo:
            return stats
        queue: asyncio.Queue = asyncio.Queue()
        for item in todo:
            queue.put_nowait(item)
        started = time.monotonic()
        blocked: list[BlockedError] = []
        empty_streak = 0
        warned = False

        async def worker():
            nonlocal empty_streak, warned
            while not self.eng.stop:
                try:
                    lid, url = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    r = await self.eng.fetch(url)
                except BlockedError as e:
                    blocked.append(e)
                    self.eng.stop = True
                    return
                except FetchError as e:
                    log.error("detail %d: %s", lid, e)
                    self.db.mark_detail(lid, "error")
                    stats["error"] += 1
                    continue
                final_path = urlparse(r.url).path
                if r.status in (404, 410) or (r.status == 200 and "/listing/" not in final_path):
                    self.db.mark_detail(lid, "gone")
                    stats["gone"] += 1
                elif r.status == 200:
                    d = parse_detail(r.text, url)
                    self.db.save_detail(lid, d)
                    stats["ok"] += 1
                    mapped = any(d.get(c) is not None for c in DETAIL_LABELS)
                    empty_streak = 0 if mapped else empty_streak + 1
                    if empty_streak >= 20 and not warned:
                        warned = True
                        log.warning("20 detail pages in a row matched none of the known labels. The detail parser needs "
                                    "tuning: run `probe` and send samples/detail.html.")
                else:
                    log.error("detail %d: HTTP %d", lid, r.status)
                    self.db.mark_detail(lid, "error")
                    stats["error"] += 1
                done = stats["ok"] + stats["gone"] + stats["error"]
                if done % 25 == 0:
                    self.db.commit()
                    rate = done / (time.monotonic() - started)
                    log.info("details %d/%d  ok=%d gone=%d err=%d  eta=%s", done, stats["todo"], stats["ok"],
                             stats["gone"], stats["error"], fmt_eta((stats["todo"] - done) / rate if rate else float("nan")))

        try:
            await asyncio.gather(*[worker() for _ in range(max(1, self.args.concurrency))])
        finally:
            self.db.commit()
        if blocked:
            raise blocked[0]
        return stats


# --------------------------------------------------------------------------- export
EXPORT_COLUMNS = [
    ("id", "ID"), ("url", "URL"), ("title", "Title"), ("property_type", "Type"),
    ("price", "Price (EGP/month)"), ("daily_price", "Daily price (EGP)"), ("area_sqm", "Area (m²)"),
    ("price_per_sqm", "Price per m²"), ("beds", "Beds"), ("baths", "Baths"), ("location", "Location"),
    ("posted_date", "Posted"), ("updated_date", "Updated"), ("floor", "Floor"), ("finishing", "Finishing"),
    ("furnished", "Furnished"), ("view", "View"), ("year_built", "Year built"), ("seller_type", "Seller type"),
    ("payment", "Payment"), ("lat", "Lat"), ("lng", "Lng"), ("image_count", "Photos"), ("images", "Image URLs"),
    ("first_seen", "First seen"), ("last_seen", "Last seen"), ("detail_status", "Detail status"),
]
TEXT_COLS = {"title", "property_type", "location", "floor", "finishing", "furnished", "view", "seller_type",
             "payment", "year_built", "detail_text"}


def load_export_rows(db: DB, with_text: bool) -> list[dict]:
    base_cols = [c for c, _ in EXPORT_COLUMNS if c != "price_per_sqm"]
    sel = ", ".join(base_cols + (["detail_text"] if with_text else []))
    rows = []
    for r in db.conn.execute(f"SELECT {sel} FROM listings ORDER BY id"):
        d = dict(r)
        d["price_per_sqm"] = round(d["price"] / d["area_sqm"], 1) if d.get("price") and d.get("area_sqm") else None
        if d.get("images"):
            try:
                d["images"] = json.loads(d["images"])
            except ValueError:
                d["images"] = [d["images"]]
        else:
            d["images"] = []
        if with_text:
            blob = d.get("detail_text")
            d["detail_text"] = zlib.decompress(blob).decode("utf-8")[:32000] if blob else None
        rows.append(d)
    return rows


def export_csv(rows: list[dict], path: Path, with_text: bool):
    cols = EXPORT_COLUMNS + ([("detail_text", "Detail text")] if with_text else [])
    with open(path, "w", newline="", encoding="utf-8-sig") as f:  # BOM so Excel reads Arabic correctly
        w = csv.writer(f)
        w.writerow([h for _, h in cols])
        for r in rows:
            out = []
            for c, _ in cols:
                v = r.get(c)
                if c == "images":
                    v = " | ".join(v)
                out.append(safe_text(v) if c in TEXT_COLS else v)
            w.writerow(out)


def export_jsonl(rows: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def export_xlsx(rows: list[dict], path: Path, stats: dict, with_text: bool):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    cols = EXPORT_COLUMNS + ([("detail_text", "Detail text")] if with_text else [])
    wb = Workbook()
    ws = wb.active
    ws.title = "Listings"
    ws.append([h for _, h in cols])
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1F3A5F")
        cell.alignment = Alignment(vertical="center")
    idx = {c: i + 1 for i, (c, _) in enumerate(cols)}
    for r in rows:
        line = []
        for c, _ in cols:
            v = r.get(c)
            if c == "images":
                v = " | ".join(v)
            line.append(safe_text(v) if c in TEXT_COLS else v)
        ws.append(line)
        ws.cell(row=ws.max_row, column=idx["url"]).hyperlink = r["url"]
    widths = {"id": 10, "url": 38, "title": 46, "property_type": 16, "location": 40, "images": 30, "detail_text": 50}
    for c, i in idx.items():
        ws.column_dimensions[get_column_letter(i)].width = widths.get(c, 15)
    for c, fmt in (("price", "#,##0"), ("daily_price", "#,##0"), ("area_sqm", "0.0"), ("price_per_sqm", "#,##0")):
        for row in ws.iter_rows(min_row=2, min_col=idx[c], max_col=idx[c]):
            row[0].number_format = fmt
    ws.freeze_panes = "C2"
    ws.auto_filter.ref = ws.dimensions
    s = wb.create_sheet("Summary")
    s.append(["Metric", "Value"])
    s["A1"].font = s["B1"].font = Font(bold=True)
    s.append(["Generated (UTC)", now_iso()])
    for k, v in stats.items():
        s.append([k, v])
    s.column_dimensions["A"].width = 26
    wb.save(path)


# --------------------------------------------------------------------------- commands
def load_sources(args) -> list[str]:
    urls = list(args.url or [])
    if getattr(args, "url_file", None):
        urls += [x.strip() for x in Path(args.url_file).read_text(encoding="utf-8").splitlines()
                 if x.strip() and not x.startswith("#")]
    return [source_key(u) for u in dict.fromkeys(urls or [DEFAULT_URL])]


def print_quality(stats: dict):
    n = stats["listings"] or 1
    pct = lambda k: f"{stats[k]} ({100 * stats[k] / n:.0f}%)"  # noqa: E731
    log.info("quality: listings=%d  price=%s  area=%s  beds=%s  baths=%s  location=%s",
             stats["listings"], pct("with_price"), pct("with_area"), pct("with_beds"), pct("with_baths"), pct("with_location"))


async def cmd_crawl(args, eng: Engine) -> int:
    sources = load_sources(args)
    await eng.ensure_robots([u for s in sources for u in (page_url(s, 1), page_url(s, 2))])
    crawler = Crawler(eng)
    results = []
    for s in sources:
        log.info("crawling %s", s)
        results.append(await crawler.crawl_source(s))
    stats = eng.db.stats()
    print_quality(stats)
    for r in results:
        log.info("source done: %s | stopped: %s | new this run: %d | in DB: %d | site says: %s",
                 r["source"], r["stopped"], r["new"], r["in_db"], r["site_total"])
        if r["site_total"] and r["in_db"] < 0.95 * r["site_total"]:
            log.warning("collected %d of ~%d listings (%.0f%%). Inventory shifts while crawling; re-run with --refresh, "
                        "and if it stays low the site caps pagination: shard with `areas` + `--url-file`.",
                        r["in_db"], r["site_total"], 100 * r["in_db"] / r["site_total"])
    return 0


async def cmd_details(args, eng: Engine) -> int:
    sources = load_sources(args)
    await eng.ensure_robots([page_url(sources[0], 1), urljoin(sources[0], "/en/listing/1-for-rent-x/")])
    stats = await DetailRunner(eng).run()
    log.info("details finished: %s", stats)
    return 0


async def cmd_probe(args, eng: Engine) -> int:
    source = load_sources(args)[0]
    outdir = Path(args.samples_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    await eng.ensure_robots([page_url(source, 1), page_url(source, 2)])
    url = page_url(source, 1)
    r = await eng.fetch(url)
    (outdir / "page1.html").write_text(r.text, encoding="utf-8")
    pr = parse_listing_page(r.text, url)
    print(f"\n=== LIST PAGE  {url}\nHTTP {r.status}  {len(r.text):,} bytes  saved: {outdir / 'page1.html'}")
    print(f"site total: {pr.total_results}   cards parsed: {len(pr.cards)}")
    if pr.cards:
        fields = ["title", "property_type", "price", "daily_price", "area_sqm", "beds", "baths", "location", "image_url"]
        print("field coverage:", {f: sum(getattr(c, f) is not None for c in pr.cards) for f in fields})
        for c in pr.cards[:3]:
            d = asdict(c)
            d.pop("card_html")
            print(json.dumps(d, ensure_ascii=False, indent=2))
        print("--- first card HTML (truncated) ---")
        print(pr.cards[0].card_html[:1800])
    detail_url = args.listing_url or (pr.cards[0].url if pr.cards and not args.no_detail else None)
    if detail_url:
        dr = await eng.fetch(detail_url)
        (outdir / "detail.html").write_text(dr.text, encoding="utf-8")
        print(f"\n=== DETAIL PAGE  {detail_url}\nHTTP {dr.status}  {len(dr.text):,} bytes  saved: {outdir / 'detail.html'}")
        if dr.status == 200:
            d = parse_detail(dr.text, detail_url)
            show = {k: v for k, v in d.items() if k not in ("detail_text",)}
            print(json.dumps(show, ensure_ascii=False, indent=2))
    print(f"\nSend me {outdir}/page1.html and {outdir}/detail.html (or this output) if any field above is wrong or empty.")
    return 0 if pr.cards else 1


async def cmd_areas(args, eng: Engine) -> int:
    source = load_sources(args)[0]
    r = await eng.fetch(page_url(source, 1))
    doc = HTMLParser(r.text)
    found = []
    for a in doc.css("a[href*='/for-rent/']"):
        h = a.attributes.get("href") or ""
        if "/listing/" in h or "?" in h:
            continue
        u = urljoin(source, h)
        if u.rstrip("/") != source.split("?")[0].rstrip("/") and "alexandria" in u.lower() and u not in found:
            found.append(u)
    for u in found:
        print(u)
    log.info("%d area URL(s) found. Save to a file and use: crawl --url-file areas.txt", len(found))
    return 0 if found else 1


def cmd_reparse(args, db: DB) -> int:
    n = 0
    if args.what in ("cards", "all"):
        for r in db.conn.execute("SELECT id,url,card_html FROM listings WHERE card_html IS NOT NULL").fetchall():
            c = parse_card_fragment(zlib.decompress(r["card_html"]).decode("utf-8"), r["id"], r["url"])
            if c is None:
                continue
            db.conn.execute(
                "UPDATE listings SET title=?,property_type=?,price=?,daily_price=?,area_sqm=?,beds=?,baths=?,location=?,"
                "image_url=?,card_text=? WHERE id=?",
                (c.title, c.property_type, c.price, c.daily_price, c.area_sqm, c.beds, c.baths, c.location,
                 c.image_url, c.card_text, r["id"]))
            n += 1
        log.info("re-parsed %d card(s)", n)
    if args.what in ("details", "all"):
        m = 0
        for r in db.conn.execute("SELECT id,detail_text FROM listings WHERE detail_text IS NOT NULL").fetchall():
            d = reparse_detail_from_text(zlib.decompress(r["detail_text"]).decode("utf-8"))
            if d:
                sets = ", ".join(f"{k}=?" for k in d)
                db.conn.execute(f"UPDATE listings SET {sets} WHERE id=?", [*d.values(), r["id"]])
                m += 1
        log.info("re-parsed %d detail page text(s) (text-based pairs only; tables/definition lists need a re-fetch)", m)
    db.commit()
    print_quality(db.stats())
    return 0


def cmd_export(args, db: DB) -> int:
    rows = load_export_rows(db, args.with_text)
    if not rows:
        log.error("no listings in the database; run `crawl` first")
        return 1
    base = Path(args.out)
    stats = db.stats()
    fmts = ["csv", "xlsx", "jsonl"] if args.format == "all" else [args.format]
    for f in fmts:
        path = base.with_suffix(f".{f}")
        if f == "csv":
            export_csv(rows, path, args.with_text)
        elif f == "jsonl":
            export_jsonl(rows, path)
        else:
            export_xlsx(rows, path, stats, args.with_text)
        log.info("wrote %s (%d rows)", path, len(rows))
    print_quality(stats)
    return 0


def cmd_status(args, db: DB) -> int:
    s = db.stats()
    print(json.dumps(s, indent=2))
    for r in db.conn.execute("SELECT source, COUNT(*) c, MAX(fetched_at) last FROM pages GROUP BY source"):
        total = db.meta_get(f"total:{r['source']}")
        print(f"source: {r['source']}\n  pages stored: {r['c']}  last fetch: {r['last']}  site total: {total}")
    if s["detail_pending"]:
        print(f"detail pages pending: {s['detail_pending']}  (~{fmt_eta(s['detail_pending'] / max(args.rps, 0.01))} at --rps {args.rps})")
    return 0


# --------------------------------------------------------------------------- CLI
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=DEFAULT_DB, help="SQLite file (default: %(default)s)")
    common.add_argument("--url", action="append", help="results URL (repeatable). Default: Alexandria for-rent")
    common.add_argument("--url-file", help="text file with one results URL per line")
    common.add_argument("--rps", type=float, default=1.0, help="max requests/second overall (default 1.0)")
    common.add_argument("--concurrency", type=int, default=3, help="parallel workers for `details` (default 3)")
    common.add_argument("--timeout", type=float, default=30.0)
    common.add_argument("--max-retries", type=int, default=5)
    common.add_argument("--backoff-base", type=float, default=2.0, help="seconds, doubles per retry")
    common.add_argument("--block-limit", type=int, default=4, help="stop after N consecutive blocked responses")
    common.add_argument("--user-agent", default=DEFAULT_UA)
    common.add_argument("--proxy", help="e.g. http://user:pass@host:port (an Egypt exit can help if geo-filtered)")
    common.add_argument("--fetcher", choices=["httpx", "playwright"], default="httpx")
    common.add_argument("--log-file", default="aqarmap_scraper.log")
    common.add_argument("-v", "--verbose", action="store_true")

    p = argparse.ArgumentParser(prog="aqarmap_scraper", description=__doc__.split("\n\n")[0])
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe", parents=[common], help="fetch one list page + one detail page, save HTML, show parse")
    sp.add_argument("--samples-dir", default="samples")
    sp.add_argument("--listing-url", help="specific listing URL to test instead of the first card")
    sp.add_argument("--no-detail", action="store_true")

    sp = sub.add_parser("crawl", parents=[common], help="phase 1: result pages -> listings")
    sp.add_argument("--max-pages", type=int, default=1000, help="hard cap per source (use 3 for a smoke test)")
    sp.add_argument("--refresh", action="store_true", help="re-fetch pages already stored")

    sp = sub.add_parser("details", parents=[common], help="phase 2: listing pages -> extra fields")
    sp.add_argument("--limit", type=int, help="only process N listings (smoke test)")
    sp.add_argument("--retry-errors", action="store_true")
    sp.add_argument("--redo", action="store_true", help="re-fetch every listing, even finished ones")

    sp = sub.add_parser("reparse", parents=[common], help="re-run parsers on stored HTML (no network)")
    sp.add_argument("--what", choices=["cards", "details", "all"], default="all")

    sp = sub.add_parser("export", parents=[common], help="write csv/xlsx/jsonl")
    sp.add_argument("--format", choices=["csv", "xlsx", "jsonl", "all"], default="all")
    sp.add_argument("--out", default="aqarmap_alexandria_rent", help="output path without extension")
    sp.add_argument("--with-text", action="store_true", help="include the stored detail-page text column")

    sub.add_parser("status", parents=[common], help="progress and data quality")
    sub.add_parser("areas", parents=[common], help="list sub-area URLs found on page 1")
    return p


def setup_logging(verbose: bool, log_file: str | None):
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        handlers.append(RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=2, encoding="utf-8"))
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=handlers, force=True,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    for noisy in ("httpx", "httpcore", "hpack"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


async def run_async(args, db: DB) -> int:
    eng = Engine(args, db)
    try:
        handler = {"crawl": cmd_crawl, "details": cmd_details, "probe": cmd_probe, "areas": cmd_areas}[args.cmd]
        return await handler(args, eng)
    finally:
        db.commit()
        await eng.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose, args.log_file)
    db = DB(args.db)
    try:
        if args.cmd == "export":
            return cmd_export(args, db)
        if args.cmd == "status":
            return cmd_status(args, db)
        if args.cmd == "reparse":
            return cmd_reparse(args, db)
        return asyncio.run(run_async(args, db))
    except RobotsDisallowed as e:
        log.error("%s", e)
        return 2
    except BlockedError as e:
        log.error("BLOCKED: %s", e)
        log.error("Progress is saved. Options: wait and re-run (it resumes), lower --rps (try 0.3), use --proxy with an "
                  "Egyptian exit, or --fetcher playwright.")
        return 3
    except FetchError as e:
        log.error("%s", e)
        return 1
    except KeyboardInterrupt:
        log.warning("interrupted; progress is saved, re-run the same command to resume")
        return 130
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())