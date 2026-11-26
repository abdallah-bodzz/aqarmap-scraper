#!/usr/bin/env python3
"""
aqarmap_scraper.py - resumable, polite scraper for Aqarmap listing pages (v3).

Commands
    probe     fetch page 1 + one detail page, save raw HTML to ./samples/, run the schema canary,
              show payload/card/ld+json agreement, sort-control hints, optional pagination depth test.
    crawl     phase 1: walk the paginated result pages. One row per listing, payload first, card as cross-check.
    details   phase 4 (optional): open listing pages only for fields the payload lacks. `--plan` shows the numbers.
    validate  data-quality flags and coverage by property type.
    reparse   rebuild every parsed column offline from stored raw pages / raw JSON (no network).
    export    write CSV / XLSX / JSONL / Parquet (atomic, typed, with a data dictionary).
    diff      new / delisted / price-changed listings between two crawl runs.
    status    progress, runs, data-quality numbers.
    areas     district URLs for sharding (fallback only; pagination is uncapped on the live site).

Design notes
    * SQLite (WAL) is the source of truth. Every page commits, so Ctrl-C / crashes lose nothing.
    * Primary data source: the JSON the page embeds (React Server Components stream, `initialListings`).
      The HTML cards are parsed too and compared field by field; on an id mismatch the page falls back to cards.
    * A "cycle" is one pass over a source's pages. An unfinished cycle resumes. After a complete one the next
      `crawl` starts a fresh snapshot. A cycle only counts as complete when every expected page was fetched,
      it saw >= 97% of the site total and >= 90% of the previous complete cycle. Only then do misses count.
    * robots.txt is fetched and enforced. There is deliberately no override flag.
    * No proxy rotation, fingerprint tricks, CAPTCHA solving, logins or phone numbers. Phone-like strings are
      scrubbed from everything stored.

Exit codes
    0 ok | 1 error | 2 robots.txt forbids the URLs | 3 blocked | 4 missing dependency
    5 page structure changed (schema canary) | 6 export file locked/unwritable | 7 crawl ended incomplete
    130 interrupted

Requires Python 3.10+.
"""
from __future__ import annotations

import importlib.util
import sys


def _check_dependencies() -> None:
    required = {"httpx": "httpx[http2]", "selectolax": "selectolax"}
    missing = [pip for mod, pip in required.items() if importlib.util.find_spec(mod) is None]
    if missing:
        sys.stderr.write(
            "aqarmap_scraper: missing required packages: " + ", ".join(missing) + "\n"
            f"Install with:  {sys.executable} -m pip install " + " ".join(f'"{m}"' for m in missing) + "\n"
            "or:            pip install -r requirements.txt\n")
        raise SystemExit(4)


_check_dependencies()

import argparse  # noqa: E402
import asyncio  # noqa: E402
import csv  # noqa: E402
import gzip  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import math  # noqa: E402
import os  # noqa: E402
import random  # noqa: E402
import re  # noqa: E402
import shutil  # noqa: E402
import sqlite3  # noqa: E402
import statistics  # noqa: E402
import time  # noqa: E402
import zlib  # noqa: E402
from collections import Counter, defaultdict  # noqa: E402
from dataclasses import asdict, dataclass, field  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
from logging.handlers import RotatingFileHandler  # noqa: E402
from pathlib import Path  # noqa: E402
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse  # noqa: E402

import httpx  # noqa: E402
from selectolax.lexbor import LexborHTMLParser as HTMLParser  # noqa: E402
from selectolax.lexbor import LexborNode as Node  # noqa: E402

__version__ = "3.0.0"
SCHEMA_VERSION = 3

DEFAULT_URL = "https://aqarmap.com.eg/en/for-rent/property-type/alexandria/"
DEFAULT_DB = "aqarmap_alexandria_rent.db"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}
BBOX = (30.0, 31.6, 28.8, 30.5)   # generous box around Alexandria governorate: lat_min, lat_max, lng_min, lng_max
MIN_SITE_COVERAGE = 0.97          # a cycle is complete only if it saw >= 97% of the site's stated total ...
MIN_PREV_RATIO = 0.90             # ... and >= 90% of the previous complete cycle's count
SHARED_COORD_MIN = 15             # >= this many listings on identical coordinates => probable centroid
MAX_PAYLOAD_FAIL_STREAK = 5       # pages in a row without a usable payload before the run stops

EXIT_OK, EXIT_ERROR, EXIT_ROBOTS, EXIT_BLOCKED, EXIT_DEPS, EXIT_STRUCTURE, EXIT_EXPORT, EXIT_INCOMPLETE = 0, 1, 2, 3, 4, 5, 6, 7

log = logging.getLogger("aqarmap")


# --------------------------------------------------------------------------- errors
class BlockedError(RuntimeError):
    """The site is refusing us (403 / challenge page) repeatedly."""


class FetchError(RuntimeError):
    """A URL could not be fetched after all retries."""


class RobotsDisallowed(RuntimeError):
    """robots.txt forbids the URLs we were asked to crawl."""


class TransportFailure(Exception):
    """Network-level failure (DNS, TLS, timeout, reset)."""


class PayloadError(ValueError):
    """The embedded listing payload is missing or malformed."""


class SchemaChanged(RuntimeError):
    """The page structure we depend on is gone. Stop instead of writing empty rows."""


class ExportLocked(RuntimeError):
    """An export target could not be replaced (usually open in Excel on Windows)."""


# --------------------------------------------------------------------------- small utils
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


_AR_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩٬٫", "0123456789,.")


def norm_digits(s: str) -> str:
    return s.translate(_AR_DIGITS)


def to_num(s) -> int | float | None:
    if s is None or isinstance(s, bool):
        return None
    if isinstance(s, (int, float)):
        return int(s) if float(s).is_integer() else s
    s = norm_digits(str(s)).replace(",", "").strip(" .")
    try:
        v = float(s)
    except ValueError:
        return None
    return int(v) if v.is_integer() else v


def first_number(s) -> int | float | None:
    if s is None or s == "":
        return None
    if isinstance(s, (int, float)) and not isinstance(s, bool):
        return to_num(s)
    m = re.search(r"\d[\d,]*(?:\.\d+)?", norm_digits(str(s)))
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


def clean(v):
    """Payload values: '' and the RSC marker '$undefined' mean 'no data'."""
    if v is None or v == "" or v == "$undefined":
        return None
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


def add_params(url: str, params: list[str] | None) -> str:
    if not params:
        return url
    p = urlparse(url)
    q = parse_qsl(p.query, keep_blank_values=True)
    for kv in params:
        k, _, v = kv.partition("=")
        q = [(a, b) for a, b in q if a != k] + [(k, v)]
    return urlunparse(p._replace(query=urlencode(q)))


_SVG_RE = re.compile(r"<svg\b.*?</svg>", re.S | re.I)
# Phone-like strings. The site masks numbers ("+20100078xxxx"); we scrub those and unmasked ones anyway.
PHONE_RE = re.compile(r"\+\d{6,}x+|\+\d{8,}|\b0?1[0-25]\d{8}\b")
_CONTACT_HREF_RE = re.compile(r'href="(?:tel:|sms:|https?://(?:wa\.me|api\.whatsapp\.com|web\.whatsapp\.com)/)[^"]*"', re.I)


def strip_svg(html: str) -> str:
    return _SVG_RE.sub("", html)


def scrub_phones(text: str) -> str:
    return PHONE_RE.sub("+20xxxxxxxxxx", text)


def scrub_card_html(html: str) -> str:
    return scrub_phones(_CONTACT_HREF_RE.sub('href="#"', strip_svg(html)))


_PHONE_KEYS = {"phones", "phone", "mobile", "whatsapp", "whatsapp_number", "has_whatsapp"}


def scrub_payload(obj):
    """Drop phone containers from a payload object and mask anything phone-like left in strings."""
    def walk(o):
        if isinstance(o, dict):
            return {k: walk(v) for k, v in o.items() if k not in _PHONE_KEYS}
        if isinstance(o, list):
            return [walk(v) for v in o]
        if isinstance(o, str):
            return scrub_phones(o)
        return o
    return walk(obj)


def zjson(obj) -> bytes:
    return zlib.compress(json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), 6)


def unzjson(blob: bytes | None):
    return json.loads(zlib.decompress(blob).decode("utf-8")) if blob else None


# --------------------------------------------------------------------------- robots.txt
class RobotsRules:
    """Minimal robots.txt matcher with Google-style wildcards and longest-match wins."""

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
                if not pattern:
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


# --------------------------------------------------------------------------- property types
# Seeded from what the brief shows ("apartment") plus the usual Aqarmap categories. The payload slug is the
# source of truth; a slug that is not listed here is stored as NULL, flagged `unknown_property_type`, and kept
# verbatim in `property_type_raw`. `validate` lists every unknown slug so this set can be completed from data.
RESIDENTIAL_TYPES = frozenset({
    "apartment", "villa", "duplex", "penthouse", "studio", "chalet", "townhouse", "twin-house", "roof",
    "hotel-apartment", "room", "cabin", "full-floor",
})
NON_RESIDENTIAL_TYPES = frozenset({
    "office", "shop", "clinic", "commercial", "administrative", "warehouse", "factory", "land", "building",
    "garage", "pharmacy", "restaurant", "medical", "farm", "villa-land", "whole-building", "showroom", "store",
})
CANONICAL_TYPES = RESIDENTIAL_TYPES | NON_RESIDENTIAL_TYPES
_TYPE_ALIASES = {
    "apartments": "apartment", "villas": "villa", "offices": "office", "shops": "shop", "clinics": "clinic",
    "studios": "studio", "chalets": "chalet", "duplexes": "duplex", "penthouses": "penthouse", "roofs": "roof",
    "lands": "land", "buildings": "building", "garages": "garage", "warehouses": "warehouse",
    "twin-houses": "twin-house", "townhouses": "townhouse", "rooms": "room", "stores": "store",
    "commercial-store": "shop", "administrative-office": "office",
}


def normalize_type_slug(slug) -> tuple[str | None, str | None]:
    """-> (canonical_type_or_None, raw_slug_or_None). Never derives anything from free text."""
    slug = clean(slug)
    if not isinstance(slug, str):
        return None, None
    raw = slug.strip()
    s = raw.lower().replace("_", "-").replace(" ", "-")
    s = _TYPE_ALIASES.get(s, s)
    return (s if s in CANONICAL_TYPES else None), raw


def type_from_title(title: str | None) -> str | None:
    """Fallback only when the payload has no type: the title must START with a canonical type word."""
    if not title:
        return None
    m = re.match(r"\s*([A-Za-z][A-Za-z\- ]{2,20}?)\s+for\s+(?:rent|sale)\b", title, re.I)
    if not m:
        return None
    cand = _TYPE_ALIASES.get(m.group(1).strip().lower().replace(" ", "-"), m.group(1).strip().lower().replace(" ", "-"))
    return cand if cand in CANONICAL_TYPES else None


# --------------------------------------------------------------------------- prices
LISTING_HREF_RE = re.compile(r"/listing/(\d+)")
LABELLEDBY_RE = re.compile(r"listing-(\d+)-title")
PRICE_RE = re.compile(r"(?:EGP|جنيه|ج\.م)\W{0,4}([\d,\.]+\d)|([\d,\.]*\d)\W{0,4}(?:EGP|جنيه|ج\.م)", re.I)
AREA_RE = re.compile(r"([\d,\.]*\d)\s*(?:m²|m2|sqm|sq\.?\s?m|م²|م2|متر)", re.I)
BEDS_RE = re.compile(r"(\d{1,2})\s*(?:beds?|bedrooms?|br\b|غرف|غرفة)", re.I)
BATHS_RE = re.compile(r"(\d{1,2})\s*(?:baths?|bathrooms?|حمام)", re.I)
TOTAL_RE = re.compile(r"([\d,]{2,})\s+(?:Propert(?:y|ies)|عقار)", re.I)
MAX_CARD_CHARS = 2500

_PERIODS = (
    ("day", re.compile(r"/\s*(?:day|daily|night)\b|per\s*(?:day|night)|\bdaily\b|يومي|/\s*يوم|ليلة", re.I)),
    ("week", re.compile(r"/\s*week|per\s*week|\bweekly\b|أسبوع", re.I)),
    ("year", re.compile(r"/\s*(?:year|yr)\b|per\s*(?:year|annum)|\byearly\b|\bannual|سنوي|/\s*سنة", re.I)),
    ("month", re.compile(r"/\s*(?:month|mo)\b|per\s*month|\bmonthly\b|شهري|/\s*شهر", re.I)),
)
_PERIOD_RANK = {"month": 0, "year": 1, "week": 2, "day": 3, None: 4}


def period_of(text: str | None) -> str | None:
    for name, rx in _PERIODS:
        if text and rx.search(text):
            return name
    return None


def monthly_equiv(amount: float | None, period: str | None) -> float | None:
    """Normalise a listed price to EGP/month. day x30 and week x52/12 are conventions; those rows are flagged."""
    if amount is None or period is None:
        return None
    return {"month": amount, "day": amount * 30, "week": amount * 52 / 12, "year": amount / 12}[period]


def pick_prices(prices: list[tuple[float, str | None]]) -> tuple[float | None, str | None, float | None, float | None]:
    """-> (price_amount, price_period, price_monthly, daily_rate).
    daily_rate has one meaning: the explicit per-day price shown on the listing, whether it is the primary price
    (period='day') or sits beside a monthly one. NULL when no per-day price is shown."""
    if not prices:
        return None, None, None, None
    ordered = sorted(prices, key=lambda x: _PERIOD_RANK[x[1]])
    amount, period = ordered[0]
    daily = amount if period == "day" else next((a for a, p in prices if p == "day"), None)
    m = monthly_equiv(amount, period)
    return amount, period, (round(m, 2) if m is not None else None), daily


def _prices_from_text(text: str) -> list[tuple[float, str | None]]:
    out: list[tuple[float, str | None]] = []
    ms = list(PRICE_RE.finditer(text))
    for i, m in enumerate(ms):
        amt = to_num(m.group(1) or m.group(2))
        if amt is None:
            continue
        end_lim = ms[i + 1].start() if i + 1 < len(ms) else len(text)
        after = text[m.end():min(end_lim, m.end() + 25)]
        before = text[max(0, m.start() - 15):m.start()]
        out.append((float(amt), period_of(after) or period_of(before)))
    return out


# --------------------------------------------------------------------------- parsing: result-page cards (cross-check + fallback)
@dataclass
class Card:
    id: int
    url: str
    url_slug: str | None
    title: str | None
    price_amount: float | None
    price_period: str | None
    price_monthly: float | None
    daily_rate: float | None
    area_sqm: float | None
    beds: int | None
    baths: int | None
    district: str | None
    area_name: str | None
    district_url: str | None
    area_url: str | None
    location: str | None
    advertiser: str | None        # agency name shown by the logo. Only present when the agency pays for the logo.
    badges: str | None
    photo_count: int | None
    image_url: str | None
    card_text: str
    card_html: str


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


def _icon_value(node: Node, icons: tuple[str, ...]) -> str | None:
    """Text of the <span> that belongs to an icon. Looks at most two ancestors up, never leaves the <li>."""
    for ic in icons:
        for i in node.css(f'i[class*="{ic}"]'):
            anc = i.parent
            for _ in range(2):
                if anc is None:
                    break
                for sp in anc.css("span"):
                    t = squash(sp.text(strip=True))
                    if t:
                        return t
                if anc.tag == "li":
                    break
                anc = anc.parent
    return None


def _abs_url(base_url: str, href: str | None) -> str | None:
    if not href:
        return None
    return urlunparse(urlparse(urljoin(base_url, href))._replace(query="", fragment=""))


def parse_card(node: Node, listing_id: int, base_url: str) -> Card:
    anchors = _anchors_in(node)
    listing_anchors = [a for a in anchors if _listing_id(a.attributes.get("href")) == listing_id]
    href = listing_anchors[0].attributes.get("href") if listing_anchors else f"/en/listing/{listing_id}/"
    url = _abs_url(base_url, href)
    slug_m = re.search(r"/listing/\d+-(?:for-rent|for-sale)-?(.*?)/?$", urlparse(url).path)
    url_slug = (slug_m.group(1) or None) if slug_m else None

    text_spaced = squash(node.text(separator=" ", strip=True))
    text_piped = node.text(separator=" | ", strip=True)
    text_norm = norm_digits(text_spaced)

    title = None
    h = node.css_first("h2,h3,h4,h1")
    if h is not None:
        title = squash(h.attributes.get("title") or h.text(strip=True)) or None
    if not title:
        for a in listing_anchors:
            t = squash(a.attributes.get("title") or a.attributes.get("aria-label") or a.text(strip=True))
            if len(t) > 10:
                title = t
                break

    prices: list[tuple[float, str | None]] = []
    for d in node.css("data[value]"):
        amt = to_num(d.attributes.get("value"))
        if amt is None:
            amt = first_number(d.text(strip=True))
        if amt is not None:
            prices.append((float(amt), period_of(d.text(separator=" ", strip=True))))
    if not prices:
        prices = _prices_from_text(text_norm)
    price_amount, price_period, price_monthly, daily_rate = pick_prices(prices)

    area = first_number(_icon_value(node, ("size-icon", "area-icon")))
    if area is None and (m := AREA_RE.search(text_norm)):
        area = to_num(m.group(1))
    beds_v = first_number(_icon_value(node, ("bedroom-icon", "bed-icon")))
    baths_v = first_number(_icon_value(node, ("bathroom-icon", "bath-icon")))
    if beds_v is None and (m := BEDS_RE.search(text_norm)):
        beds_v = int(m.group(1))
    if baths_v is None and (m := BATHS_RE.search(text_norm)):
        baths_v = int(m.group(1))

    crumbs: list[tuple[str, str]] = []
    for a in anchors:
        hr = a.attributes.get("href") or ""
        if "/for-rent/" in hr and "/listing/" not in hr:
            t = squash(a.text(strip=True))
            if t and (t, hr) not in crumbs:
                crumbs.append((t, hr))
    district = crumbs[0][0] if crumbs else None
    district_url = _abs_url(base_url, crumbs[0][1]) if crumbs else None
    area_name = crumbs[-1][0] if len(crumbs) > 1 else None
    area_url = _abs_url(base_url, crumbs[-1][1]) if len(crumbs) > 1 else None
    location = " > ".join(t for t, _ in crumbs) or None

    advertiser = None
    adv = node.css_first("aside.listing-user-logo a[aria-label]")
    if adv is not None:
        advertiser = squash(re.sub(r"^View listing by\s+", "", adv.attributes.get("aria-label") or "", flags=re.I)) or None
    if advertiser is None:
        logo = node.css_first("aside.listing-user-logo img[alt]")
        if logo is not None:
            advertiser = squash(re.sub(r"\s*logo\s*$", "", logo.attributes.get("alt") or "", flags=re.I)) or None
    badge_txt = [squash(li.text(strip=True)) for li in node.css('ul[class*="start-2x"] li')]
    badges = ", ".join(b for b in badge_txt if b) or None

    photo_count = first_number(_icon_value(node, ("image-icon",)))
    photo_count = int(photo_count) if photo_count is not None else None
    img = node.css_first("div.embla img") or node.css_first("img")
    image_url = None
    if img is not None:
        src = img.attributes.get("src") or img.attributes.get("data-src") or ""
        if src and not src.startswith("data:"):
            image_url = urljoin(base_url, src)

    return Card(
        id=listing_id, url=url, url_slug=url_slug, title=title,
        price_amount=price_amount, price_period=price_period, price_monthly=price_monthly, daily_rate=daily_rate,
        area_sqm=float(area) if area is not None else None,
        beds=int(beds_v) if beds_v is not None else None,
        baths=int(baths_v) if baths_v is not None else None,
        district=district, area_name=area_name, district_url=district_url, area_url=area_url, location=location,
        advertiser=advertiser, badges=badges, photo_count=photo_count, image_url=image_url,
        card_text=scrub_phones(text_piped), card_html=scrub_card_html(node.html or ""),
    )


def _card_id(article: Node) -> int | None:
    m = LABELLEDBY_RE.search(article.attributes.get("aria-labelledby") or "")
    if m:
        return int(m.group(1))
    ids = sorted(_ids_in(article))
    return ids[0] if ids else None


@dataclass
class PageResult:
    cards: list[Card]
    total_results: int | None


def parse_listing_page(html: str, page_url_: str) -> PageResult:
    doc = HTMLParser(html)
    articles = doc.css("article.listing-card")
    cards: list[Card] = []
    seen_ids: set[int] = set()
    if articles:
        for art in articles:
            lid = _card_id(art)
            if lid is None or lid in seen_ids:
                continue
            seen_ids.add(lid)
            cards.append(parse_card(art, lid, page_url_))
    else:
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
        if cards:
            log.warning("no <article class='listing-card'> found; used the generic fallback (%d cards). "
                        "Layout probably changed: run `probe`.", len(cards))
    total = None
    for art in articles:
        art.decompose()
    body_text = norm_digits(doc.body.text(separator=" ", strip=True)) if doc.body else ""
    for m in TOTAL_RE.finditer(body_text[:20000]):
        v = to_num(m.group(1))
        if v and v >= 1:
            total = int(v)
            break
    return PageResult(cards=cards, total_results=total)


def parse_card_fragment(card_html: str, listing_id: int, base_url: str) -> Card | None:
    doc = HTMLParser(card_html)
    root = doc.css_first("article.listing-card") or doc.css_first("article")
    if root is None and doc.body is not None:
        root = next((c for c in doc.body.iter() if c.tag not in ("-text", "-comment")), None)
    return parse_card(root, listing_id, base_url) if root is not None else None


def looks_like_challenge(text: str) -> bool:
    if len(text) > 60_000:
        return False
    t = text[:8000].lower()
    markers = ("just a moment", "cf-browser-verification", "/cdn-cgi/challenge-platform",
               "attention required! | cloudflare", "px-captcha", "enable javascript and cookies",
               "access denied", "verify you are human", "checking your browser")
    return any(m in t for m in markers)


# --------------------------------------------------------------------------- embedded payload (primary source)
_PUSH = "self.__next_f.push([1,"


def rsc_text(html: str) -> str:
    """Join every decoded `self.__next_f.push([1,"..."])` string, in order (React Server Components stream)."""
    dec = json.JSONDecoder()
    out: list[str] = []
    pos = 0
    while True:
        i = html.find(_PUSH, pos)
        if i < 0:
            break
        j = i + len(_PUSH)
        try:
            s, end = dec.raw_decode(html, j)
        except ValueError:
            pos = j
            continue
        if isinstance(s, str):
            out.append(s)
        pos = end
    return "".join(out)


@dataclass
class Payload:
    listings: list[dict]
    query: str | None


# keys every listing object must have for the payload to be usable at all
HARD_KEYS = ("id", "slug", "title", "price", "area", "location", "previewDetails")


def extract_payload(html: str) -> Payload:
    text = rsc_text(html)
    if not text:
        raise PayloadError("no self.__next_f stream found in the page")
    marker = '"initialListings":'
    k = text.find(marker)
    if k < 0:
        raise PayloadError("'initialListings' not found in the RSC stream")
    try:
        arr, _ = json.JSONDecoder().raw_decode(text, k + len(marker))
    except ValueError as e:
        raise PayloadError(f"initialListings is not valid JSON: {e}") from e
    if not isinstance(arr, list) or not all(isinstance(x, dict) for x in arr):
        raise PayloadError("initialListings is not a list of objects")
    query = None
    qk = text.find('"initialQuery":')
    if qk >= 0:
        try:
            q, _ = json.JSONDecoder().raw_decode(text, qk + len('"initialQuery":'))
            query = q if isinstance(q, str) else None
        except ValueError:
            pass
    return Payload(listings=arr, query=query)


def payload_problems(listings: list[dict]) -> list[str]:
    """Hard structural problems: a listing without an id, or a key missing on > 20% of listings."""
    out = []
    if any("id" not in p for p in listings):
        out.append("some payload listings have no 'id'")
    for key in HARD_KEYS[1:]:
        miss = sum(1 for p in listings if key not in p)
        if listings and miss / len(listings) > 0.2:
            out.append(f"key '{key}' missing on {miss}/{len(listings)} listings")
    return out


@dataclass
class LdItemList:
    total: int | None
    ids: list[int]


def extract_ld(html: str) -> LdItemList | None:
    doc = HTMLParser(html)
    for s in doc.css('script[type="application/ld+json"]'):
        try:
            data = json.loads(s.text())
        except (ValueError, TypeError):
            continue
        stack = [data]
        while stack:
            o = stack.pop()
            if isinstance(o, dict):
                if o.get("@type") == "ItemList":
                    ids = []
                    for el in o.get("itemListElement") or []:
                        u = el.get("url") if isinstance(el, dict) else None
                        i = _listing_id(u)
                        if i is not None:
                            ids.append(i)
                    return LdItemList(to_num(o.get("numberOfItems")), ids)
                stack.extend(o.values())
            elif isinstance(o, list):
                stack.extend(o)
    return None


def parse_location(loc) -> dict:
    """payload `location`: title_full_path "Alexandria / <district> / [group /] <area>" plus parent."""
    if not isinstance(loc, dict):
        return {}
    path = [x.strip() for x in str(loc.get("title_full_path") or "").split(" / ") if x.strip()]
    parent = loc.get("parent") if isinstance(loc.get("parent"), dict) else {}
    district = path[1] if len(path) >= 2 else None
    area = path[-1] if len(path) >= 3 else None
    if len(path) == 2:
        district_id = loc.get("id")
    elif len(path) == 3:
        district_id = parent.get("id")
    else:
        district_id = None  # parent is a group such as "Compounds in X", the district id is not in the object
    return {
        "district": district, "area_name": area, "district_id": district_id,
        "area_id": loc.get("id") if len(path) >= 3 else None, "area_parent_id": parent.get("id"),
        "location_slug": loc.get("slug"),
        "location": (f"{district} > {area}" if district and area else district),
        "in_compound": int(any("compound" in x.lower() for x in path[2:])) if path else None,
    }


_POSTED_KEYS = ("createdAt", "created_at", "postedAt", "publishedAt", "creationDate", "created")
_FURNISHED_KEYS = ("furnished", "isFurnished", "is_furnished")
_LAT_KEYS, _LNG_KEYS = ("lat", "latitude"), ("lng", "lon", "longitude")


def _find_key(obj, keys: tuple[str, ...]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys and not isinstance(v, (dict, list)) and clean(v) is not None:
                return v
        for v in obj.values():
            r = _find_key(v, keys)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_key(v, keys)
            if r is not None:
                return r
    return None


def in_bbox(lat, lng) -> bool:
    return lat is not None and lng is not None and BBOX[0] <= lat <= BBOX[1] and BBOX[2] <= lng <= BBOX[3]


def to_int(v) -> int | None:
    n = first_number(clean(v))
    return int(n) if n is not None else None


def build_row(p: dict | None, card: Card | None, origin: str = "https://aqarmap.com.eg") -> tuple[dict, list[tuple]]:
    """Merge one payload object and one card into a listing row. Payload first, card fallback, NULL otherwise.
    Returns (row, disagreements) where each disagreement is (field, payload_value, card_value)."""
    row: dict = {}
    dis: list[tuple] = []
    pd_ = (p or {}).get("previewDetails") or {}
    attrs = (p or {}).get("attributes") or {}
    user = (p or {}).get("user") or {}
    lid = (p or {}).get("id") if p else card.id
    row["id"] = int(lid)
    row["parse_source"] = "payload" if p else "card"
    slug = clean((p or {}).get("slug"))
    row["slug"] = slug
    row["url"] = card.url if card and card.url else f"{origin}/en/listing/{lid}-{slug}/"
    row["title"] = clean((p or {}).get("title")) or (card.title if card else None)

    # --- type: payload slug only. Free text is never used.
    ptype, raw_slug = normalize_type_slug(pd_.get("propertyTypeSlug"))
    if raw_slug is None:
        ptype = type_from_title(row["title"]) if row["title"] else None
    row["property_type"], row["property_type_raw"] = ptype, raw_slug
    row["property_type_title"] = clean(pd_.get("propertyTypeTitle"))

    # --- price
    call_req = bool((p or {}).get("isCallRequest"))
    pay_price = to_num(clean((p or {}).get("price"))) if p else None
    if call_req or not pay_price:
        pay_price = None
    c_amt = card.price_amount if card else None
    amount = float(pay_price) if pay_price is not None else c_amt
    period = card.price_period if card else None
    if amount is not None and p and c_amt is not None and abs(float(pay_price or 0) - c_amt) > 0.5 and pay_price is not None:
        dis.append(("price", pay_price, c_amt))
    mon = monthly_equiv(amount, period)
    row.update(price_amount=amount, price_period=period, price_monthly=round(mon, 2) if mon is not None else None,
               daily_rate=(amount if period == "day" else (card.daily_rate if card else None)),
               is_call_request=int(call_req), currency="EGP")

    # --- area
    pay_area = to_num(clean((p or {}).get("area"))) if p else None
    c_area = card.area_sqm if card else None
    if pay_area is not None and c_area is not None and abs(pay_area - c_area) > 0.5:
        dis.append(("area", pay_area, c_area))
    row["area_sqm"] = float(pay_area) if pay_area is not None else c_area

    # --- beds / baths: residential only, raw kept
    beds_raw = clean(attrs.get("rooms")) if p else (card.beds if card else None)
    baths_raw = clean(attrs.get("baths")) if p else (card.baths if card else None)
    row["beds_raw"], row["baths_raw"] = (str(beds_raw) if beds_raw is not None else None,
                                        str(baths_raw) if baths_raw is not None else None)
    residential = ptype in RESIDENTIAL_TYPES
    b, ba = to_int(beds_raw), to_int(baths_raw)
    row["beds"] = b if residential else None
    row["baths"] = ba if residential else None
    if p and card:
        if b is not None and card.beds is not None and b != card.beds:
            dis.append(("beds", b, card.beds))
        if ba is not None and card.baths is not None and ba != card.baths:
            dis.append(("baths", ba, card.baths))

    row["floor"] = clean(attrs.get("floor"))
    row["finish_type"] = clean(attrs.get("finishType"))
    yb = clean(pd_.get("yearBuilt"))
    row["year_built"] = str(yb) if yb is not None else None
    vc = clean(pd_.get("propertyView"))
    row["view_code"] = to_int(vc) if vc is not None else None

    # --- location
    loc = parse_location((p or {}).get("location"))
    row["district"] = loc.get("district") or (card.district if card else None)
    row["area_name"] = loc.get("area_name") or (card.area_name if card else None)
    row["district_id"], row["area_id"] = loc.get("district_id"), loc.get("area_id")
    row["area_parent_id"], row["location_slug"] = loc.get("area_parent_id"), loc.get("location_slug")
    row["location"] = loc.get("location") or (card.location if card else None)
    row["in_compound"] = loc.get("in_compound")
    row["district_url"] = card.district_url if card else None
    row["area_url"] = card.area_url if card else None

    # --- advertiser: NULL = no data, never "owner"
    uid = user.get("id")
    row["advertiser_id"] = int(uid) if isinstance(uid, int) or (isinstance(uid, str) and uid.isdigit()) else None
    row["advertiser_name"] = clean(user.get("fullName")) or (card.advertiser if card else None)
    row["card_advertiser"] = card.advertiser if card else None
    paid = user.get("has_active_logo_service") if p else None
    row["has_paid_logo"] = int(bool(paid)) if paid is not None else (int(card.advertiser is not None) if card else None)
    row["featured_type"] = clean((p or {}).get("featuredType"))
    row["badges"] = card.badges if card else None

    # --- photos
    pc = (p or {}).get("photosCount")
    row["photo_count"] = to_int(pc) if pc is not None else (card.photo_count if card else None)
    big = pd_.get("largePhotos")
    photos = [x.get("src") for x in big if isinstance(x, dict) and x.get("src")] if isinstance(big, list) else None
    row["photos"] = json.dumps(photos) if photos else None
    row["image_url"] = card.image_url if card else None
    row["updated_at"] = clean(pd_.get("updatedAt"))

    # --- fields the brief hopes might exist in the payload (taken only when really present)
    posted = _find_key(p, _POSTED_KEYS) if p else None
    row["posted_date"] = str(posted) if posted is not None else None
    fur = _find_key(p, _FURNISHED_KEYS) if p else None
    row["furnished"] = ("yes" if fur in (True, 1, "1", "true", "yes") else "no") if fur is not None else None
    lat, lng = to_num(_find_key(p, _LAT_KEYS)) if p else None, to_num(_find_key(p, _LNG_KEYS)) if p else None
    row["lat"], row["lng"] = (lat, lng) if in_bbox(lat, lng) else (None, None)
    row["payment_method"] = to_int((p or {}).get("payment_method"))
    row["payment_method_label"] = clean(pd_.get("paymentMethodLabel"))
    plans = (p or {}).get("payment_plans")
    row["payment_duration"] = to_int(plans[0].get("duration")) if isinstance(plans, list) and plans and isinstance(plans[0], dict) else None
    row["category"] = clean((p or {}).get("category"))
    row["has_map"] = int(bool(pd_.get("hasMap"))) if "hasMap" in pd_ else None

    row["card_text"] = card.card_text if card else None
    row["card_html"] = zlib.compress(card.card_html.encode("utf-8"), 6) if card else None
    row["raw_json"] = zjson(scrub_payload(p)) if p else None
    return row, dis


@dataclass
class PageParse:
    rows: list[dict]
    disagreements: list[tuple]           # (listing_id, field, payload_value, card_value)
    total: int | None
    cards: list[Card]
    payload: Payload | None = None
    ld: LdItemList | None = None
    parse_source: str = "card"
    id_match: bool | None = None
    ld_match: bool | None = None
    notes: list[str] = field(default_factory=list)


def parse_page(html: str, url: str) -> PageParse:
    """Pure function: one list page -> merged rows plus every cross-check result."""
    origin = "{0.scheme}://{0.netloc}".format(urlparse(url))
    pr = parse_listing_page(html, url)
    notes: list[str] = []
    payload = None
    try:
        payload = extract_payload(html)
    except PayloadError as e:
        notes.append(f"payload unavailable: {e}")
    ld = extract_ld(html)
    card_ids = [c.id for c in pr.cards]
    id_match = ld_match = None
    use_payload = False
    if payload is not None:
        pids = [p.get("id") for p in payload.listings]
        id_match = sorted(pids) == sorted(card_ids)
        use_payload = id_match
        if not id_match:
            notes.append(f"payload ids != card ids (payload {len(pids)}, cards {len(card_ids)}); using cards for this page")
        probs = payload_problems(payload.listings)
        if probs:
            use_payload = False
            notes.append("payload structure problems: " + "; ".join(probs))
    if ld is not None and ld.ids:
        ld_match = sorted(ld.ids) == sorted(card_ids)
        if not ld_match:
            notes.append("ld+json ids differ from card ids")
    total = pr.total_results
    if total is None and ld is not None:
        total = ld.total
    rows, dis = [], []
    by_id = {p["id"]: p for p in payload.listings} if use_payload else {}
    for c in pr.cards:
        r, d = build_row(by_id.get(c.id), c, origin)
        rows.append(r)
        dis.extend((c.id, f, pv, cv) for f, pv, cv in d)
    return PageParse(rows=rows, disagreements=dis, total=total, cards=pr.cards, payload=payload, ld=ld,
                     parse_source="payload" if use_payload else "card", id_match=id_match, ld_match=ld_match,
                     notes=notes)


def sort_hints(html: str) -> list[str]:
    """Look for a sort control: <select>/<a> with sort/order params, and sort-ish keys in the RSC stream."""
    out: list[str] = []
    doc = HTMLParser(html)
    for a in doc.css("a[href*='sort'], a[href*='order']"):
        out.append(f"link: {(a.attributes.get('href') or '')[:140]}")
    for s in doc.css("select"):
        opts = [(o.attributes.get("value"), squash(o.text(strip=True))) for o in s.css("option")][:12]
        out.append(f"select name={s.attributes.get('name')!r}: {opts}")
    text = rsc_text(html)
    for m in re.finditer(r'"(?:sort|sortBy|sort_by|order|orderBy|sortOptions?)[A-Za-z]*":\s*(\[[^\]]{0,300}\]|"[^"]{0,80}"|\{[^}]{0,300}\})', text):
        out.append("rsc: " + m.group(0)[:200])
        if len(out) > 25:
            break
    return list(dict.fromkeys(out))[:25]


def schema_canary(pp: PageParse) -> list[str]:
    """Problems with page 1 that mean the layout changed. Empty list = healthy."""
    bad: list[str] = []
    if not pp.cards:
        bad.append("no listing cards parsed (selector article.listing-card / listing links)")
    else:
        n = len(pp.cards)
        for f in ("price_amount", "area_sqm", "location", "title"):
            if sum(getattr(c, f) is not None for c in pp.cards) / n < 0.8:
                bad.append(f"card field '{f}' parsed on < 80% of cards")
    if pp.payload is None:
        bad.extend(pp.notes or ["payload missing"])
    else:
        if not pp.payload.listings:
            bad.append("initialListings is empty")
        bad.extend(payload_problems(pp.payload.listings))
        if pp.id_match is False:
            bad.append("payload ids do not match card ids on page 1")
    if pp.total is None:
        bad.append("site total not found (header text and ld+json)")
    return bad


# --------------------------------------------------------------------------- parsing: detail pages (phase 4, only for missing fields)
DETAIL_LABELS: dict[str, list[str]] = {
    "detail_posted_date": ["date posted", "posted", "posted on", "listed on", "تاريخ النشر"],
    "detail_updated_date": ["date updated", "updated", "last updated", "تاريخ التحديث"],
    "detail_area_sqm": ["built-up area", "built up area", "area", "size", "المساحة"],
    "detail_beds": ["bedrooms", "bed rooms", "beds", "غرف النوم", "غرف"],
    "detail_baths": ["bathrooms", "baths", "bath", "الحمامات"],
    "detail_floor": ["floor", "الدور"],
    "detail_view": ["view", "الإطلالة"],
    "detail_year_built": ["year built", "built year", "سنة البناء"],
    "detail_finishing": ["finishing", "finish", "التشطيب"],
    "detail_furnished": ["furnished", "furnishing", "مفروش"],
    "detail_seller_type": ["seller type", "seller", "advertiser type", "نوع المعلن"],
    "detail_payment": ["payment method", "payment terms", "payment", "طريقة الدفع"],
}
_ALL_LABELS = {s for syns in DETAIL_LABELS.values() for s in syns} | {"reception", "reception rooms", "listing id", "property id"}
_DATE_FORMATS = ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y")
_SIMILAR_RE = re.compile(r"similar|related|recommend|you may also|more listings|people also", re.I)


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
    return s


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


def _drop_similar_sections(doc: HTMLParser) -> None:
    for n in doc.css("section,aside,div,ul"):
        ident = f"{n.attributes.get('class') or ''} {n.attributes.get('id') or ''} {n.attributes.get('aria-label') or ''}"
        if ident.strip() and _SIMILAR_RE.search(ident):
            n.decompose()


def parse_detail(html: str, url: str) -> dict:
    """Detail page -> detail_* values only. Photos are deliberately NOT read from here (the payload has them at
    `large` size; this page also carries 'similar listings' thumbnails). 'Similar' sections are removed first."""
    doc = HTMLParser(html)
    out: dict = {}
    h1 = doc.css_first("h1")
    full_title = squash(h1.text(strip=True)) if h1 is not None else None
    if not full_title:
        t = doc.css_first("title")
        full_title = squash(t.text(strip=True)) if t is not None else None
    out["detail_full_title"] = full_title

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
    if lat is not None and not in_bbox(lat, lng):
        log.debug("rejecting coordinates outside bounding box: %s,%s (%s)", lat, lng, url)
        lat = lng = None
    out["detail_lat"], out["detail_lng"] = lat, lng

    _drop_similar_sections(doc)
    root = doc.css_first("main") or doc.body or doc
    pairs: dict[str, str] = {}
    for tr in root.css("tr"):
        cells = [squash(c.text(strip=True)) for c in tr.css("th,td")]
        if len(cells) == 2 and cells[0] and cells[1]:
            pairs.setdefault(norm_label(cells[0]), cells[1])
    for dt in root.css("dt"):
        dd = dt.next
        while dd is not None and dd.tag != "dd":
            dd = dd.next
        if dd is not None:
            k, v = squash(dt.text(strip=True)), squash(dd.text(strip=True))
            if k and v:
                pairs.setdefault(norm_label(k), v)
    for n in root.css("script,style,noscript,svg,nav,footer"):
        n.decompose()
    text = root.text(separator="\n", strip=True)
    lines = [squash(x) for x in text.split("\n") if x.strip()]
    cut = next((i for i, ln in enumerate(lines) if len(ln) < 60 and _SIMILAR_RE.search(ln)), None)
    if cut is not None:
        lines = lines[:cut]
    for k, v in pairs_from_lines(lines).items():
        pairs.setdefault(k, v)
    out.update(map_detail_pairs(pairs))
    out["detail_attrs_json"] = json.dumps(pairs, ensure_ascii=False) if pairs else None
    out["detail_text"] = scrub_phones("\n".join(lines)[:60_000])
    return out


def map_detail_pairs(pairs: dict[str, str]) -> dict:
    out: dict = {}
    for col, syns in DETAIL_LABELS.items():
        val = next((pairs[s] for s in syns if s in pairs), None)
        if val is None:
            continue
        if col == "detail_area_sqm":
            out[col] = first_number(val)
        elif col in ("detail_beds", "detail_baths"):
            n = first_number(val)
            out[col] = int(n) if n is not None else None
        elif col in ("detail_posted_date", "detail_updated_date"):
            out[col] = parse_date(val)
        else:
            out[col] = val[:200]
    return out


def reparse_detail_from_text(detail_text: str) -> dict:
    lines = [x for x in detail_text.split("\n") if x.strip()]
    return map_detail_pairs(pairs_from_lines(lines))


# --------------------------------------------------------------------------- storage
LISTING_COLS: list[tuple[str, str]] = [
    ("id", "INTEGER PRIMARY KEY"), ("url", "TEXT NOT NULL"), ("slug", "TEXT"), ("url_slug", "TEXT"),
    ("title", "TEXT"),
    ("property_type", "TEXT"), ("property_type_raw", "TEXT"), ("property_type_title", "TEXT"),
    ("price_amount", "REAL"), ("price_period", "TEXT"), ("price_monthly", "REAL"), ("daily_rate", "REAL"),
    ("is_call_request", "INTEGER"), ("currency", "TEXT DEFAULT 'EGP'"),
    ("area_sqm", "REAL"), ("beds", "INTEGER"), ("baths", "INTEGER"), ("beds_raw", "TEXT"), ("baths_raw", "TEXT"),
    ("floor", "TEXT"), ("finish_type", "TEXT"), ("year_built", "TEXT"), ("view_code", "INTEGER"),
    ("district", "TEXT"), ("area_name", "TEXT"), ("district_id", "INTEGER"), ("area_id", "INTEGER"),
    ("area_parent_id", "INTEGER"), ("location_slug", "TEXT"), ("location", "TEXT"), ("in_compound", "INTEGER"),
    ("district_url", "TEXT"), ("area_url", "TEXT"),
    ("advertiser_id", "INTEGER"), ("advertiser_name", "TEXT"), ("card_advertiser", "TEXT"),
    ("has_paid_logo", "INTEGER"), ("featured_type", "TEXT"), ("badges", "TEXT"),
    ("photo_count", "INTEGER"), ("photos", "TEXT"), ("image_url", "TEXT"),
    ("updated_at", "TEXT"), ("posted_date", "TEXT"), ("furnished", "TEXT"), ("lat", "REAL"), ("lng", "REAL"),
    ("payment_method", "INTEGER"), ("payment_method_label", "TEXT"), ("payment_duration", "INTEGER"),
    ("category", "TEXT"), ("has_map", "INTEGER"), ("parse_source", "TEXT"),
    ("card_text", "TEXT"), ("card_html", "BLOB"), ("raw_json", "BLOB"),
    ("source", "TEXT"), ("page", "INTEGER"),
    ("first_seen", "TEXT"), ("last_seen", "TEXT"), ("first_seen_run", "INTEGER"), ("last_seen_run", "INTEGER"),
    ("active", "INTEGER DEFAULT 1"), ("missed_runs", "INTEGER DEFAULT 0"), ("delisted_run", "INTEGER"),
    ("flags", "TEXT"),
    ("detail_status", "TEXT"), ("detail_fetched_at", "TEXT"), ("detail_full_title", "TEXT"),
    ("detail_posted_date", "TEXT"), ("detail_updated_date", "TEXT"), ("detail_area_sqm", "REAL"),
    ("detail_beds", "INTEGER"), ("detail_baths", "INTEGER"), ("detail_floor", "TEXT"), ("detail_view", "TEXT"),
    ("detail_year_built", "TEXT"), ("detail_finishing", "TEXT"), ("detail_furnished", "TEXT"),
    ("detail_seller_type", "TEXT"), ("detail_payment", "TEXT"), ("detail_lat", "REAL"), ("detail_lng", "REAL"),
    ("detail_attrs_json", "TEXT"), ("detail_text", "BLOB"),
]
OTHER_TABLES: dict[str, list[tuple[str, str]]] = {
    "price_history": [("id", "INTEGER NOT NULL"), ("seen_at", "TEXT NOT NULL"), ("price_amount", "REAL"),
                      ("price_period", "TEXT"), ("price_monthly", "REAL"), ("run_id", "INTEGER")],
    "runs": [("id", "INTEGER PRIMARY KEY AUTOINCREMENT"), ("kind", "TEXT"), ("source", "TEXT"), ("started_at", "TEXT"),
             ("ended_at", "TEXT"), ("status", "TEXT"), ("complete", "INTEGER DEFAULT 0"), ("site_total", "INTEGER"),
             ("seen", "INTEGER"), ("new_listings", "INTEGER DEFAULT 0"), ("stopped_reason", "TEXT"),
             ("stats_json", "TEXT")],
    "pages": [("source", "TEXT NOT NULL"), ("page", "INTEGER NOT NULL"), ("status", "TEXT NOT NULL"),
              ("n_cards", "INTEGER"), ("ids", "TEXT"), ("fetched_at", "TEXT"), ("error", "TEXT"),
              ("parse_source", "TEXT"), ("payload_n", "INTEGER"), ("id_match", "INTEGER"), ("ld_match", "INTEGER"),
              ("n_disagree", "INTEGER")],
    "raw_pages": [("source", "TEXT NOT NULL"), ("page", "INTEGER NOT NULL"), ("run_id", "INTEGER"),
                  ("fetched_at", "TEXT"), ("html", "BLOB")],
    "disagreements": [("run_id", "INTEGER"), ("id", "INTEGER"), ("field", "TEXT"), ("payload_value", "TEXT"),
                      ("card_value", "TEXT")],
    "meta": [("key", "TEXT PRIMARY KEY"), ("value", "TEXT")],
}
STATIC_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_pages ON pages(source, page);
CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_pages ON raw_pages(source, page);
CREATE INDEX IF NOT EXISTS idx_price_history_id ON price_history(id);
CREATE INDEX IF NOT EXISTS idx_disagreements_run ON disagreements(run_id);
CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(active, detail_status);
CREATE INDEX IF NOT EXISTS idx_listings_source_run ON listings(source, last_seen_run);
"""

# columns rewritten on every sighting / reparse. Crawl-state and detail_* columns are never in this list.
PARSE_COLS = [
    "url", "slug", "title", "property_type", "property_type_raw", "property_type_title", "price_amount",
    "price_period", "price_monthly", "daily_rate", "is_call_request", "currency", "area_sqm", "beds", "baths",
    "beds_raw", "baths_raw", "floor", "finish_type", "year_built", "view_code", "district", "area_name",
    "district_id", "area_id", "area_parent_id", "location_slug", "location", "in_compound", "district_url",
    "area_url", "advertiser_id", "advertiser_name", "card_advertiser", "has_paid_logo", "featured_type", "badges",
    "photo_count", "photos", "image_url", "updated_at", "posted_date", "furnished", "lat", "lng", "payment_method",
    "payment_method_label", "payment_duration", "category", "has_map", "parse_source", "card_text",
]
_BLOB_PARSE = ("card_html", "raw_json")   # kept when the new sighting has none (card-only page keeps old raw_json)

# v2 -> v3 explicit column renames (the v2 detail fields now live under detail_*)
_V2_RENAMES = {
    "listings": [("advertiser", "card_advertiser"), ("floor", "detail_floor"), ("view", "detail_view"),
                 ("year_built", "detail_year_built"), ("finishing", "detail_finishing"),
                 ("furnished", "detail_furnished"), ("seller_type", "detail_seller_type"),
                 ("payment", "detail_payment"), ("lat", "detail_lat"), ("lng", "detail_lng"),
                 ("full_title", "detail_full_title"), ("posted_date", "detail_posted_date"),
                 ("updated_date", "detail_updated_date"), ("attrs_json", "detail_attrs_json")],
    "price_history": [("price", "price_monthly")],
}


def _upsert_sql() -> str:
    state = ["id", "source", "page", "first_seen", "last_seen", "first_seen_run", "last_seen_run", "active",
             "missed_runs", "delisted_run"]
    cols = ["id", *PARSE_COLS, *_BLOB_PARSE, *state[1:]]
    sets = [f"{c}=excluded.{c}" for c in PARSE_COLS]
    sets += [f"{c}=COALESCE(excluded.{c},{c})" for c in _BLOB_PARSE]
    sets += ["source=excluded.source", "page=excluded.page", "last_seen=excluded.last_seen",
             "last_seen_run=excluded.last_seen_run", "first_seen_run=COALESCE(first_seen_run,excluded.first_seen_run)",
             "active=1", "missed_runs=0", "delisted_run=NULL"]
    return (f"INSERT INTO listings ({','.join(cols)}) VALUES ({','.join(':' + c for c in cols)}) "
            f"ON CONFLICT(id) DO UPDATE SET {', '.join(sets)}")


UPSERT_LISTING = _upsert_sql()
DETAIL_COLS = ["detail_full_title", "detail_posted_date", "detail_updated_date", "detail_area_sqm", "detail_beds",
               "detail_baths", "detail_floor", "detail_view", "detail_year_built", "detail_finishing",
               "detail_furnished", "detail_seller_type", "detail_payment", "detail_lat", "detail_lng",
               "detail_attrs_json"]


class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()
        self.conn.commit()

    # -- schema / migrations
    def _tables(self) -> set[str]:
        return {r["name"] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def _columns(self, table: str) -> set[str]:
        return {r["name"] for r in self.conn.execute(f"PRAGMA table_info({table})")}

    def _backup(self, version: int) -> str:
        self.conn.commit()
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        dest = f"{self.path}.bak-v{version}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        shutil.copy2(self.path, dest)
        log.warning("database backed up to %s before migrating v%d -> v%d", dest, version, SCHEMA_VERSION)
        return dest

    def _add_missing_columns(self, name: str, cols: list[tuple[str, str]]) -> None:
        have = self._columns(name)
        for c, d in cols:
            if c not in have:
                d2 = d.replace("NOT NULL", "").replace("PRIMARY KEY", "").replace("AUTOINCREMENT", "").strip()
                self.conn.execute(f"ALTER TABLE {name} ADD COLUMN {c} {d2 or 'TEXT'}")
                log.info("migrated: added %s.%s", name, c)

    def _migrate_2_to_3(self) -> None:
        for table, renames in _V2_RENAMES.items():
            if table not in self._tables():
                continue
            have = self._columns(table)
            for old, new in renames:
                if old in have and new not in have:
                    self.conn.execute(f"ALTER TABLE {table} RENAME COLUMN {old} TO {new}")
        if "listings" in self._tables():
            # v2 stored free text from the title here. It cannot be trusted; the payload slug replaces it.
            self.conn.execute("UPDATE listings SET property_type=NULL WHERE property_type IS NOT NULL")
            log.warning("v2 property_type values were cleared (they came from seller-written titles). "
                        "Run a fresh `crawl` to fill them from the payload.")

    MIGRATIONS = {2: _migrate_2_to_3}   # key = version being migrated FROM

    def _init_schema(self) -> None:
        tables = self._tables()
        if "meta" in tables:
            r = self.conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
            ver = int(r["value"]) if r else (2 if "listings" in tables else 0)
        else:
            ver = 2 if "listings" in tables else 0
        if ver > SCHEMA_VERSION:
            raise SystemExit(f"database schema v{ver} is newer than this script (v{SCHEMA_VERSION}). Update the script.")
        if 0 < ver < SCHEMA_VERSION:
            self._backup(ver)
            v = ver
            while v < SCHEMA_VERSION:
                self.MIGRATIONS[v](self)
                v += 1
        all_tables = {"listings": LISTING_COLS, **OTHER_TABLES}
        for name, cols in all_tables.items():
            ddl = ", ".join(f"{c} {d}" for c, d in cols)
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({ddl})")
            self._add_missing_columns(name, cols)
        self.conn.executescript(STATIC_INDEXES)
        self.conn.execute("INSERT INTO meta(key,value) VALUES('schema_version',?) "
                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(SCHEMA_VERSION),))

    def close(self):
        self.conn.commit()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def count_listings(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    def meta_get(self, key: str) -> str | None:
        r = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return r["value"] if r else None

    def meta_set(self, key: str, value) -> None:
        self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                          (key, str(value)))

    # -- runs / cycles
    def new_run(self, kind: str, source: str | None) -> int:
        cur = self.conn.execute("INSERT INTO runs(kind,source,started_at,status) VALUES(?,?,?,'running')",
                                (kind, source, now_iso()))
        self.conn.commit()
        return cur.lastrowid

    def end_run(self, run_id: int, **fields):
        fields.setdefault("ended_at", now_iso())
        sets = ", ".join(f"{k}=?" for k in fields)
        self.conn.execute(f"UPDATE runs SET {sets} WHERE id=?", [*fields.values(), run_id])
        self.conn.commit()

    def begin_cycle(self, source: str, fresh: bool, keep_raw: int = 1) -> tuple[int, bool]:
        """Resume the latest unfinished crawl cycle for `source`, or start a new snapshot cycle."""
        row = self.conn.execute("SELECT * FROM runs WHERE kind='crawl' AND source=? ORDER BY id DESC LIMIT 1",
                                (source,)).fetchone()
        if row and not row["complete"] and row["status"] != "finished_complete" and not fresh \
                and not (row["status"] == "finished" and row["stopped_reason"] and "max-pages" not in row["stopped_reason"]
                         and row["complete"] == 0 and row["ended_at"] and False):
            self.conn.execute("UPDATE runs SET status='running', ended_at=NULL WHERE id=?", (row["id"],))
            self.conn.commit()
            return row["id"], True
        self.conn.execute("DELETE FROM pages WHERE source=?", (source,))
        self.conn.execute("DELETE FROM raw_pages WHERE source=?", (source,))
        return self.new_run("crawl", source), False

    def finalize_cycle(self, source: str, run_id: int, natural_end: bool, site_total: int | None, expected_pages: int | None,
                       delist_after: int, allow_delist: bool) -> dict:
        seen = self.conn.execute("SELECT COUNT(*) FROM listings WHERE last_seen_run=?", (run_id,)).fetchone()[0]
        errors = self.conn.execute("SELECT COUNT(*) FROM pages WHERE source=? AND status='error'", (source,)).fetchone()[0]
        ok_pages = self.conn.execute("SELECT COUNT(*) FROM pages WHERE source=? AND status='ok'", (source,)).fetchone()[0]
        prev = self.conn.execute("SELECT seen FROM runs WHERE kind='crawl' AND source=? AND complete=1 AND id<? "
                                 "ORDER BY id DESC LIMIT 1", (source, run_id)).fetchone()
        prev_seen = prev["seen"] if prev and prev["seen"] else None
        reasons = []
        if not natural_end:
            reasons.append("cycle did not reach the natural end of results")
        if errors:
            reasons.append(f"{errors} page error(s)")
        if expected_pages is not None and ok_pages < expected_pages:
            reasons.append(f"fetched {ok_pages} of {expected_pages} expected pages")
        if site_total and seen < MIN_SITE_COVERAGE * site_total:
            reasons.append(f"saw {seen} of {site_total} ({100 * seen / site_total:.1f}%), need >= {MIN_SITE_COVERAGE:.0%}")
        if prev_seen and seen < MIN_PREV_RATIO * prev_seen:
            reasons.append(f"saw {seen}, only {100 * seen / prev_seen:.0f}% of the previous complete cycle ({prev_seen})")
        complete = not reasons
        delisted = 0
        if complete and allow_delist:
            self.conn.execute("UPDATE listings SET missed_runs=missed_runs+1 WHERE source=? AND active=1 "
                              "AND (last_seen_run IS NULL OR last_seen_run!=?)", (source, run_id))
            cur = self.conn.execute("UPDATE listings SET active=0, delisted_run=? WHERE source=? AND active=1 "
                                    "AND missed_runs>=?", (run_id, source, delist_after))
            delisted = cur.rowcount
        elif complete:
            reasons.append("delisting skipped: multi-source (sharded) run")
        self.conn.commit()
        return {"complete": complete, "seen": seen, "delisted": delisted, "page_errors": errors, "ok_pages": ok_pages,
                "expected_pages": expected_pages, "prev_complete_seen": prev_seen, "incomplete_reasons": reasons}

    # -- pages
    def page_row(self, source: str, page: int):
        return self.conn.execute("SELECT * FROM pages WHERE source=? AND page=?", (source, page)).fetchone()

    def record_page(self, source: str, page: int, status: str, n_cards: int, ids: list[int] | None,
                    error: str | None = None, pp: PageParse | None = None):
        self.conn.execute(
            "INSERT INTO pages(source,page,status,n_cards,ids,fetched_at,error,parse_source,payload_n,id_match,ld_match,"
            "n_disagree) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source,page) DO UPDATE SET status=excluded.status,"
            "n_cards=excluded.n_cards,ids=excluded.ids,fetched_at=excluded.fetched_at,error=excluded.error,"
            "parse_source=excluded.parse_source,payload_n=excluded.payload_n,id_match=excluded.id_match,"
            "ld_match=excluded.ld_match,n_disagree=excluded.n_disagree",
            (source, page, status, n_cards, json.dumps(ids) if ids is not None else None, now_iso(), error,
             pp.parse_source if pp else None, len(pp.payload.listings) if pp and pp.payload else None,
             None if not pp or pp.id_match is None else int(pp.id_match),
             None if not pp or pp.ld_match is None else int(pp.ld_match), len(pp.disagreements) if pp else None))

    def save_raw_page(self, source: str, page: int, run_id: int, html: str):
        blob = gzip.compress(scrub_phones(html).encode("utf-8"), 6)
        self.conn.execute("INSERT INTO raw_pages(source,page,run_id,fetched_at,html) VALUES(?,?,?,?,?) "
                          "ON CONFLICT(source,page) DO UPDATE SET run_id=excluded.run_id,fetched_at=excluded.fetched_at,"
                          "html=excluded.html", (source, page, run_id, now_iso(), blob))

    # -- listings
    def upsert_rows(self, source: str, page: int, rows: list[dict], disagreements: list[tuple], run_id: int) -> int:
        ts = now_iso()
        new = 0
        for r in rows:
            prev = self.conn.execute("SELECT id FROM listings WHERE id=?", (r["id"],)).fetchone()
            if prev is None:
                new += 1
            if r["price_amount"] is not None:
                last = self.conn.execute("SELECT price_amount, price_period FROM price_history WHERE id=? "
                                         "ORDER BY rowid DESC LIMIT 1", (r["id"],)).fetchone()
                if last is None or last["price_amount"] != r["price_amount"] or last["price_period"] != r["price_period"]:
                    self.conn.execute("INSERT INTO price_history(id,seen_at,price_amount,price_period,price_monthly,run_id) "
                                      "VALUES(?,?,?,?,?,?)", (r["id"], ts, r["price_amount"], r["price_period"],
                                                              r["price_monthly"], run_id))
            d = {c: r.get(c) for c in ["id", *PARSE_COLS, *_BLOB_PARSE]}
            d.update(source=source, page=page, first_seen=ts, last_seen=ts, first_seen_run=run_id, last_seen_run=run_id,
                     active=1, missed_runs=0, delisted_run=None)
            self.conn.execute(UPSERT_LISTING, d)
        self.conn.execute("DELETE FROM disagreements WHERE run_id=? AND id IN (%s)" % ",".join("?" * len(rows)),
                          [run_id, *[r["id"] for r in rows]]) if rows else None
        for lid, f, pv, cv in disagreements:
            self.conn.execute("INSERT INTO disagreements(run_id,id,field,payload_value,card_value) VALUES(?,?,?,?,?)",
                              (run_id, lid, f, str(pv), str(cv)))
        return new

    def update_parse_cols(self, r: dict, blobs: bool = False) -> None:
        cols = PARSE_COLS + (list(_BLOB_PARSE) if blobs else [])
        sets = ", ".join(f"{c}=?" for c in cols)
        self.conn.execute(f"UPDATE listings SET {sets} WHERE id=?", [*[r.get(c) for c in cols], r["id"]])

    def pending_detail_ids(self, limit: int | None, retry_errors: bool, redo: bool, fields: list[str] | None) -> list[tuple[int, str]]:
        if redo:
            cond = "active=1"
        elif retry_errors:
            cond = "active=1 AND (detail_status IS NULL OR detail_status='error')"
        else:
            cond = "active=1 AND detail_status IS NULL"
        miss = {"posted_date": "posted_date IS NULL", "furnished": "furnished IS NULL",
                "coords": "lat IS NULL", "payment": "payment_method_label IS NULL AND payment_method IS NULL"}
        if fields:
            cond += " AND (" + " OR ".join(miss[f] for f in fields) + ")"
        sql = f"SELECT id,url FROM listings WHERE {cond} ORDER BY id DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [(r["id"], r["url"]) for r in self.conn.execute(sql)]

    def mark_detail(self, lid: int, status: str, deactivate_run: int | None = None):
        self.conn.execute("UPDATE listings SET detail_status=?, detail_fetched_at=? WHERE id=?", (status, now_iso(), lid))
        if deactivate_run is not None:
            self.conn.execute("UPDATE listings SET active=0, delisted_run=? WHERE id=? AND active=1", (deactivate_run, lid))

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
        return {
            "listings": q("SELECT COUNT(*) FROM listings"),
            "active": q("SELECT COUNT(*) FROM listings WHERE active=1"),
            "delisted": q("SELECT COUNT(*) FROM listings WHERE active=0"),
            "with_price": q("SELECT COUNT(*) FROM listings WHERE price_monthly IS NOT NULL"),
            "with_area": q("SELECT COUNT(*) FROM listings WHERE area_sqm IS NOT NULL"),
            "with_type": q("SELECT COUNT(*) FROM listings WHERE property_type IS NOT NULL"),
            "with_location": q("SELECT COUNT(*) FROM listings WHERE location IS NOT NULL"),
            "with_title": q("SELECT COUNT(*) FROM listings WHERE title IS NOT NULL"),
            "with_photo_count": q("SELECT COUNT(*) FROM listings WHERE photo_count IS NOT NULL"),
            "with_advertiser_name": q("SELECT COUNT(*) FROM listings WHERE advertiser_name IS NOT NULL"),
            "residential": q("SELECT COUNT(*) FROM listings WHERE property_type IN (%s)" % ",".join("?" * 0) if False else
                             "SELECT COUNT(*) FROM listings WHERE " + " OR ".join(f"property_type='{t}'" for t in sorted(RESIDENTIAL_TYPES))),
            "residential_with_beds": q("SELECT COUNT(*) FROM listings WHERE beds IS NOT NULL"),
            "residential_with_baths": q("SELECT COUNT(*) FROM listings WHERE baths IS NOT NULL"),
            "card_only_rows": q("SELECT COUNT(*) FROM listings WHERE parse_source='card'"),
            "detail_ok": q("SELECT COUNT(*) FROM listings WHERE detail_status='ok'"),
            "detail_gone": q("SELECT COUNT(*) FROM listings WHERE detail_status='gone'"),
            "detail_error": q("SELECT COUNT(*) FROM listings WHERE detail_status='error'"),
            "detail_pending": q("SELECT COUNT(*) FROM listings WHERE active=1 AND detail_status IS NULL"),
            "pages_ok": q("SELECT COUNT(*) FROM pages WHERE status='ok'"),
            "pages_error": q("SELECT COUNT(*) FROM pages WHERE status='error'"),
            "raw_pages": q("SELECT COUNT(*) FROM raw_pages"),
            "price_changes": q("SELECT COUNT(*) FROM (SELECT id FROM price_history GROUP BY id HAVING COUNT(*)>1)"),
        }


# --------------------------------------------------------------------------- validation
def compute_flags(rows: list[dict]) -> dict[int, list[str]]:
    """Data-quality flags per listing id. Pure function over dict rows (see load_rows)."""
    flags: dict[int, list[str]] = {r["id"]: [] for r in rows}
    for r in rows:
        f = flags[r["id"]]
        if r.get("active") == 0:
            f.append("delisted")
        if r.get("parse_source") == "card":
            f.append("card_only")
        if r.get("is_call_request"):
            f.append("call_for_price")
        if r.get("price_amount") is None:
            if not r.get("is_call_request"):
                f.append("no_price")
        else:
            p = r.get("price_period")
            if p != "month":
                f.append({None: "unknown_price_period", "day": "daily_price", "week": "weekly_price",
                          "year": "yearly_price"}[p])
        if r.get("property_type") is None:
            f.append("unknown_property_type" if r.get("property_type_raw") else "no_property_type")
        ca, da = r.get("area_sqm"), r.get("detail_area_sqm")
        if ca is None and da is None:
            f.append("no_area")
        elif ca and da and abs(da - ca) / ca > 0.10:
            f.append("area_mismatch")
    groups: dict = defaultdict(list)
    for r in rows:
        area = r.get("area_sqm") or r.get("detail_area_sqm")
        if r.get("price_monthly") and area:
            groups[r.get("property_type")].append((r["id"], math.log(r["price_monthly"] / area)))
    for items in groups.values():
        if len(items) < 30:
            continue
        vals = [v for _, v in items]
        med = statistics.median(vals)
        mad = statistics.median([abs(v - med) for v in vals]) * 1.4826
        if mad < 1e-9:
            continue
        for lid, v in items:
            z = (v - med) / mad
            if z > 4:
                flags[lid].append("ppsm_high")
            elif z < -4:
                flags[lid].append("ppsm_low")
    # possible duplicates: same advertiser ID, type, price, area, rooms and area id. Rows with no advertiser id are
    # skipped (NULL is "no data", not a group). Units inside a compound are flagged separately: identical units
    # in one compound are normal.
    dup: dict = defaultdict(list)
    for r in rows:
        if r.get("advertiser_id") is not None and r.get("price_amount") is not None and r.get("area_sqm") is not None:
            key = (r["advertiser_id"], r.get("property_type"), r["price_amount"], r["area_sqm"], r.get("beds_raw"),
                   r.get("baths_raw"), r.get("area_id") or r.get("location"))
            dup[key].append(r)
    for grp in dup.values():
        if len(grp) > 1:
            for r in grp:
                flags[r["id"]].append("possible_dup_compound" if r.get("in_compound") else "possible_dup")
    cg: dict = defaultdict(list)
    for r in rows:
        if r.get("lat") is not None and r.get("lng") is not None:
            cg[(round(r["lat"], 4), round(r["lng"], 4))].append(r["id"])
    for ids in cg.values():
        if len(ids) >= SHARED_COORD_MIN:
            for i in ids:
                flags[i].append("shared_coords")
    return flags


def flag_counts(flags: dict[int, list[str]]) -> dict[str, int]:
    c: dict[str, int] = defaultdict(int)
    for fl in flags.values():
        for f in fl:
            c[f] += 1
    return dict(sorted(c.items(), key=lambda kv: -kv[1]))


COVERAGE_FIELDS = ("price_monthly", "area_sqm", "beds", "baths", "floor", "finish_type", "advertiser_name", "photo_count")


def coverage_by_type(rows: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    by: dict = defaultdict(list)
    for r in rows:
        by[r.get("property_type") or "(none)"].append(r)
    for t, rs in sorted(by.items(), key=lambda kv: -len(kv[1])):
        out[t] = {"n": len(rs), **{f: round(100 * sum(r.get(f) is not None for r in rs) / len(rs), 1) for f in COVERAGE_FIELDS}}
    return out


# --------------------------------------------------------------------------- networking
@dataclass
class Fetched:
    status: int
    text: str
    url: str
    retry_after: float | None = None


class RateLimiter:
    """Global request pacing with jitter. Slows down when penalised, recovers slowly. Never speeds past --rps."""

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
    """Optional fallback for plain-HTTP failures that are NOT bot protection (e.g. a JS-only layout).
    Never exercised against the real site or a mock. Not a block workaround; see README."""

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
        except Exception as e:  # noqa: BLE001
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
        self.metrics: Counter = Counter()

    async def close(self):
        await self.fetcher.close()

    async def _backoff(self, attempt: int, retry_after: float | None, why: str, url: str, base: float | None = None):
        base = base if base is not None else self.args.backoff_base
        delay = retry_after if retry_after is not None else min(180.0, base * 2 ** attempt) * random.uniform(0.7, 1.3)
        delay = min(delay, 300.0)
        self.metrics["retries"] += 1
        log.warning("retry %d/%d in %.1fs: %s  (%s)", attempt + 1, self.args.max_retries, delay, why, url)
        await asyncio.sleep(delay)

    async def fetch(self, url: str) -> Fetched:
        last = "n/a"
        for attempt in range(self.args.max_retries + 1):
            await self.limiter.wait()
            self.metrics["requests"] += 1
            try:
                r = await self.fetcher.get(url)
            except (TransportFailure, asyncio.TimeoutError) as e:
                last = str(e) or type(e).__name__
                self.metrics["transport_errors"] += 1
                self.limiter.penalize()
                if attempt < self.args.max_retries:
                    await self._backoff(attempt, None, last, url)
                continue
            self.metrics[f"http_{r.status}"] += 1
            blocked = r.status in (401, 403) or (r.status in (200, 429, 503) and looks_like_challenge(r.text))
            if blocked:
                self.consec_blocks += 1
                self.metrics["blocks"] += 1
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
        log.info("robots.txt OK for %d checked URL(s) (%d rule lines applied)", len(urls), len(rules.raw))


# --------------------------------------------------------------------------- run report
def flatten_paths(obj, prefix: str = "", out: set | None = None, depth: int = 0) -> set[str]:
    out = set() if out is None else out
    if depth > 4:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            out.add(p)
            flatten_paths(v, p, out, depth + 1)
    elif isinstance(obj, list):
        for v in obj[:3]:
            flatten_paths(v, prefix + "[]", out, depth + 1)
    return out


OBSERVED = {
    "featuredType": lambda p: p.get("featuredType"),
    "category": lambda p: p.get("category"),
    "payment_method": lambda p: p.get("payment_method"),
    "payment_plans[0].duration": lambda p: ((p.get("payment_plans") or [{}])[0] or {}).get("duration"),
    "previewDetails.propertyView": lambda p: (p.get("previewDetails") or {}).get("propertyView"),
    "previewDetails.propertyTypeSlug": lambda p: (p.get("previewDetails") or {}).get("propertyTypeSlug"),
    "isCallRequest": lambda p: p.get("isCallRequest"),
    "is_project_or_unit": lambda p: p.get("is_project_or_unit"),
    "price_is_null_or_zero": lambda p: not p.get("price"),
}


def payload_survey(db: DB, run_id: int) -> dict:
    """Which payload keys are always / sometimes present, and which values occur. Answers the brief's open questions
    from data. Reads the stored raw_json of every listing seen in this run."""
    keys: Counter = Counter()
    obs: dict[str, Counter] = {k: Counter() for k in OBSERVED}
    n = 0
    for r in db.conn.execute("SELECT raw_json FROM listings WHERE last_seen_run=? AND raw_json IS NOT NULL", (run_id,)):
        p = unzjson(r["raw_json"])
        n += 1
        keys.update(flatten_paths(p))
        for k, fn in OBSERVED.items():
            try:
                obs[k][json.dumps(fn(p), ensure_ascii=False)] += 1
            except Exception:  # noqa: BLE001
                obs[k]["<error>"] += 1
    always = sorted(k for k, c in keys.items() if c == n)
    sometimes = {k: f"{c}/{n}" for k, c in sorted(keys.items()) if c != n}
    return {"payload_objects": n, "keys_always_present": always, "keys_sometimes_present": sometimes,
            "observed_values": {k: dict(c.most_common(25)) for k, c in obs.items()},
            "coordinate_keys_seen": sorted(k for k in keys if re.search(r"(^|\.)(lat|lng|lon|latitude|longitude)$", k, re.I)),
            "date_keys_seen": sorted(k for k in keys if re.search(r"(created|posted|published|date)", k, re.I)),
            "furnished_keys_seen": sorted(k for k in keys if re.search(r"furnish", k, re.I))}


def build_run_report(db: DB, run_id: int, eng: Engine | None, info: dict, site_total: int | None, stopped: str) -> dict:
    row = db.conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    pg = db.conn.execute("SELECT COUNT(*) n, SUM(status='ok') ok, SUM(status='error') err, SUM(parse_source='payload') pay,"
                         "SUM(parse_source='card') card, SUM(id_match=0) idm, SUM(ld_match=0) ldm, SUM(n_disagree) dis "
                         "FROM pages WHERE source=?", (row["source"],)).fetchone()
    seen_rows = [dict(r) for r in db.conn.execute(
        "SELECT * FROM listings WHERE last_seen_run=?", (run_id,))]
    n = len(seen_rows) or 1
    cov = {f: round(100 * sum(r.get(f) is not None for r in seen_rows) / n, 2)
           for f in ("title", "price_monthly", "area_sqm", "location", "photo_count", "advertiser_name", "property_type",
                     "advertiser_id", "finish_type", "floor", "photos", "updated_at", "posted_date", "furnished", "lat")}
    res = [r for r in seen_rows if r.get("property_type") in RESIDENTIAL_TYPES]
    cov["beds_residential"] = round(100 * sum(r["beds"] is not None for r in res) / len(res), 2) if res else None
    cov["baths_residential"] = round(100 * sum(r["baths"] is not None for r in res) / len(res), 2) if res else None
    dis = [dict(r) for r in db.conn.execute("SELECT id, field, payload_value, card_value FROM disagreements WHERE run_id=?", (run_id,))]
    by_field = Counter(d["field"] for d in dis)
    unknown = Counter(r["property_type_raw"] for r in seen_rows if r.get("property_type") is None and r.get("property_type_raw"))
    price_pct = 100 * (by_field["price"]) / n
    area_pct = 100 * (by_field["area"]) / n
    report = {
        "run_id": run_id, "script_version": __version__, "source": row["source"], "started_at": row["started_at"],
        "ended_at": now_iso(), "stopped_reason": stopped, "complete": info["complete"],
        "incomplete_reasons": info.get("incomplete_reasons", []),
        "site_total": site_total, "listings_seen": info["seen"], "new_listings": row["new_listings"],
        "delisted_this_run": info["delisted"], "prev_complete_seen": info.get("prev_complete_seen"),
        "pages": {"expected": info.get("expected_pages"), "ok": pg["ok"] or 0, "errors": pg["err"] or 0,
                  "parsed_from_payload": pg["pay"] or 0, "fell_back_to_cards": pg["card"] or 0,
                  "payload_card_id_mismatch": pg["idm"] or 0, "ldjson_card_id_mismatch": pg["ldm"] or 0},
        "engine": dict(eng.metrics) if eng else {},
        "retries": (eng.metrics["retries"] if eng else 0), "blocks": (eng.metrics["blocks"] if eng else 0),
        "payload_vs_card": {"disagreements_total": len(dis), "by_field": dict(by_field),
                            "price_disagree_pct": round(price_pct, 3), "area_disagree_pct": round(area_pct, 3),
                            "listed": dis},
        "coverage_pct": cov, "coverage_by_type": coverage_by_type(seen_rows),
        "unknown_property_slugs": dict(unknown.most_common()),
        "survey": payload_survey(db, run_id),
    }
    return report


def write_run_report(report: dict, runs_dir: str) -> Path:
    d = Path(runs_dir)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{report['run_id']}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return path


# --------------------------------------------------------------------------- crawl
class Crawler:
    def __init__(self, eng: Engine):
        self.eng = eng
        self.db = eng.db
        self.args = eng.args

    async def crawl_source(self, source: str, allow_delist: bool) -> dict:
        run_id, resumed = self.db.begin_cycle(source, fresh=self.args.refresh)
        log.info("%s cycle (run #%d)", "resuming unfinished" if resumed else "starting new", run_id)
        try:
            res = await self._crawl(source, run_id, allow_delist)
        except BaseException as e:  # noqa: BLE001
            self.db.end_run(run_id, status="interrupted", stopped_reason=type(e).__name__)
            raise
        res["run_id"] = run_id
        return res

    async def _crawl(self, source: str, run_id: int, allow_delist: bool) -> dict:
        db, args = self.db, self.args
        total = int(db.meta_get(f"total:{source}") or 0) or None
        per_page = int(db.meta_get(f"per_page:{source}") or 0) or None
        first_row = db.page_row(source, 1)
        first_ids = set(json.loads(first_row["ids"])) if first_row and first_row["ids"] else None
        first_query = db.meta_get(f"query:{source}")
        n, new_total, pages_done, errors_in_row, payload_fail_streak = 1, 0, 0, 0, 0
        started = time.monotonic()
        stopped = "reached --max-pages"
        natural = False
        expected = math.ceil(total / per_page) if total and per_page else None

        while True:
            limit, by_total = args.max_pages, False
            if expected and expected + 5 < limit:
                limit, by_total = expected + 5, True
            if n > limit:
                stopped = "reached expected last page" if by_total else "reached --max-pages"
                natural = by_total
                break
            row = db.page_row(source, n)
            if row and row["status"] == "ok" and n != 1:   # page 1 is always re-fetched: it is the schema canary
                n += 1
                continue

            url = page_url(source, n)
            res = await self.eng.fetch(url)
            if res.status in (404, 410) and n > 1:
                db.record_page(source, n, "empty", 0, [], f"http {res.status}")
                db.commit()
                natural = not (expected and n <= expected)
                stopped = f"HTTP {res.status} at page {n} (end of results)"
                break
            if res.status != 200:
                db.record_page(source, n, "error", 0, None, f"http {res.status}")
                db.commit()
                errors_in_row += 1
                log.error("page %d: HTTP %d", n, res.status)
                if errors_in_row >= 3:
                    stopped = "3 consecutive page errors"
                    break
                n += 1
                continue

            pp = parse_page(res.text, url)
            if n == 1:
                problems = schema_canary(pp)
                if problems:
                    (Path(args.samples_dir) / "canary_failed_page1.html").parent.mkdir(parents=True, exist_ok=True)
                    (Path(args.samples_dir) / "canary_failed_page1.html").write_text(res.text, encoding="utf-8")
                    raise SchemaChanged(
                        "page 1 no longer looks like what the parser expects:\n  - " + "\n  - ".join(problems) +
                        f"\nThe page was saved to {args.samples_dir}/canary_failed_page1.html. Nothing was written to the "
                        "database. Run `probe` and compare with the fixtures.")
                if pp.total:
                    total = pp.total
                    db.meta_set(f"total:{source}", total)
                    if pp.ld and pp.ld.total and pp.ld.total != pp.total:
                        log.warning("total mismatch: header says %s, ld+json numberOfItems says %s", pp.total, pp.ld.total)
                per_page = len(pp.cards)
                db.meta_set(f"per_page:{source}", per_page)
                expected = math.ceil(total / per_page) if total and per_page else None
                if pp.payload and pp.payload.query:
                    if first_query and first_query != pp.payload.query:
                        log.warning("initialQuery changed since the last cycle: %r -> %r", first_query, pp.payload.query)
                    first_query = pp.payload.query
                    db.meta_set(f"query:{source}", first_query)
                    log.info("initialQuery: %s", first_query)

            for note in pp.notes:
                log.warning("page %d: %s", n, note)
            if pp.cards and pp.payload is None:
                payload_fail_streak += 1
                if payload_fail_streak >= MAX_PAYLOAD_FAIL_STREAK:
                    raise SchemaChanged(f"no usable embedded payload on {payload_fail_streak} pages in a row "
                                        f"(last: page {n}). The site probably changed its page structure.")
            elif pp.payload is not None:
                payload_fail_streak = 0
            if pp.payload and pp.payload.query and first_query:
                strip = lambda q: sorted(x for x in q.split("&") if not x.startswith("page="))  # noqa: E731
                if strip(pp.payload.query) != strip(first_query):
                    log.warning("page %d: initialQuery differs from page 1: %r", n, pp.payload.query)

            if not pp.cards:
                expected_more = bool(expected and n <= expected)
                if expected_more:
                    for k in range(2):
                        log.warning("page %d empty but ~%s pages expected; re-checking (%d/2)", n, expected, k + 1)
                        await asyncio.sleep(args.backoff_base * 1.5)
                        res = await self.eng.fetch(url)
                        pp = parse_page(res.text, url)
                        if pp.cards:
                            break
                if not pp.cards:
                    db.record_page(source, n, "empty", 0, [])
                    db.commit()
                    if expected_more:
                        log.warning("results ended at page %d but ~%d pages were expected -> probable pagination cap. "
                                    "Use `areas` + `crawl --url-file` to shard by area.", n, expected)
                    natural = not expected_more
                    stopped = f"empty page {n}"
                    break

            ids = [c.id for c in pp.cards]
            if n > 1 and first_ids and set(ids) == first_ids:
                db.record_page(source, n, "wrap", len(ids), ids)
                db.commit()
                log.warning("page %d returned the same listings as page 1 (pagination wraps).", n)
                natural = False
                stopped = f"pagination wraps at page {n}"
                break

            if args.save_raw:
                db.save_raw_page(source, n, run_id, res.text)
            new = db.upsert_rows(source, n, pp.rows, pp.disagreements, run_id)
            db.record_page(source, n, "ok", len(ids), ids, pp=pp)
            db.commit()
            if n == 1:
                first_ids = set(ids)
            errors_in_row = 0
            new_total += new
            pages_done += 1
            elapsed = time.monotonic() - started
            rate = pages_done / elapsed if elapsed else 0
            eta = (expected - n) / rate if rate and expected else float("nan")
            log.info("page %d/%s  cards=%d new=%d disagree=%d src=%s db=%d  eta=%s", n, expected or "?", len(ids), new,
                     len(pp.disagreements), pp.parse_source, db.count_listings(), fmt_eta(eta))
            n += 1

        info = db.finalize_cycle(source, run_id, natural, total, expected, args.delist_after, allow_delist)
        prev_new = db.conn.execute("SELECT new_listings FROM runs WHERE id=?", (run_id,)).fetchone()[0] or 0
        db.end_run(run_id, status="finished", complete=int(info["complete"]), site_total=total, stopped_reason=stopped,
                   seen=info["seen"], new_listings=prev_new + new_total, stats_json=json.dumps(info))
        report = build_run_report(db, run_id, self.eng, info, total, stopped)
        rpath = write_run_report(report, args.runs_dir)
        db.end_run(run_id, stats_json=json.dumps({**info, "report": str(rpath)}))
        in_db = db.conn.execute("SELECT COUNT(*) FROM listings WHERE source=?", (source,)).fetchone()[0]
        return {"source": source, "stopped": stopped, "natural": natural, "new": new_total, "in_db": in_db,
                "site_total": total, "report": report, "report_path": str(rpath), **info}


# --------------------------------------------------------------------------- details (phase 4)
class DetailRunner:
    def __init__(self, eng: Engine):
        self.eng = eng
        self.db = eng.db
        self.args = eng.args

    async def run(self) -> dict:
        fields = [f for f in (self.args.fields or "").split(",") if f] or None
        todo = self.db.pending_detail_ids(self.args.limit, self.args.retry_errors, self.args.redo, fields)
        stats = {"ok": 0, "gone": 0, "deactivated": 0, "error": 0, "todo": len(todo)}
        run_id = self.db.new_run("details", None)
        if not todo:
            self.db.end_run(run_id, status="finished", complete=1, stats_json=json.dumps(stats))
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
                if r.status in (404, 410):
                    self.db.mark_detail(lid, "gone", deactivate_run=run_id)   # confirmed gone: strongest delisting signal
                    stats["gone"] += 1
                    stats["deactivated"] += 1
                elif r.status == 200 and "/listing/" not in final_path:
                    self.db.mark_detail(lid, "gone")   # redirected away: not confirmed, leave active
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
            self.db.end_run(run_id, status="interrupted" if blocked else "finished", complete=0 if blocked else 1,
                            stats_json=json.dumps(stats))
        if blocked:
            raise blocked[0]
        return stats


def detail_plan(db: DB) -> dict:
    """The phase-4 decision numbers: which target fields the payload left empty, and the cost of fetching them."""
    q = lambda sql: db.conn.execute(sql).fetchone()[0]  # noqa: E731
    act = q("SELECT COUNT(*) FROM listings WHERE active=1")
    plan = {"active_listings": act}
    for name, cond in (("posted_date", "posted_date IS NULL"), ("furnished", "furnished IS NULL"),
                       ("coordinates", "lat IS NULL"), ("payment_method", "payment_method IS NULL AND payment_method_label IS NULL"),
                       ("year_built", "year_built IS NULL"), ("floor", "floor IS NULL")):
        plan[f"missing_{name}"] = q(f"SELECT COUNT(*) FROM listings WHERE active=1 AND {cond}")
    plan["detail_ok_so_far"] = q("SELECT COUNT(*) FROM listings WHERE detail_status='ok'")
    plan["detail_page_cost"] = "about 357 KB per page uncompressed; at 1 req/s the full pass is act seconds " \
                               f"(~{fmt_eta(act)}) and ~{act * 357 / 1024 / 1024:.1f} GB"
    return plan


# --------------------------------------------------------------------------- export
# (key, header, arrow type, source, meaning, how NULL is treated)
_COLS = [
    ("id", "ID", "int64", "payload", "Aqarmap listing id (primary key)", "never null"),
    ("url", "URL", "string", "payload slug / card link", "Canonical listing URL", "never null"),
    ("title", "Title", "string", "payload", "Raw seller-entered title; never used to derive the type", "no data"),
    ("property_type", "Type", "string", "payload previewDetails.propertyTypeSlug",
     "Canonical property type slug", "slug missing or not in the canonical set (see Flags)"),
    ("property_type_raw", "Type (raw slug)", "string", "payload", "Slug exactly as the site sent it", "no data"),
    ("price_monthly", "Price (EGP/month)", "float64", "payload price + card period",
     "Listed price normalised to EGP/month (day x30, week x52/12, year /12; see Flags)", "no price, call-for-price or unknown period"),
    ("price_amount", "Listed price", "float64", "payload price", "Price as listed, in its own period", "no price / call for price"),
    ("price_period", "Price period", "string", "card <data> text", "month | day | week | year", "period not shown"),
    ("daily_rate", "Daily rate (EGP)", "float64", "card", "Explicit per-day price shown on the listing", "no per-day price shown"),
    ("area_sqm", "Area (m²)", "float64", "payload area, card fallback", "Area in square metres", "no data"),
    ("price_per_sqm", "Price per m²", "float64", "derived", "price_monthly / area_sqm", "either input is null"),
    ("beds", "Beds", "int64", "payload attributes.rooms", "Bedrooms, residential types only", "non-residential type, unknown type or no data"),
    ("baths", "Baths", "int64", "payload attributes.baths", "Bathrooms, residential types only", "non-residential type, unknown type or no data"),
    ("beds_raw", "Beds (raw)", "string", "payload", "Seller-entered rooms value for every type", "no data"),
    ("baths_raw", "Baths (raw)", "string", "payload", "Seller-entered baths value for every type", "no data"),
    ("floor", "Floor", "string", "payload attributes.floor, detail fallback", "Floor as entered", "no data"),
    ("finish_type", "Finish type", "string", "payload attributes.finishType, detail fallback", "Raw enum string, e.g. SUPER_LUX", "no data"),
    ("year_built", "Year built", "string", "payload previewDetails.yearBuilt, detail fallback", "Year built", "empty string in the payload"),
    ("view_code", "View code", "int64", "payload previewDetails.propertyView", "Numeric view code; no mapping is assumed", "no data"),
    ("district", "District", "string", "payload location path, card fallback", "District name", "no data"),
    ("area_name", "Area / compound", "string", "payload location path, card fallback", "Most specific location name", "listing sits at district level"),
    ("district_id", "District ID", "int64", "payload location.parent", "Location id of the district", "parent is a group such as 'Compounds in X'"),
    ("area_id", "Area ID", "int64", "payload location.id", "Location id of the area", "listing sits at district level"),
    ("advertiser_id", "Advertiser ID", "int64", "payload user.id", "Aqarmap user id of the advertiser", "no data (never means owner)"),
    ("advertiser_name", "Advertiser", "string", "payload user.fullName, card logo fallback", "Advertiser display name", "no data (never means owner)"),
    ("has_paid_logo", "Paid logo", "int64", "payload user.has_active_logo_service", "1 if the agency pays for the card logo", "no data"),
    ("featured_type", "Featured type", "string", "payload featuredType", "Promotion flag, e.g. sponsored", "not promoted"),
    ("photo_count", "Photos", "int64", "payload photosCount, card fallback", "Number of photos", "no data"),
    ("photos", "Photo URLs", "string", "payload previewDetails.largePhotos", "JSON list of large photo URLs", "no data"),
    ("updated_at", "Updated at", "string", "payload previewDetails.updatedAt", "Last update, ISO with offset. Not the posting date", "no data"),
    ("posted_date", "Posted", "string", "payload if present, else detail page", "Posting date. Never filled from updated_at", "not available from any source"),
    ("furnished", "Furnished", "string", "payload if present, else detail page", "yes / no / raw detail text", "not available from any source"),
    ("lat", "Lat", "float64", "payload if present, else detail page", "Latitude inside the Alexandria box; shared-centroid rows dropped", "no coordinates or centroid"),
    ("lng", "Lng", "float64", "payload if present, else detail page", "Longitude, same rules as Lat", "no coordinates or centroid"),
    ("payment_method_label", "Payment method", "string", "payload previewDetails.paymentMethodLabel", "Raw label key", "no data"),
    ("category", "Category", "string", "payload category", "Raw site category, meaning unconfirmed", "no data"),
    ("detail_seller_type", "Seller type (detail)", "string", "detail page", "Detail-page seller taxonomy (e.g. Developer); differs from Advertiser", "detail page not fetched"),
    ("parse_source", "Parsed from", "string", "crawl", "payload | card (page fell back to cards)", "never null"),
    ("first_seen", "First seen", "string", "crawl", "UTC time of first sighting", "never null"),
    ("last_seen", "Last seen", "string", "crawl", "UTC time of latest sighting", "never null"),
    ("active", "Active", "bool", "crawl", "False after delisting rules or a confirmed 404/410", "never null"),
    ("flags", "Flags", "string", "validate", "Semicolon-separated data-quality flags", "empty = no flags"),
]
EXPORT_COLUMNS = [(k, h) for k, h, *_ in _COLS]
TEXT_COLS = {k for k, _, t, *_ in _COLS if t == "string" and k not in ("url", "photos", "first_seen", "last_seen")}
_LOAD_COLS = [c for c, _ in LISTING_COLS if c not in ("card_html", "card_text", "raw_json", "detail_text", "detail_attrs_json")]


def load_rows(db: DB, with_text: bool = False) -> list[dict]:
    sel = ", ".join(_LOAD_COLS + (["detail_text"] if with_text else []))
    rows = []
    for r in db.conn.execute(f"SELECT {sel} FROM listings ORDER BY id"):
        d = dict(r)
        d["photos_list"] = json.loads(d["photos"]) if d.get("photos") else []
        if with_text:
            blob = d.get("detail_text")
            d["detail_text"] = zlib.decompress(blob).decode("utf-8")[:32000] if blob else None
        rows.append(d)
    flags = compute_flags(rows)
    for d in rows:
        d["flags_list"] = flags[d["id"]]
        d["flags"] = ";".join(flags[d["id"]])
        # payload / card values win; detail values only fill gaps (the raw detail_* columns stay in the DB)
        for c, dc in (("floor", "detail_floor"), ("finish_type", "detail_finishing"), ("year_built", "detail_year_built"),
                      ("posted_date", "detail_posted_date"), ("furnished", "detail_furnished"), ("area_sqm", "detail_area_sqm")):
            if d.get(c) is None:
                d[c] = d.get(dc)
        if d.get("lat") is None and "shared_coords" not in flags[d["id"]]:
            d["lat"], d["lng"] = d.get("detail_lat"), d.get("detail_lng")
        elif "shared_coords" in flags[d["id"]]:
            d["lat"] = d["lng"] = None
        d["price_per_sqm"] = round(d["price_monthly"] / d["area_sqm"], 1) if d.get("price_monthly") and d.get("area_sqm") else None
        d["active"] = bool(d.get("active"))
    return rows


def _cell(r: dict, c: str):
    v = r.get(c)
    if c == "active":
        v = "yes" if v else "no"
    return safe_text(v) if c in TEXT_COLS else v


def atomic_write(path: Path, writer) -> None:
    """Write to a temp file next to the target, then rename. A target held open (Excel on Windows) fails the rename;
    that becomes an ExportLocked error with instructions instead of a traceback."""
    tmp = path.with_name(path.name + ".tmp")
    try:
        writer(tmp)
        os.replace(tmp, path)
    except PermissionError as e:
        tmp.unlink(missing_ok=True)
        raise ExportLocked(f"cannot write {path}: the file is open in another program (usually Excel). "
                           "Close it and run the export again.") from e
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def export_csv(rows: list[dict], path: Path, with_text: bool):
    cols = EXPORT_COLUMNS + ([("detail_text", "Detail text")] if with_text else [])

    def w(p: Path):
        with open(p, "w", newline="", encoding="utf-8-sig") as f:   # BOM so Excel reads Arabic correctly
            wr = csv.writer(f)
            wr.writerow([h for _, h in cols])
            for r in rows:
                wr.writerow([_cell(r, c) for c, _ in cols])
    atomic_write(path, w)


def export_jsonl(rows: list[dict], path: Path):
    def w(p: Path):
        with open(p, "w", encoding="utf-8") as f:
            for r in rows:
                o = {k: r.get(k) for k, _ in EXPORT_COLUMNS}
                o["photos"] = r.get("photos_list")
                o["flags"] = r.get("flags_list")
                f.write(json.dumps(o, ensure_ascii=False) + "\n")
    atomic_write(path, w)


def export_parquet(rows: list[dict], path: Path):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        raise SystemExit("Parquet needs pyarrow: pip install pyarrow") from e
    types = {"int64": pa.int64(), "float64": pa.float64(), "string": pa.string(), "bool": pa.bool_()}
    schema = pa.schema([(k, types[t]) for k, _, t, *_ in _COLS])   # list columns are already JSON strings
    data = {k: [r.get(k) for r in rows] for k, *_ in _COLS}
    table = pa.Table.from_pydict(data, schema=schema)
    atomic_write(path, lambda p: pq.write_table(table, p))


def export_xlsx(db: DB, rows: list[dict], path: Path, stats: dict, with_text: bool):
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
        ws.append([_cell(r, c) for c, _ in cols])
        ws.cell(row=ws.max_row, column=idx["url"]).hyperlink = r["url"]
    widths = {"id": 10, "url": 38, "title": 46, "district": 20, "area_name": 28, "advertiser_name": 22, "photos": 30,
              "flags": 26, "detail_text": 50}
    for c, i in idx.items():
        ws.column_dimensions[get_column_letter(i)].width = widths.get(c, 15)
    for c, fmt in (("price_monthly", "#,##0"), ("price_amount", "#,##0"), ("daily_rate", "#,##0"),
                   ("area_sqm", "0.0"), ("price_per_sqm", "#,##0")):
        for row in ws.iter_rows(min_row=2, min_col=idx[c], max_col=idx[c]):
            row[0].number_format = fmt
    ws.freeze_panes = "C2"
    ws.auto_filter.ref = ws.dimensions

    changes = db.conn.execute(
        "SELECT ph.id, l.url, l.title, ph.seen_at, ph.price_amount, ph.price_period, ph.price_monthly, ph.run_id "
        "FROM price_history ph JOIN listings l ON l.id=ph.id "
        "WHERE ph.id IN (SELECT id FROM price_history GROUP BY id HAVING COUNT(*)>1) ORDER BY ph.id, ph.seen_at").fetchall()
    if changes:
        pc = wb.create_sheet("Price changes")
        pc.append(["ID", "URL", "Title", "Seen at (UTC)", "Listed price", "Period", "Price (EGP/month)", "Run"])
        for c in pc[1]:
            c.font = Font(bold=True)
        for r in changes:
            pc.append([r["id"], r["url"], safe_text(r["title"]), r["seen_at"], r["price_amount"], r["price_period"],
                       r["price_monthly"], r["run_id"]])
        pc.freeze_panes = "A2"
        pc.column_dimensions["B"].width = 38
        pc.column_dimensions["C"].width = 40
        pc.column_dimensions["D"].width = 22

    s = wb.create_sheet("Summary")
    s.append(["Metric", "Value"])
    s["A1"].font = s["B1"].font = Font(bold=True)
    s.append(["Generated (UTC)", now_iso()])
    s.append(["Script version", __version__])
    for k, v in stats.items():
        s.append([k, v])
    s.append([])
    s.append(["Flag", "Listings"])
    for k, v in flag_counts({r["id"]: r["flags_list"] for r in rows}).items():
        s.append([k, v])
    s.column_dimensions["A"].width = 28

    dd = wb.create_sheet("Data dictionary")
    dd.append(["Column", "Header", "Type", "Source", "Meaning", "NULL means"])
    for c in dd[1]:
        c.font = Font(bold=True)
    for k, h, t, src, meaning, nulls in _COLS:
        dd.append([k, h, t, src, meaning, nulls])
    for col, w in zip("ABCDEF", (20, 22, 9, 42, 70, 52)):
        dd.column_dimensions[col].width = w
    dd.freeze_panes = "A2"
    atomic_write(path, lambda p: wb.save(p))


# --------------------------------------------------------------------------- commands
def load_sources(args) -> list[str]:
    urls = list(args.url or [])
    if getattr(args, "url_file", None):
        urls += [x.strip() for x in Path(args.url_file).read_text(encoding="utf-8").splitlines()
                 if x.strip() and not x.startswith("#")]
    params = getattr(args, "param", None)
    return [source_key(add_params(u, params)) for u in dict.fromkeys(urls or [DEFAULT_URL])]


def print_quality(stats: dict):
    n = stats["listings"] or 1
    pct = lambda k: f"{stats[k]} ({100 * stats[k] / n:.0f}%)"  # noqa: E731
    res = stats["residential"] or 1
    log.info("quality: listings=%d active=%d  price=%s  area=%s  type=%s  location=%s  photos=%s  advertiser=%s  "
             "beds(residential)=%d/%d  baths(residential)=%d/%d  card_only=%d",
             stats["listings"], stats["active"], pct("with_price"), pct("with_area"), pct("with_type"),
             pct("with_location"), pct("with_photo_count"), pct("with_advertiser_name"), stats["residential_with_beds"],
             res, stats["residential_with_baths"], res, stats["card_only_rows"])


def refresh_flags(db: DB) -> dict[str, int]:
    rows = load_rows(db)
    flags = compute_flags(rows)
    db.conn.executemany("UPDATE listings SET flags=? WHERE id=?", [(";".join(v), k) for k, v in flags.items()])
    db.commit()
    return flag_counts(flags)


async def cmd_crawl(args, eng: Engine) -> int:
    sources = load_sources(args)
    await eng.ensure_robots([u for s in sources for u in (page_url(s, 1), page_url(s, 2))])
    crawler = Crawler(eng)
    allow_delist = len(sources) == 1 and not args.url_file
    code = EXIT_OK
    for p in range(max(1, args.passes)):
        if p:
            log.info("=== pass %d of %d ===", p + 1, args.passes)
        results = []
        for s in sources:
            log.info("crawling %s", s)
            results.append(await crawler.crawl_source(s, allow_delist))
        for r in results:
            rep = r["report"]
            log.info("source done: stopped: %s | new: %d | in DB: %d | site says: %s | cycle complete: %s | delisted now: %d "
                     "| payload-vs-card disagreements: %d (price %.2f%%, area %.2f%%) | report: %s",
                     r["stopped"], r["new"], r["in_db"], r["site_total"], "yes" if r["complete"] else "NO", r["delisted"],
                     rep["payload_vs_card"]["disagreements_total"], rep["payload_vs_card"]["price_disagree_pct"],
                     rep["payload_vs_card"]["area_disagree_pct"], r["report_path"])
            if not r["complete"]:
                for why in r["incomplete_reasons"]:
                    log.info("  not complete: %s", why)
            # coverage advice only when the cycle actually ran to its natural end
            if r["natural"] and r["site_total"] and r["seen"] < 0.95 * r["site_total"]:
                log.warning("cycle ended naturally but saw only %d of ~%d listings (%.0f%%). Inventory shifts while crawling; "
                            "run another pass (--passes 2).", r["seen"], r["site_total"], 100 * r["seen"] / r["site_total"])
            if not r["natural"] and "pagination" in r["stopped"]:
                log.warning("results ended early. If `probe --deep` shows the site now caps pagination, shard with "
                            "`areas` + `--url-file` (fallback path, no delisting).")
            if not r["complete"] and "max-pages" not in r["stopped"]:
                code = EXIT_INCOMPLETE
        if not all(r["complete"] for r in results):
            break
    print_quality(eng.db.stats())
    counts = refresh_flags(eng.db)
    log.info("flags: %s", ", ".join(f"{k}={v}" for k, v in counts.items()) or "none")
    return code


async def cmd_details(args, eng: Engine) -> int:
    if args.plan:
        print(json.dumps(detail_plan(eng.db), indent=2))
        return EXIT_OK
    sources = load_sources(args)
    await eng.ensure_robots([page_url(sources[0], 1), urljoin(sources[0], "/en/listing/1-for-rent-x/")])
    stats = await DetailRunner(eng).run()
    log.info("details finished: %s", stats)
    return EXIT_OK


async def deep_check(eng: Engine, source: str, pr1: PageParse) -> None:
    per, total = len(pr1.cards), pr1.total
    print("\n=== PAGINATION DEPTH CHECK")
    if not (per and total):
        print("cannot run: page 1 gave no card count or no total")
        return
    last = math.ceil(total / per)
    pages = sorted({p for p in (2, max(2, last // 4), last // 2, last - 1, last, last + 1) if 2 <= p <= last + 1})
    first_ids = {c.id for c in pr1.cards}
    empty_mid, reached_last = [], None
    for p in pages:
        r = await eng.fetch(page_url(source, p))
        pr = parse_listing_page(r.text, page_url(source, p)) if r.status == 200 else PageResult([], None)
        overlap = len({c.id for c in pr.cards} & first_ids)
        print(f"page {p}/{last}: HTTP {r.status}  cards={len(pr.cards)}  overlap_with_page1={overlap}")
        if p < last and not pr.cards:
            empty_mid.append(p)
        if p == last:
            reached_last = bool(pr.cards) and overlap == 0
    if empty_mid:
        print(f"VERDICT: pages {empty_mid} are empty before the expected last page -> pagination looks CAPPED.")
    elif reached_last:
        print(f"VERDICT: last expected page ({last}) returned fresh listings -> pagination looks uncapped.")
    else:
        print("VERDICT: inconclusive; check the numbers above.")


async def cmd_probe(args, eng: Engine) -> int:
    source = load_sources(args)[0]
    outdir = Path(args.samples_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    await eng.ensure_robots([page_url(source, 1), page_url(source, 2)])
    url = page_url(source, 1)
    r = await eng.fetch(url)
    (outdir / "page1.html").write_text(r.text, encoding="utf-8")
    pp = parse_page(r.text, url)
    print(f"\n=== LIST PAGE  {url}\nHTTP {r.status}  {len(r.text):,} bytes  saved: {outdir / 'page1.html'}")
    print(f"site total: {pp.total}   cards: {len(pp.cards)}   payload objects: {len(pp.payload.listings) if pp.payload else 0}"
          f"   parsed from: {pp.parse_source}")
    print(f"payload ids == card ids: {pp.id_match}   ld+json ids == card ids: {pp.ld_match}   "
          f"ld+json total: {pp.ld.total if pp.ld else None}")
    print(f"initialQuery: {pp.payload.query if pp.payload else None}")
    for note in pp.notes:
        print("  note:", note)
    problems = schema_canary(pp)
    print("SCHEMA CANARY:", "OK" if not problems else "FAILED\n  - " + "\n  - ".join(problems))
    print(f"payload vs card disagreements on page 1: {len(pp.disagreements)}  {pp.disagreements[:6]}")
    if pp.rows:
        fields = ["property_type", "price_monthly", "area_sqm", "beds", "baths", "floor", "finish_type", "advertiser_id",
                  "advertiser_name", "photo_count", "updated_at", "district", "area_name", "posted_date", "furnished", "lat"]
        print("field coverage on page 1:", {f: sum(x.get(f) is not None for x in pp.rows) for f in fields},
              f"of {len(pp.rows)}")
        for row in pp.rows[:3]:
            print(json.dumps({k: v for k, v in row.items() if k not in ("card_html", "raw_json", "card_text")},
                             ensure_ascii=False, indent=2, default=str))
    print("\n=== SORT CONTROL HINTS (open question 1)")
    for h in sort_hints(r.text) or ["(none found in the HTML or the RSC stream)"]:
        print(" ", h)
    if args.deep and pp.cards:
        await deep_check(eng, source, pp)
    detail_url = args.listing_url or (pp.cards[0].url if pp.cards and not args.no_detail else None)
    if detail_url:
        dr = await eng.fetch(detail_url)
        (outdir / "detail.html").write_text(dr.text, encoding="utf-8")
        print(f"\n=== DETAIL PAGE  {detail_url}\nHTTP {dr.status}  {len(dr.text):,} bytes  saved: {outdir / 'detail.html'}")
        if dr.status == 200:
            d = parse_detail(dr.text, detail_url)
            print(json.dumps({k: v for k, v in d.items() if k != "detail_text"}, ensure_ascii=False, indent=2))
    print(f"\nTo build real test fixtures: python tools/make_fixtures.py {outdir}/page1.html")
    return EXIT_OK if pp.cards and not problems else EXIT_STRUCTURE


_AREA_URL_RE = re.compile(r"/for-rent/property-type/alexandria/[^/?#]+/?$")


async def cmd_areas(args, eng: Engine) -> int:
    source = load_sources(args)[0]
    found: dict[str, int] = {}
    for r in eng.db.conn.execute("SELECT district_url u, COUNT(*) n FROM listings WHERE district_url IS NOT NULL GROUP BY district_url"):
        found[r["u"]] = r["n"]
    if not found:
        r = await eng.fetch(page_url(source, 1))
        for a in HTMLParser(r.text).css("a[href*='/for-rent/']"):
            h = a.attributes.get("href") or ""
            if "/listing/" in h or "?" in h:
                continue
            u = urljoin(source, h)
            if _AREA_URL_RE.search(u):
                found.setdefault(u, 0)
    for u, n in sorted(found.items(), key=lambda kv: -kv[1]):
        print(u)
    log.info("%d district URL(s). Fallback only: pagination is uncapped on the live site. Sharded runs never delist.", len(found))
    return EXIT_OK if found else EXIT_ERROR


def cmd_reparse(args, db: DB) -> int:
    """Rebuild every parsed column offline. `pages` re-reads raw_pages (HTML); `listings` rebuilds from each row's
    stored raw_json + card_html; `details` re-maps stored detail text. Crawl-state columns are never touched."""
    what = args.what
    n = 0
    pages = db.conn.execute("SELECT source, page, html FROM raw_pages ORDER BY source, page").fetchall()
    if what in ("pages", "all") and pages:
        agree = {"pages": 0, "payload": 0, "card_fallback": 0, "disagreements": 0}
        for r in pages:
            html = gzip.decompress(r["html"]).decode("utf-8")
            pp = parse_page(html, page_url(r["source"], r["page"]))
            for row in pp.rows:
                if db.conn.execute("SELECT 1 FROM listings WHERE id=?", (row["id"],)).fetchone():
                    db.update_parse_cols(row, blobs=True)
                    n += 1
            agree["pages"] += 1
            agree["payload" if pp.parse_source == "payload" else "card_fallback"] += 1
            agree["disagreements"] += len(pp.disagreements)
            log.debug("page %s/%s: %s ids_match=%s ld_match=%s disagree=%d", r["source"], r["page"], pp.parse_source,
                      pp.id_match, pp.ld_match, len(pp.disagreements))
        log.info("re-parsed %d listing(s) from %d stored page(s): %s", n, agree["pages"], agree)
    elif what in ("listings", "all", "pages", "payload", "cards"):
        if what == "pages":
            log.warning("no raw pages stored (crawl with --save-raw); falling back to per-listing data")
        for r in db.conn.execute("SELECT id, url, card_html, raw_json FROM listings").fetchall():
            payload = unzjson(r["raw_json"])
            card = None
            if r["card_html"]:
                card = parse_card_fragment(zlib.decompress(r["card_html"]).decode("utf-8"), r["id"], r["url"])
            if payload is None and card is None:
                continue
            row, _ = build_row(payload, card, "{0.scheme}://{0.netloc}".format(urlparse(r["url"])))
            db.update_parse_cols(row, blobs=False)
            n += 1
        log.info("re-parsed %d listing(s) from stored raw_json + card_html", n)
    if what in ("details", "all"):
        m = 0
        for r in db.conn.execute("SELECT id, detail_text FROM listings WHERE detail_text IS NOT NULL").fetchall():
            d = reparse_detail_from_text(zlib.decompress(r["detail_text"]).decode("utf-8"))
            if d:
                db.conn.execute(f"UPDATE listings SET {', '.join(f'{k}=?' for k in d)} WHERE id=?", [*d.values(), r["id"]])
                m += 1
        log.info("re-parsed %d detail text(s) (text-based pairs only)", m)
    db.commit()
    counts = refresh_flags(db)
    print_quality(db.stats())
    log.info("flags: %s", ", ".join(f"{k}={v}" for k, v in counts.items()) or "none")
    return EXIT_OK


def cmd_validate(args, db: DB) -> int:
    rows = load_rows(db)
    if not rows:
        log.error("no listings in the database; run `crawl` first")
        return EXIT_ERROR
    flags = compute_flags(rows)
    counts = flag_counts(flags)
    print(f"{len(rows)} listings, {sum(1 for f in flags.values() if f)} with at least one flag")
    for k, v in counts.items():
        print(f"  {k:26s} {v}")
    print("\ncoverage by property type (% non-null):")
    cov = coverage_by_type(rows)
    hdr = ["type", "n", *COVERAGE_FIELDS]
    print("  " + "  ".join(f"{h:>14s}" if i else f"{h:20s}" for i, h in enumerate(hdr)))
    for t, d in cov.items():
        print("  " + f"{t:20s}" + "  ".join(f"{d[h]:>14}" for h in hdr[1:]))
    unknown = Counter(r["property_type_raw"] for r in rows if r["property_type"] is None and r.get("property_type_raw"))
    if unknown:
        print("\nunknown property slugs (add them to RESIDENTIAL_TYPES / NON_RESIDENTIAL_TYPES if legitimate):")
        for s, c in unknown.most_common():
            print(f"  {s!r}: {c}")
    dis = db.conn.execute("SELECT field, COUNT(*) c FROM disagreements WHERE run_id=(SELECT MAX(run_id) FROM disagreements) GROUP BY field").fetchall()
    if dis:
        print("\npayload vs card disagreements (latest run with any): " + ", ".join(f"{d['field']}={d['c']}" for d in dis))
    if args.out:
        by_id = {r["id"]: r for r in rows}
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["id", "url", "title", "property_type", "price_amount", "price_period", "area_sqm", "flags"])
            for lid, fl in flags.items():
                if fl:
                    r = by_id[lid]
                    w.writerow([lid, r["url"], safe_text(r["title"]), r["property_type"], r["price_amount"],
                                r["price_period"], r["area_sqm"], ";".join(fl)])
        log.info("wrote %s", args.out)
    refresh_flags(db)
    return EXIT_OK


def cmd_diff(args, db: DB) -> int:
    done = [r["id"] for r in db.conn.execute("SELECT id FROM runs WHERE kind='crawl' AND complete=1 ORDER BY id")]
    a = args.from_run if args.from_run else (done[-2] if len(done) >= 2 else None)
    b = args.to_run if args.to_run else (done[-1] if done else None)
    if a is None or b is None:
        log.error("need two complete crawl runs (have %d). Pass --from and --to run ids explicitly if you want others.", len(done))
        return EXIT_ERROR
    q = db.conn.execute
    new = q("SELECT id,url,title,property_type,price_monthly,district FROM listings WHERE first_seen_run>? AND first_seen_run<=?", (a, b)).fetchall()
    gone = q("SELECT id,url,title,property_type,price_monthly,district FROM listings WHERE delisted_run>? AND delisted_run<=?", (a, b)).fetchall()
    chg = q("""SELECT n.id, l.url, l.title, o.price_monthly old_p, n.price_monthly new_p, o.price_period old_period, n.price_period new_period
               FROM price_history n JOIN listings l ON l.id=n.id
               JOIN price_history o ON o.id=n.id AND o.rowid=(SELECT MAX(rowid) FROM price_history x WHERE x.id=n.id AND x.rowid<n.rowid)
               WHERE n.run_id>? AND n.run_id<=?""", (a, b)).fetchall()
    active = q("SELECT COUNT(*) FROM listings WHERE active=1").fetchone()[0]
    print(f"diff run #{a} -> #{b}:  new={len(new)}  delisted={len(gone)} ({100 * len(gone) / max(active + len(gone), 1):.1f}% of listings that were active)  price changes={len(chg)}")
    if args.out:
        def w(p: Path):
            with open(p, "w", newline="", encoding="utf-8-sig") as f:
                wr = csv.writer(f)
                wr.writerow(["change", "id", "url", "title", "type", "district", "price_monthly", "old_price", "new_price"])
                for r in new:
                    wr.writerow(["new", r["id"], r["url"], safe_text(r["title"]), r["property_type"], r["district"], r["price_monthly"], "", ""])
                for r in gone:
                    wr.writerow(["delisted", r["id"], r["url"], safe_text(r["title"]), r["property_type"], r["district"], r["price_monthly"], "", ""])
                for r in chg:
                    wr.writerow(["price", r["id"], r["url"], safe_text(r["title"]), "", "", "", r["old_p"], r["new_p"]])
        atomic_write(Path(args.out), w)
        log.info("wrote %s", args.out)
    else:
        for label, rs in (("NEW", new), ("DELISTED", gone)):
            for r in rs[:10]:
                print(f"  {label:8s} {r['id']}  {r['property_type']}  {r['price_monthly']}  {r['url']}")
        for r in chg[:10]:
            print(f"  PRICE    {r['id']}  {r['old_p']} -> {r['new_p']}  {r['url']}")
    return EXIT_OK


def cmd_export(args, db: DB) -> int:
    rows = load_rows(db, args.with_text)
    if not rows:
        log.error("no listings in the database; run `crawl` first")
        return EXIT_ERROR
    if args.active_only:
        rows = [r for r in rows if r["active"]]
    base = Path(args.out)
    stats = db.stats()
    fmts = ["csv", "xlsx", "jsonl", "parquet"] if args.format == "all" else [args.format]
    for f in fmts:
        path = base.with_suffix(f".{f}")
        if f == "csv":
            export_csv(rows, path, args.with_text)
        elif f == "jsonl":
            export_jsonl(rows, path)
        elif f == "parquet":
            export_parquet(rows, path)
        else:
            export_xlsx(db, rows, path, stats, args.with_text)
        log.info("wrote %s (%d rows)", path, len(rows))
    print_quality(stats)
    return EXIT_OK


def cmd_status(args, db: DB) -> int:
    print(json.dumps(db.stats(), indent=2))
    for r in db.conn.execute("SELECT source, COUNT(*) c, MAX(fetched_at) last FROM pages GROUP BY source"):
        print(f"source: {r['source']}\n  pages in current cycle: {r['c']}  last fetch: {r['last']}  site total: {db.meta_get('total:' + r['source'])}")
    print("recent runs:")
    for r in db.conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 6"):
        print(f"  #{r['id']} {r['kind']:7s} {r['status']:11s} complete={r['complete']} seen={r['seen']} started={r['started_at']} "
              f"new={r['new_listings']} stopped={r['stopped_reason']}")
    s = db.stats()
    if s["detail_pending"]:
        print(f"detail pages pending: {s['detail_pending']}  (`details --plan` shows what is actually missing)")
    return EXIT_OK


# --------------------------------------------------------------------------- CLI
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=DEFAULT_DB, help="SQLite file (default: %(default)s)")
    common.add_argument("--url", action="append", help="results URL (repeatable). Default: the Alexandria for-rent URL")
    common.add_argument("--url-file", help="text file with one results URL per line (sharding fallback; such runs never delist)")
    common.add_argument("--param", action="append", metavar="KEY=VALUE",
                        help="extra query parameter added to every results URL (e.g. a sort parameter found by `probe`)")
    common.add_argument("--rps", type=float, default=1.0, help="max requests/second overall (default 1.0; do not raise)")
    common.add_argument("--concurrency", type=int, default=3, help="parallel workers for `details` (rps stays global)")
    common.add_argument("--timeout", type=float, default=30.0)
    common.add_argument("--max-retries", type=int, default=5)
    common.add_argument("--backoff-base", type=float, default=2.0, help="seconds, doubles per retry")
    common.add_argument("--block-limit", type=int, default=4, help="stop after N consecutive blocked responses")
    common.add_argument("--user-agent", default=DEFAULT_UA)
    common.add_argument("--proxy", help="plain HTTP(S) proxy for your own network setup. No rotation is built in")
    common.add_argument("--fetcher", choices=["httpx", "playwright"], default="httpx")
    common.add_argument("--log-file", default="aqarmap_scraper.log")
    common.add_argument("--log-json", action="store_true", help="write JSON lines (to the log file and stderr)")
    common.add_argument("--runs-dir", default="runs", help="where crawl run reports (runs/<id>.json) are written")
    common.add_argument("--samples-dir", default="samples")
    common.add_argument("-v", "--verbose", action="store_true")

    p = argparse.ArgumentParser(prog="aqarmap_scraper", description=__doc__.split("\n\n")[0])
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe", parents=[common], help="fetch page 1 + one detail page, run the schema canary, save HTML")
    sp.add_argument("--listing-url", help="specific listing URL to test instead of the first card")
    sp.add_argument("--no-detail", action="store_true")
    sp.add_argument("--deep", action="store_true", help="also fetch deep pages (up to the last) to test for a pagination cap")

    sp = sub.add_parser("crawl", parents=[common], help="result pages -> listings (payload first, cards cross-checked)")
    sp.add_argument("--max-pages", type=int, default=1000, help="hard cap per source (use 3 for a smoke test)")
    sp.add_argument("--refresh", action="store_true",
                    help="discard an UNFINISHED cycle and start a new snapshot (a finished cycle always starts fresh anyway)")
    sp.add_argument("--passes", type=int, default=2,
                    help="full cycles back to back (default 2: a ranked, shifting sort order can skip listings in one pass)")
    sp.add_argument("--delist-after", type=int, default=2,
                    help="mark a listing inactive after N consecutive COMPLETE cycles that missed it (default 2)")
    sp.add_argument("--save-raw", action=argparse.BooleanOptionalAction, default=True,
                    help="store each list page gzipped in raw_pages so parser changes are an offline `reparse` (default on)")

    sp = sub.add_parser("details", parents=[common], help="listing pages -> detail_* columns, only for missing fields")
    sp.add_argument("--plan", action="store_true", help="print which fields the payload left empty and the cost; fetch nothing")
    sp.add_argument("--fields", help="only listings still missing one of: posted_date,furnished,coords,payment")
    sp.add_argument("--limit", type=int, help="only process N listings (smoke test)")
    sp.add_argument("--retry-errors", action="store_true")
    sp.add_argument("--redo", action="store_true", help="re-fetch every active listing, even finished ones")

    sp = sub.add_parser("reparse", parents=[common], help="rebuild parsed columns offline (no network)")
    sp.add_argument("--what", choices=["all", "pages", "listings", "payload", "cards", "details"], default="all",
                    help="pages = from raw_pages HTML; listings/payload/cards = from stored raw_json + card_html")

    sp = sub.add_parser("validate", parents=[common], help="data-quality flags and coverage by property type")
    sp.add_argument("--out", help="write flagged listings to this CSV")

    sp = sub.add_parser("diff", parents=[common], help="new / delisted / price-changed listings between two crawl runs")
    sp.add_argument("--from", dest="from_run", type=int, help="older run id (default: second-to-last complete crawl)")
    sp.add_argument("--to", dest="to_run", type=int, help="newer run id (default: last complete crawl)")
    sp.add_argument("--out", help="write the full diff to this CSV")

    sp = sub.add_parser("export", parents=[common], help="write csv/xlsx/jsonl/parquet (atomic)")
    sp.add_argument("--format", choices=["csv", "xlsx", "jsonl", "parquet", "all"], default="all")
    sp.add_argument("--out", default="aqarmap_alexandria_rent", help="output path without extension")
    sp.add_argument("--with-text", action="store_true", help="include the stored detail-page text column")
    sp.add_argument("--active-only", action="store_true", help="skip listings marked delisted")

    sub.add_parser("status", parents=[common], help="progress, runs and data quality")
    sub.add_parser("areas", parents=[common], help="list district URLs for sharding (fallback only)")
    return p


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({"ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="seconds"),
                           "level": record.levelname, "msg": record.getMessage()}, ensure_ascii=False)


def setup_logging(verbose: bool, log_file: str | None, log_json: bool = False):
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if log_file:
        handlers.append(RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=2, encoding="utf-8"))
    fmt: logging.Formatter = JsonFormatter() if log_json else logging.Formatter(
        "%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    for h in handlers:
        h.setFormatter(fmt)
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, handlers=handlers, force=True)
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
    setup_logging(args.verbose, args.log_file, args.log_json)
    db = DB(args.db)
    try:
        sync = {"export": cmd_export, "status": cmd_status, "reparse": cmd_reparse, "validate": cmd_validate, "diff": cmd_diff}
        if args.cmd in sync:
            return sync[args.cmd](args, db)
        return asyncio.run(run_async(args, db))
    except RobotsDisallowed as e:
        log.error("%s", e)
        return EXIT_ROBOTS
    except BlockedError as e:
        log.error("BLOCKED: %s", e)
        log.error("Progress is saved. Wait and re-run (it resumes) or lower --rps (try 0.3). If the site keeps blocking, "
                  "ask them for a data feed instead of escalating.")
        return EXIT_BLOCKED
    except SchemaChanged as e:
        log.error("STRUCTURE CHANGED: %s", e)
        return EXIT_STRUCTURE
    except ExportLocked as e:
        log.error("%s", e)
        return EXIT_EXPORT
    except FetchError as e:
        log.error("%s", e)
        return EXIT_ERROR
    except KeyboardInterrupt:
        log.warning("interrupted; progress is saved, re-run the same command to resume")
        return 130
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
