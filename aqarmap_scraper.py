#!/usr/bin/env python3
"""
aqarmap_scraper.py - resumable, polite scraper for Aqarmap listing pages.

Commands
    probe     fetch page 1 (of one or more URLs) + one detail page, save the raw HTML to ./samples/,
              show what the parser extracted, look for embedded JSON, optionally test pagination depth.
    crawl     phase 1: walk the paginated result pages, one row per listing.
    details   phase 2 (optional): open each active listing page for the extra fields.
    validate  data-quality flags (outliers, mixed price periods, duplicates, shared coordinates).
    reparse   re-run the parsers over HTML stored in the DB (no network).
    export    write CSV / XLSX / JSONL / Parquet from the DB.
    status    progress, runs, data-quality numbers.
    areas     list district/area URLs (from the DB and page 1) for sharding if pagination is capped.

Design notes
    * SQLite (WAL) is the source of truth; every page commits, so Ctrl-C / crashes lose nothing.
    * A "cycle" is one pass over a source's pages. An unfinished cycle is resumed; after a complete
      one, the next `crawl` starts a fresh snapshot (price changes -> price_history, disappeared
      listings -> active=0 after --delist-after consecutive complete cycles that miss them).
    * robots.txt is fetched and enforced (wildcards, Allow/Disallow, Crawl-delay). If the crawl URLs
      are disallowed the run aborts. There is deliberately no override flag.
    * Rate limiting is global, jittered and adaptive (slows down on 429/5xx/blocks). Repeated blocks
      stop the run instead of hammering the site.
    * Card HTML (svg stripped, zlib) is stored, so parser fixes never need a re-crawl: use `reparse`.
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
import statistics
import sys
import time
import zlib
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser
from selectolax.lexbor import LexborNode as Node

__version__ = "2.0.0"

DEFAULT_URL = "https://aqarmap.com.eg/en/for-rent/property-type/alexandria/"
ALT_URL = "https://aqarmap.com.eg/en/for-rent/property-type/alexandria/?location=alexandria"
DEFAULT_DB = "aqarmap_alexandria_rent.db"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}
# generous bounding box around Alexandria governorate: (lat_min, lat_max, lng_min, lng_max)
BBOX = (30.0, 31.6, 28.8, 30.5)
COMPLETE_COVERAGE = 0.90      # a cycle only counts as complete if it saw >= 90% of the site's stated total
SHARED_COORD_MIN = 15         # >= this many listings on identical coordinates => probably a centroid

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


def to_num(s) -> int | float | None:
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


_SVG_RE = re.compile(r"<svg\b.*?</svg>", re.S | re.I)


def strip_svg(html: str) -> str:
    return _SVG_RE.sub("", html)


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
    """Normalise a listed price to EGP/month. Daily x30 is an assumption; such rows are flagged."""
    if amount is None or period is None:
        return None
    return {"month": amount, "day": amount * 30, "week": amount * 52 / 12, "year": amount / 12}[period]


def pick_prices(prices: list[tuple[float, str | None]]) -> tuple[float | None, str | None, float | None, float | None]:
    """-> (price_amount, price_period, price_monthly, daily_rate). Never mixes periods in one column."""
    if not prices:
        return None, None, None, None
    ordered = sorted(prices, key=lambda x: _PERIOD_RANK[x[1]])
    amount, period = ordered[0]
    daily = next((a for a, p in prices if p == "day"), None) if period != "day" else None
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


@dataclass
class Card:
    id: int
    url: str
    url_slug: str | None
    title: str | None
    property_type: str | None
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
    advertiser: str | None
    badges: str | None
    photo_count: int | None
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


def _icon_value(node: Node, icons: tuple[str, ...]) -> str | None:
    """Text of the <span> that belongs to an icon: <li><div><i class='bedroom-icon'/></div><span>3</span></li>.
    Looks at most two ancestors up and never leaves the enclosing <li>, so a neighbour's value can't leak in."""
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

    # --- title / type
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
    ptype = None
    if title:
        m = re.match(r"\s*(.+?)\s+for\s+(?:rent|sale)\b", title, re.I)
        ptype = m.group(1) if m else None

    # --- price: <data value="25000">25,000 EGP/month</data>; text regex only as a fallback
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

    # --- area / beds / baths from their icons, regex fallbacks on the card text
    area_txt = _icon_value(node, ("size-icon", "area-icon"))
    area = first_number(area_txt)
    if area is None and (m := AREA_RE.search(text_norm)):
        area = to_num(m.group(1))
    beds_v = first_number(_icon_value(node, ("bedroom-icon", "bed-icon")))
    baths_v = first_number(_icon_value(node, ("bathroom-icon", "bath-icon")))
    if beds_v is None and (m := BEDS_RE.search(text_norm)):
        beds_v = int(m.group(1))
    if baths_v is None and (m := BATHS_RE.search(text_norm)):
        baths_v = int(m.group(1))

    # --- location breadcrumbs: first = district, last = area/compound
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

    # --- advertiser (agency logo) and badges
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

    # --- photos
    photo_count = first_number(_icon_value(node, ("image-icon",)))
    photo_count = int(photo_count) if photo_count is not None else None
    img = node.css_first("div.embla img") or node.css_first("img")
    image_url = None
    if img is not None:
        src = img.attributes.get("src") or img.attributes.get("data-src") or ""
        if src and not src.startswith("data:"):
            image_url = urljoin(base_url, src)

    return Card(
        id=listing_id, url=url, url_slug=url_slug, title=title, property_type=ptype,
        price_amount=price_amount, price_period=price_period, price_monthly=price_monthly, daily_rate=daily_rate,
        area_sqm=float(area) if area is not None else None,
        beds=int(beds_v) if beds_v is not None else None,
        baths=int(baths_v) if baths_v is not None else None,
        district=district, area_name=area_name, district_url=district_url, area_url=area_url, location=location,
        advertiser=advertiser, badges=badges, photo_count=photo_count, image_url=image_url,
        card_text=text_piped, card_html=strip_svg(node.html or ""),
    )


def _card_id(article: Node) -> int | None:
    m = LABELLEDBY_RE.search(article.attributes.get("aria-labelledby") or "")
    if m:
        return int(m.group(1))
    ids = sorted(_ids_in(article))
    return ids[0] if ids else None


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
        # fallback if the site renames the card class: climb from each listing link to the card root
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
    # drop cards before looking for the "N Properties" header so card text can't be mistaken for it
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


# --------------------------------------------------------------------------- embedded-data inspection (probe)
def find_record_arrays(obj, path: str = "$", out: list | None = None, depth: int = 0) -> list[tuple[str, int, list[str]]]:
    """Find arrays of >=5 dicts that look like records (have an id-ish key). Used to spot a JSON listing payload."""
    out = [] if out is None else out
    if depth > 12:
        return out
    if isinstance(obj, list):
        dicts = [x for x in obj if isinstance(x, dict)]
        if len(dicts) >= 5 and any(k.lower() in ("id", "listingid", "listing_id") for k in dicts[0]):
            out.append((path, len(dicts), sorted(dicts[0].keys())[:25]))
        for i, v in enumerate(obj[:50]):
            find_record_arrays(v, f"{path}[{i}]", out, depth + 1)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            find_record_arrays(v, f"{path}.{k}", out, depth + 1)
    return out


def inspect_embedded(html: str, sample_ids: list[int]) -> list[str]:
    doc = HTMLParser(html)
    lines = []
    markers = {
        "__NEXT_DATA__": 'id="__NEXT_DATA__"' in html,
        "self.__next_f (RSC flight)": "self.__next_f" in html,
        "ld+json": "application/ld+json" in html,
        "window.__*": bool(re.search(r"window\.__\w+", html)),
    }
    lines.append("markers: " + ", ".join(f"{k}={'yes' if v else 'no'}" for k, v in markers.items()))
    nd = doc.css_first("script#__NEXT_DATA__")
    if nd is not None:
        try:
            arrays = find_record_arrays(json.loads(nd.text()))
            for path, n, keys in arrays[:5]:
                lines.append(f"__NEXT_DATA__ record array at {path}: {n} items, keys: {keys}")
            if not arrays:
                lines.append("__NEXT_DATA__ present but no obvious listing array found")
        except ValueError:
            lines.append("__NEXT_DATA__ present but not valid JSON")
    hits = 0
    for idx, sc in enumerate(doc.css("script")):
        t = sc.text() or ""
        for lid in sample_ids[:3]:
            pos = t.find(str(lid))
            if pos >= 0 and hits < 3:
                hits += 1
                lines.append(f"listing id {lid} appears in <script> #{idx} (type={sc.attributes.get('type')}, "
                             f"{len(t):,} chars): ...{t[max(0, pos - 120):pos + 220]!r}...")
                break
    if not hits:
        lines.append("no listing id found inside any <script>: data is only in the HTML cards")
    return lines


# --------------------------------------------------------------------------- parsing: detail pages
DETAIL_LABELS: dict[str, list[str]] = {
    "posted_date": ["date posted", "posted", "posted on", "listed on", "تاريخ النشر"],
    "updated_date": ["date updated", "updated", "last updated", "تاريخ التحديث"],
    "detail_area_sqm": ["built-up area", "built up area", "area", "size", "المساحة"],
    "detail_beds": ["bedrooms", "bed rooms", "beds", "غرف النوم", "غرف"],
    "detail_baths": ["bathrooms", "baths", "bath", "الحمامات"],
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


def in_bbox(lat, lng) -> bool:
    return lat is not None and lng is not None and BBOX[0] <= lat <= BBOX[1] and BBOX[2] <= lng <= BBOX[3]


def parse_detail(html: str, url: str) -> dict:
    doc = HTMLParser(html)
    out: dict = {}

    h1 = doc.css_first("h1")
    full_title = squash(h1.text(strip=True)) if h1 is not None else None
    if not full_title:
        t = doc.css_first("title")
        full_title = squash(t.text(strip=True)) if t is not None else None
    out["full_title"] = full_title

    # coordinates: JSON-LD first, then an inline "latitude"/"longitude" pair. Anything outside the
    # Alexandria bounding box is rejected (typically a site-wide default or the company address).
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
    out["lat"], out["lng"] = lat, lng

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
        if col == "detail_area_sqm":
            out[col] = first_number(val)
        elif col in ("detail_beds", "detail_baths"):
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
LISTING_COLS: list[tuple[str, str]] = [
    ("id", "INTEGER PRIMARY KEY"), ("url", "TEXT NOT NULL"), ("url_slug", "TEXT"),
    ("title", "TEXT"), ("property_type", "TEXT"),
    ("price_amount", "REAL"), ("price_period", "TEXT"), ("price_monthly", "REAL"), ("daily_rate", "REAL"),
    ("currency", "TEXT DEFAULT 'EGP'"),
    ("area_sqm", "REAL"), ("beds", "INTEGER"), ("baths", "INTEGER"),
    ("district", "TEXT"), ("area_name", "TEXT"), ("district_url", "TEXT"), ("area_url", "TEXT"), ("location", "TEXT"),
    ("advertiser", "TEXT"), ("badges", "TEXT"), ("photo_count", "INTEGER"), ("image_url", "TEXT"),
    ("source", "TEXT"), ("page", "INTEGER"),
    ("first_seen", "TEXT"), ("last_seen", "TEXT"), ("first_seen_run", "INTEGER"), ("last_seen_run", "INTEGER"),
    ("active", "INTEGER DEFAULT 1"), ("missed_runs", "INTEGER DEFAULT 0"),
    ("card_text", "TEXT"), ("card_html", "BLOB"),
    ("detail_status", "TEXT"), ("detail_fetched_at", "TEXT"),
    ("full_title", "TEXT"), ("posted_date", "TEXT"), ("updated_date", "TEXT"),
    ("detail_area_sqm", "REAL"), ("detail_beds", "INTEGER"), ("detail_baths", "INTEGER"),
    ("floor", "TEXT"), ("view", "TEXT"), ("year_built", "TEXT"), ("finishing", "TEXT"), ("furnished", "TEXT"),
    ("seller_type", "TEXT"), ("payment", "TEXT"),
    ("lat", "REAL"), ("lng", "REAL"), ("images", "TEXT"), ("attrs_json", "TEXT"), ("detail_text", "BLOB"),
]
OTHER_TABLES: dict[str, list[tuple[str, str]]] = {
    "price_history": [("id", "INTEGER NOT NULL"), ("seen_at", "TEXT NOT NULL"), ("price", "REAL NOT NULL"), ("run_id", "INTEGER")],
    "runs": [("id", "INTEGER PRIMARY KEY AUTOINCREMENT"), ("kind", "TEXT"), ("source", "TEXT"), ("started_at", "TEXT"),
             ("ended_at", "TEXT"), ("status", "TEXT"), ("complete", "INTEGER DEFAULT 0"), ("site_total", "INTEGER"),
             ("new_listings", "INTEGER DEFAULT 0"), ("stopped_reason", "TEXT"), ("stats_json", "TEXT")],
}
STATIC_SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
  source TEXT NOT NULL, page INTEGER NOT NULL, status TEXT NOT NULL,
  n_cards INTEGER, ids TEXT, fetched_at TEXT, error TEXT,
  PRIMARY KEY (source, page)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE INDEX IF NOT EXISTS idx_price_history_id ON price_history(id);
CREATE INDEX IF NOT EXISTS idx_listings_detail ON listings(active, detail_status);
CREATE INDEX IF NOT EXISTS idx_listings_source_run ON listings(source, last_seen_run);
"""

CARD_COLS = ["title", "property_type", "price_amount", "price_period", "price_monthly", "daily_rate", "area_sqm",
             "beds", "baths", "district", "area_name", "district_url", "area_url", "location", "advertiser", "badges",
             "photo_count", "image_url", "url_slug"]
_PRICE_TRIO = {"price_amount", "price_period", "price_monthly", "daily_rate"}


def _upsert_sql() -> str:
    cols = ["id", "url", *CARD_COLS, "source", "page", "first_seen", "last_seen", "first_seen_run", "last_seen_run",
            "active", "missed_runs", "card_text", "card_html"]
    sets = []
    for c in CARD_COLS:
        if c in _PRICE_TRIO:  # a new price replaces the whole trio; no new price keeps the old trio intact
            sets.append(f"{c}=CASE WHEN excluded.price_amount IS NOT NULL THEN excluded.{c} ELSE {c} END")
        else:
            sets.append(f"{c}=COALESCE(excluded.{c},{c})")
    sets += ["url=excluded.url", "source=excluded.source", "page=excluded.page", "last_seen=excluded.last_seen",
             "last_seen_run=excluded.last_seen_run", "first_seen_run=COALESCE(first_seen_run,excluded.first_seen_run)",
             "active=1", "missed_runs=0", "card_text=excluded.card_text", "card_html=excluded.card_html"]
    return (f"INSERT INTO listings ({','.join(cols)}) VALUES ({','.join(':' + c for c in cols)}) "
            f"ON CONFLICT(id) DO UPDATE SET {', '.join(sets)}")


UPSERT_CARD = _upsert_sql()
DETAIL_COLS = ["full_title", "posted_date", "updated_date", "detail_area_sqm", "detail_beds", "detail_baths", "floor",
               "view", "year_built", "finishing", "furnished", "seller_type", "payment", "lat", "lng", "images", "attrs_json"]


class DB:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()
        self.conn.commit()

    def _init_schema(self):
        tables = {"listings": LISTING_COLS, **OTHER_TABLES}
        for name, cols in tables.items():
            ddl = ", ".join(f"{c} {d}" for c, d in cols)
            self.conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({ddl})")
            have = {r["name"] for r in self.conn.execute(f"PRAGMA table_info({name})")}
            for c, d in cols:  # migrate databases created by an older version
                if c not in have:
                    d2 = d.replace("NOT NULL", "").replace("PRIMARY KEY", "").replace("AUTOINCREMENT", "").strip()
                    self.conn.execute(f"ALTER TABLE {name} ADD COLUMN {c} {d2 or 'TEXT'}")
                    log.info("migrated: added %s.%s", name, c)
        self.conn.executescript(STATIC_SCHEMA)

    def close(self):
        self.conn.commit()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def count_listings(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    # -- meta
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

    def begin_cycle(self, source: str, fresh: bool) -> tuple[int, bool]:
        """Resume the latest unfinished crawl cycle for `source`, or start a new snapshot cycle."""
        row = self.conn.execute("SELECT * FROM runs WHERE kind='crawl' AND source=? ORDER BY id DESC LIMIT 1",
                                (source,)).fetchone()
        if row and not row["complete"] and not fresh:
            self.conn.execute("UPDATE runs SET status='running', ended_at=NULL WHERE id=?", (row["id"],))
            self.conn.commit()
            return row["id"], True
        self.conn.execute("DELETE FROM pages WHERE source=?", (source,))
        return self.new_run("crawl", source), False

    def finalize_cycle(self, source: str, run_id: int, natural_end: bool, site_total: int | None, delist_after: int) -> dict:
        seen = self.conn.execute("SELECT COUNT(*) FROM listings WHERE last_seen_run=?", (run_id,)).fetchone()[0]
        errors = self.conn.execute("SELECT COUNT(*) FROM pages WHERE source=? AND status='error'", (source,)).fetchone()[0]
        coverage_ok = site_total is None or seen >= COMPLETE_COVERAGE * site_total
        complete = bool(natural_end and errors == 0 and coverage_ok)
        delisted = 0
        if complete:
            self.conn.execute(
                "UPDATE listings SET missed_runs=missed_runs+1 WHERE source=? AND active=1 "
                "AND (last_seen_run IS NULL OR last_seen_run!=?)", (source, run_id))
            cur = self.conn.execute(
                "UPDATE listings SET active=0 WHERE source=? AND active=1 AND missed_runs>=?", (source, delist_after))
            delisted = cur.rowcount
        self.conn.commit()
        return {"complete": complete, "seen": seen, "delisted": delisted, "page_errors": errors}

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
    def upsert_cards(self, source: str, page: int, cards: list[Card], run_id: int) -> int:
        ts = now_iso()
        new = 0
        for c in cards:
            row = self.conn.execute("SELECT price_monthly FROM listings WHERE id=?", (c.id,)).fetchone()
            if row is None:
                new += 1
            if c.price_monthly is not None and (row is None or row["price_monthly"] != c.price_monthly):
                self.conn.execute("INSERT INTO price_history(id,seen_at,price,run_id) VALUES(?,?,?,?)",
                                  (c.id, ts, c.price_monthly, run_id))
            d = asdict(c)
            d["card_html"] = zlib.compress(c.card_html.encode("utf-8"), 6)
            d.update(source=source, page=page, first_seen=ts, last_seen=ts, first_seen_run=run_id,
                     last_seen_run=run_id, active=1, missed_runs=0)
            self.conn.execute(UPSERT_CARD, d)
        return new

    def pending_detail_ids(self, limit: int | None, retry_errors: bool, redo: bool) -> list[tuple[int, str]]:
        if redo:
            cond = "active=1"
        elif retry_errors:
            cond = "active=1 AND (detail_status IS NULL OR detail_status='error')"
        else:
            cond = "active=1 AND detail_status IS NULL"
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
        return {
            "listings": q("SELECT COUNT(*) FROM listings"),
            "active": q("SELECT COUNT(*) FROM listings WHERE active=1"),
            "delisted": q("SELECT COUNT(*) FROM listings WHERE active=0"),
            "with_price": q("SELECT COUNT(*) FROM listings WHERE price_monthly IS NOT NULL"),
            "with_area": q("SELECT COUNT(*) FROM listings WHERE area_sqm IS NOT NULL"),
            "with_beds": q("SELECT COUNT(*) FROM listings WHERE beds IS NOT NULL"),
            "with_baths": q("SELECT COUNT(*) FROM listings WHERE baths IS NOT NULL"),
            "with_location": q("SELECT COUNT(*) FROM listings WHERE location IS NOT NULL"),
            "with_title": q("SELECT COUNT(*) FROM listings WHERE title IS NOT NULL"),
            "with_photo_count": q("SELECT COUNT(*) FROM listings WHERE photo_count IS NOT NULL"),
            "with_advertiser": q("SELECT COUNT(*) FROM listings WHERE advertiser IS NOT NULL"),
            "detail_ok": q("SELECT COUNT(*) FROM listings WHERE detail_status='ok'"),
            "detail_gone": q("SELECT COUNT(*) FROM listings WHERE detail_status='gone'"),
            "detail_error": q("SELECT COUNT(*) FROM listings WHERE detail_status='error'"),
            "detail_pending": q("SELECT COUNT(*) FROM listings WHERE active=1 AND detail_status IS NULL"),
            "pages_ok": q("SELECT COUNT(*) FROM pages WHERE status='ok'"),
            "pages_error": q("SELECT COUNT(*) FROM pages WHERE status='error'"),
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
        if r.get("price_amount") is None:
            f.append("no_price")
        else:
            p = r.get("price_period")
            f.append({None: "unknown_price_period", "day": "daily_price", "week": "weekly_price",
                      "year": "yearly_price"}.get(p, "")) if p != "month" else None
        ca, da = r.get("area_sqm"), r.get("detail_area_sqm")
        if ca is None and da is None:
            f.append("no_area")
        elif ca and da and abs(da - ca) / ca > 0.10:
            f.append("area_mismatch")
    # price per m2 outliers within property type (robust z-score on log values)
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
    # likely duplicate postings: same advertiser, type, price, area, rooms and place
    dup: dict = defaultdict(list)
    for r in rows:
        if r.get("price_amount") is not None and r.get("area_sqm") is not None:
            key = (r.get("advertiser"), r.get("property_type"), r["price_amount"], r["area_sqm"],
                   r.get("beds"), r.get("baths"), r.get("location"))
            dup[key].append(r["id"])
    for ids in dup.values():
        if len(ids) > 1:
            for i in ids:
                flags[i].append(f"dup_x{len(ids)}")
    # coordinates shared by many listings are probably a map centroid, not the property
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
            c[re.sub(r"_x\d+$", "_x", f)] += 1
    return dict(sorted(c.items(), key=lambda kv: -kv[1]))


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
    NOTE: written to the Playwright API but never exercised against the real site or a mock."""

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
        log.info("robots.txt OK for %d checked URL(s) (%d rule lines applied)", len(urls), len(rules.raw))


# --------------------------------------------------------------------------- crawl (phase 1)
class Crawler:
    def __init__(self, eng: Engine):
        self.eng = eng
        self.db = eng.db
        self.args = eng.args

    async def crawl_source(self, source: str) -> dict:
        run_id, resumed = self.db.begin_cycle(source, fresh=self.args.refresh)
        if resumed:
            log.info("resuming unfinished cycle (run #%d)", run_id)
        else:
            log.info("starting new cycle (run #%d)", run_id)
        try:
            res = await self._crawl(source, run_id)
        except BaseException as e:  # noqa: BLE001 - record the interruption, then re-raise
            self.db.end_run(run_id, status="interrupted", stopped_reason=type(e).__name__)
            raise
        res["run_id"] = run_id
        return res

    def _first_page_sanity(self, cards: list[Card]):
        n = len(cards)
        cov = {f: sum(getattr(c, f) is not None for c in cards) / n for f in ("price_monthly", "area_sqm", "location", "title")}
        bad = [f for f, v in cov.items() if v < 0.8]
        if bad:
            log.warning("page 1 sanity: low coverage for %s (%s). The parser may not match the current layout: "
                        "run `probe`.", ", ".join(bad), ", ".join(f"{k}={v:.0%}" for k, v in cov.items()))

    async def _crawl(self, source: str, run_id: int) -> dict:
        db, args = self.db, self.args
        total = int(db.meta_get(f"total:{source}") or 0) or None
        per_page = int(db.meta_get(f"per_page:{source}") or 0) or None
        first_row = db.page_row(source, 1)
        first_ids = set(json.loads(first_row["ids"])) if first_row and first_row["ids"] else None
        n, new_total, pages_done, errors_in_row = 1, 0, 0, 0
        started = time.monotonic()
        stopped = "reached --max-pages"
        natural = False

        while True:
            expected = math.ceil(total / per_page) if total and per_page else None
            limit, by_total = args.max_pages, False
            if expected and expected + 5 < limit:
                limit, by_total = expected + 5, True
            if n > limit:
                stopped = "reached expected last page" if by_total else "reached --max-pages"
                natural = by_total
                break
            row = db.page_row(source, n)
            if row and row["status"] == "ok":
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

            pr = parse_listing_page(res.text, url)
            if n == 1:
                if pr.total_results:
                    total = pr.total_results
                    db.meta_set(f"total:{source}", total)
                if pr.cards:
                    per_page = len(pr.cards)
                    db.meta_set(f"per_page:{source}", per_page)
                    self._first_page_sanity(pr.cards)
                expected = math.ceil(total / per_page) if total and per_page else None

            if not pr.cards:
                expected_more = bool(expected and n <= expected)
                if expected_more:
                    for k in range(2):  # maybe a transient empty render; retry before concluding
                        log.warning("page %d empty but ~%s pages expected; re-checking (%d/2)", n, expected, k + 1)
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
                                    "Use `areas` + `crawl --url-file` to shard by area.", n, expected)
                    natural = not expected_more
                    stopped = f"empty page {n}"
                    break

            ids = [c.id for c in pr.cards]
            if n > 1 and first_ids and set(ids) == first_ids:
                db.record_page(source, n, "wrap", len(ids), ids)
                db.commit()
                log.warning("page %d returned the same listings as page 1 (pagination wraps).", n)
                natural = bool(expected and n > expected)
                stopped = f"pagination wraps at page {n}"
                break

            new = db.upsert_cards(source, n, pr.cards, run_id)
            db.record_page(source, n, "ok", len(ids), ids)
            db.commit()
            if n == 1:
                first_ids = set(ids)
            errors_in_row = 0
            new_total += new
            pages_done += 1
            elapsed = time.monotonic() - started
            rate = pages_done / elapsed if elapsed else 0
            eta = (expected - n) / rate if rate and expected else float("nan")
            log.info("page %d/%s  cards=%d new=%d db=%d  eta=%s", n, expected or "?", len(ids), new,
                     db.count_listings(), fmt_eta(eta))
            n += 1

        info = db.finalize_cycle(source, run_id, natural, total, args.delist_after)
        prev_new = db.conn.execute("SELECT new_listings FROM runs WHERE id=?", (run_id,)).fetchone()[0] or 0
        db.end_run(run_id, status="finished", complete=int(info["complete"]), site_total=total, stopped_reason=stopped,
                   new_listings=prev_new + new_total, stats_json=json.dumps(info))
        in_db = db.conn.execute("SELECT COUNT(*) FROM listings WHERE source=?", (source,)).fetchone()[0]
        return {"source": source, "stopped": stopped, "new": new_total, "in_db": in_db, "site_total": total, **info}


# --------------------------------------------------------------------------- details (phase 2)
class DetailRunner:
    def __init__(self, eng: Engine):
        self.eng = eng
        self.db = eng.db
        self.args = eng.args

    async def run(self) -> dict:
        todo = self.db.pending_detail_ids(self.args.limit, self.args.retry_errors, self.args.redo)
        stats = {"ok": 0, "gone": 0, "error": 0, "todo": len(todo)}
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
            self.db.end_run(run_id, status="interrupted" if blocked else "finished", complete=0 if blocked else 1,
                            stats_json=json.dumps(stats))
        if blocked:
            raise blocked[0]
        return stats


# --------------------------------------------------------------------------- export
EXPORT_COLUMNS = [
    ("id", "ID"), ("url", "URL"), ("title", "Title"), ("property_type", "Type"),
    ("price_monthly", "Price (EGP/month)"), ("price_amount", "Listed price"), ("price_period", "Price period"),
    ("daily_rate", "Daily rate (EGP)"), ("area_sqm", "Area (m²)"), ("price_per_sqm", "Price per m²"),
    ("beds", "Beds"), ("baths", "Baths"), ("district", "District"), ("area_name", "Area / compound"),
    ("advertiser", "Advertiser"), ("badges", "Badges"), ("photo_count", "Photos"),
    ("posted_date", "Posted"), ("updated_date", "Updated"), ("floor", "Floor"), ("finishing", "Finishing"),
    ("furnished", "Furnished"), ("view", "View"), ("year_built", "Year built"), ("seller_type", "Seller type"),
    ("payment", "Payment"), ("lat", "Lat"), ("lng", "Lng"), ("images", "Detail image URLs"),
    ("first_seen", "First seen"), ("last_seen", "Last seen"), ("active", "Active"), ("detail_status", "Detail status"),
    ("flags", "Flags"),
]
TEXT_COLS = {"title", "property_type", "district", "area_name", "advertiser", "badges", "floor", "finishing",
             "furnished", "view", "seller_type", "payment", "year_built", "detail_text", "flags"}
_LOAD_COLS = [c for c, _ in LISTING_COLS if c not in ("card_html", "card_text", "detail_text", "attrs_json", "images")]


def load_rows(db: DB, with_text: bool = False) -> list[dict]:
    sel = ", ".join(_LOAD_COLS + ["images"] + (["detail_text"] if with_text else []))
    rows = []
    for r in db.conn.execute(f"SELECT {sel} FROM listings ORDER BY id"):
        d = dict(r)
        d["images"] = json.loads(d["images"]) if d.get("images") else []
        if with_text:
            blob = d.get("detail_text")
            d["detail_text"] = zlib.decompress(blob).decode("utf-8")[:32000] if blob else None
        rows.append(d)
    flags = compute_flags(rows)
    for d in rows:
        d["flags"] = flags[d["id"]]
        # card values win; detail values only fill gaps (the raw detail_* columns stay in the DB)
        for c in ("area_sqm", "beds", "baths"):
            if d.get(c) is None:
                d[c] = d.get(f"detail_{c}")
        d["price_per_sqm"] = round(d["price_monthly"] / d["area_sqm"], 1) if d.get("price_monthly") and d.get("area_sqm") else None
    return rows


def _cell(r: dict, c: str):
    v = r.get(c)
    if c == "images":
        v = " | ".join(v)
    elif c == "flags":
        v = ";".join(v)
    elif c == "active":
        v = "yes" if v else "no"
    return safe_text(v) if c in TEXT_COLS else v


def export_csv(rows: list[dict], path: Path, with_text: bool):
    cols = EXPORT_COLUMNS + ([("detail_text", "Detail text")] if with_text else [])
    with open(path, "w", newline="", encoding="utf-8-sig") as f:  # BOM so Excel reads Arabic correctly
        w = csv.writer(f)
        w.writerow([h for _, h in cols])
        for r in rows:
            w.writerow([_cell(r, c) for c, _ in cols])


def export_jsonl(rows: list[dict], path: Path):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def export_parquet(rows: list[dict], path: Path):
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        raise SystemExit("Parquet needs pyarrow: pip install pyarrow") from e
    pq.write_table(pa.Table.from_pylist(rows), path)


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
    widths = {"id": 10, "url": 38, "title": 46, "property_type": 16, "district": 20, "area_name": 28, "advertiser": 20,
              "images": 30, "detail_text": 50, "flags": 24}
    for c, i in idx.items():
        ws.column_dimensions[get_column_letter(i)].width = widths.get(c, 15)
    for c, fmt in (("price_monthly", "#,##0"), ("price_amount", "#,##0"), ("daily_rate", "#,##0"),
                   ("area_sqm", "0.0"), ("price_per_sqm", "#,##0")):
        for row in ws.iter_rows(min_row=2, min_col=idx[c], max_col=idx[c]):
            row[0].number_format = fmt
    ws.freeze_panes = "C2"
    ws.auto_filter.ref = ws.dimensions

    changes = db.conn.execute(
        "SELECT ph.id, l.url, l.title, ph.seen_at, ph.price, ph.run_id FROM price_history ph JOIN listings l ON l.id=ph.id "
        "WHERE ph.id IN (SELECT id FROM price_history GROUP BY id HAVING COUNT(*)>1) ORDER BY ph.id, ph.seen_at").fetchall()
    if changes:
        pc = wb.create_sheet("Price changes")
        pc.append(["ID", "URL", "Title", "Seen at (UTC)", "Price (EGP/month)", "Run"])
        for c in pc[1]:
            c.font = Font(bold=True)
        for r in changes:
            pc.append([r["id"], r["url"], safe_text(r["title"]), r["seen_at"], r["price"], r["run_id"]])
        pc.freeze_panes = "A2"
        pc.column_dimensions["B"].width = 38
        pc.column_dimensions["C"].width = 40
        pc.column_dimensions["D"].width = 22

    s = wb.create_sheet("Summary")
    s.append(["Metric", "Value"])
    s["A1"].font = s["B1"].font = Font(bold=True)
    s.append(["Generated (UTC)", now_iso()])
    for k, v in stats.items():
        s.append([k, v])
    s.append([])
    s.append(["Flag", "Listings"])
    for k, v in flag_counts({r["id"]: r["flags"] for r in rows}).items():
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
    log.info("quality: listings=%d active=%d  price=%s  area=%s  beds=%s  baths=%s  location=%s  photos=%s  advertiser=%s",
             stats["listings"], stats["active"], pct("with_price"), pct("with_area"), pct("with_beds"),
             pct("with_baths"), pct("with_location"), pct("with_photo_count"), pct("with_advertiser"))


def log_flag_summary(db: DB):
    counts = flag_counts(compute_flags(load_rows(db)))
    if counts:
        log.info("flags: %s   (details: `validate --out flags.csv`)", ", ".join(f"{k}={v}" for k, v in counts.items()))
    else:
        log.info("flags: none")


async def cmd_crawl(args, eng: Engine) -> int:
    sources = load_sources(args)
    await eng.ensure_robots([u for s in sources for u in (page_url(s, 1), page_url(s, 2))])
    crawler = Crawler(eng)
    for p in range(max(1, args.passes)):
        if p:
            log.info("=== pass %d of %d ===", p + 1, args.passes)
        results = []
        for s in sources:
            log.info("crawling %s", s)
            results.append(await crawler.crawl_source(s))
        for r in results:
            log.info("source done: %s | stopped: %s | new: %d | in DB: %d | site says: %s | cycle complete: %s | delisted now: %d",
                     r["source"], r["stopped"], r["new"], r["in_db"], r["site_total"], "yes" if r["complete"] else "NO",
                     r["delisted"])
            if r["site_total"] and r["seen"] < 0.95 * r["site_total"]:
                log.warning("saw %d of ~%d listings this cycle (%.0f%%). Inventory shifts while crawling; run another pass "
                            "(--passes 2), and if it stays low the site caps pagination: shard with `areas` + `--url-file`.",
                            r["seen"], r["site_total"], 100 * r["seen"] / r["site_total"])
        if not all(r["complete"] for r in results):
            break
    print_quality(eng.db.stats())
    log_flag_summary(eng.db)
    return 0


async def cmd_details(args, eng: Engine) -> int:
    sources = load_sources(args)
    await eng.ensure_robots([page_url(sources[0], 1), urljoin(sources[0], "/en/listing/1-for-rent-x/")])
    stats = await DetailRunner(eng).run()
    log.info("details finished: %s", stats)
    return 0


async def deep_check(eng: Engine, source: str, pr1: PageResult) -> None:
    per, total = len(pr1.cards), pr1.total_results
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
        print(f"VERDICT: pages {empty_mid} are empty before the expected last page -> pagination looks CAPPED. "
              "Shard with `areas` + `crawl --url-file`.")
    elif reached_last:
        print(f"VERDICT: last expected page ({last}) returned fresh listings -> pagination looks uncapped.")
    else:
        print("VERDICT: inconclusive; check the numbers above.")


async def cmd_probe(args, eng: Engine) -> int:
    explicit = bool(args.url or args.url_file)
    sources = load_sources(args) if explicit else [source_key(DEFAULT_URL), source_key(ALT_URL)]
    outdir = Path(args.samples_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    await eng.ensure_robots([page_url(sources[0], 1), page_url(sources[0], 2)])
    results = []
    for k, source in enumerate(sources):
        url = page_url(source, 1)
        r = await eng.fetch(url)
        fname = "page1.html" if k == 0 else f"page1_{k + 1}.html"
        (outdir / fname).write_text(r.text, encoding="utf-8")
        pr = parse_listing_page(r.text, url)
        results.append((source, r, pr))
        print(f"\n=== LIST PAGE  {url}\nHTTP {r.status}  {len(r.text):,} bytes  saved: {outdir / fname}")
        print(f"site total: {pr.total_results}   cards parsed: {len(pr.cards)}")
    if len(results) > 1:
        print("\n=== URL COMPARISON (which one do we want?)")
        for source, _, pr in results:
            print(f"  total={pr.total_results}  cards/page={len(pr.cards)}  {source}")
        ids = [{c.id for c in pr.cards} for _, _, pr in results]
        print(f"  page-1 listing overlap between the first two: {len(ids[0] & ids[1])}")

    source, r, pr = results[0]
    if pr.cards:
        fields = ["title", "property_type", "price_amount", "price_period", "price_monthly", "area_sqm", "beds", "baths",
                  "district", "area_name", "advertiser", "photo_count", "image_url"]
        print("\nfield coverage on page 1:", {f: sum(getattr(c, f) is not None for c in pr.cards) for f in fields},
              f"of {len(pr.cards)}")
        for c in pr.cards[:3]:
            d = asdict(c)
            d.pop("card_html")
            d.pop("card_text")
            print(json.dumps(d, ensure_ascii=False, indent=2))
        print("\n=== EMBEDDED DATA")
        for line in inspect_embedded(r.text, [c.id for c in pr.cards]):
            print(" ", line)
    if args.deep and pr.cards:
        await deep_check(eng, source, pr)
    detail_url = args.listing_url or (pr.cards[0].url if pr.cards and not args.no_detail else None)
    if detail_url:
        dr = await eng.fetch(detail_url)
        (outdir / "detail.html").write_text(dr.text, encoding="utf-8")
        print(f"\n=== DETAIL PAGE  {detail_url}\nHTTP {dr.status}  {len(dr.text):,} bytes  saved: {outdir / 'detail.html'}")
        if dr.status == 200:
            d = parse_detail(dr.text, detail_url)
            print(json.dumps({k: v for k, v in d.items() if k != "detail_text"}, ensure_ascii=False, indent=2))
    print(f"\nSend me {outdir}/page1.html and {outdir}/detail.html (or this output) if any field above is wrong or empty.")
    return 0 if pr.cards else 1


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
    log.info("%d district URL(s). Save to a file and use: crawl --url-file areas.txt  "
             "(counts come from crawled cards; run a partial crawl first for a fuller list)", len(found))
    return 0 if found else 1


def cmd_reparse(args, db: DB) -> int:
    n = 0
    if args.what in ("cards", "all"):
        for r in db.conn.execute("SELECT id,url,card_html FROM listings WHERE card_html IS NOT NULL").fetchall():
            c = parse_card_fragment(zlib.decompress(r["card_html"]).decode("utf-8"), r["id"], r["url"])
            if c is None:
                continue
            vals = [getattr(c, col) for col in CARD_COLS]
            sets = ", ".join(f"{col}=?" for col in CARD_COLS)
            db.conn.execute(f"UPDATE listings SET {sets}, card_text=?, card_html=? WHERE id=?",
                            [*vals, c.card_text, zlib.compress(c.card_html.encode("utf-8"), 6), r["id"]])
            n += 1
        log.info("re-parsed %d card(s) (stored HTML also slimmed: svg stripped)", n)
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
    if args.what in ("cards", "all"):
        db.conn.execute("VACUUM")
    print_quality(db.stats())
    return 0


def cmd_validate(args, db: DB) -> int:
    rows = load_rows(db)
    if not rows:
        log.error("no listings in the database; run `crawl` first")
        return 1
    flags = compute_flags(rows)
    counts = flag_counts(flags)
    print(f"{len(rows)} listings, {sum(1 for f in flags.values() if f)} with at least one flag")
    for k, v in counts.items():
        print(f"  {k:24s} {v}")
    if args.out:
        by_id = {r["id"]: r for r in rows}
        with open(args.out, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["id", "url", "title", "price_amount", "price_period", "area_sqm", "flags"])
            for lid, fl in flags.items():
                if fl:
                    r = by_id[lid]
                    w.writerow([lid, r["url"], safe_text(r["title"]), r["price_amount"], r["price_period"], r["area_sqm"], ";".join(fl)])
        log.info("wrote %s", args.out)
    return 0


def cmd_export(args, db: DB) -> int:
    rows = load_rows(db, args.with_text)
    if not rows:
        log.error("no listings in the database; run `crawl` first")
        return 1
    if args.active_only:
        rows = [r for r in rows if r["active"]]
    base = Path(args.out)
    stats = db.stats()
    fmts = ["csv", "xlsx", "jsonl"] if args.format == "all" else [args.format]
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
    return 0


def cmd_status(args, db: DB) -> int:
    s = db.stats()
    print(json.dumps(s, indent=2))
    for r in db.conn.execute("SELECT source, COUNT(*) c, MAX(fetched_at) last FROM pages GROUP BY source"):
        total = db.meta_get(f"total:{r['source']}")
        print(f"source: {r['source']}\n  pages in current cycle: {r['c']}  last fetch: {r['last']}  site total: {total}")
    print("recent runs:")
    for r in db.conn.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 6"):
        print(f"  #{r['id']} {r['kind']:7s} {r['status']:11s} complete={r['complete']} started={r['started_at']} "
              f"new={r['new_listings']} stopped={r['stopped_reason']}")
    if s["detail_pending"]:
        print(f"detail pages pending: {s['detail_pending']}  (~{fmt_eta(s['detail_pending'] / max(args.rps, 0.01))} at --rps {args.rps})")
    return 0


# --------------------------------------------------------------------------- CLI
def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=DEFAULT_DB, help="SQLite file (default: %(default)s)")
    common.add_argument("--url", action="append", help="results URL (repeatable). Default: the Alexandria for-rent URL you gave")
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

    sp = sub.add_parser("probe", parents=[common], help="fetch list page(s) + one detail page, save HTML, show parse")
    sp.add_argument("--samples-dir", default="samples")
    sp.add_argument("--listing-url", help="specific listing URL to test instead of the first card")
    sp.add_argument("--no-detail", action="store_true")
    sp.add_argument("--deep", action="store_true", help="also fetch a few deep pages (up to the last) to test for a pagination cap")

    sp = sub.add_parser("crawl", parents=[common], help="phase 1: result pages -> listings")
    sp.add_argument("--max-pages", type=int, default=1000, help="hard cap per source (use 3 for a smoke test)")
    sp.add_argument("--refresh", action="store_true", help="discard an unfinished cycle and start a new snapshot")
    sp.add_argument("--passes", type=int, default=1, help="full cycles back to back (2 improves coverage on a shifting site)")
    sp.add_argument("--delist-after", type=int, default=2,
                    help="mark a listing inactive after N consecutive COMPLETE cycles that missed it (default 2)")

    sp = sub.add_parser("details", parents=[common], help="phase 2: listing pages -> extra fields")
    sp.add_argument("--limit", type=int, help="only process N listings (smoke test)")
    sp.add_argument("--retry-errors", action="store_true")
    sp.add_argument("--redo", action="store_true", help="re-fetch every active listing, even finished ones")

    sp = sub.add_parser("reparse", parents=[common], help="re-run parsers on stored HTML (no network)")
    sp.add_argument("--what", choices=["cards", "details", "all"], default="all")

    sp = sub.add_parser("validate", parents=[common], help="data-quality flags")
    sp.add_argument("--out", help="write flagged listings to this CSV")

    sp = sub.add_parser("export", parents=[common], help="write csv/xlsx/jsonl/parquet")
    sp.add_argument("--format", choices=["csv", "xlsx", "jsonl", "parquet", "all"], default="all",
                    help="'all' = csv + xlsx + jsonl; parquet needs pyarrow and is requested explicitly")
    sp.add_argument("--out", default="aqarmap_alexandria_rent", help="output path without extension")
    sp.add_argument("--with-text", action="store_true", help="include the stored detail-page text column")
    sp.add_argument("--active-only", action="store_true", help="skip listings marked delisted")

    sub.add_parser("status", parents=[common], help="progress, runs and data quality")
    sub.add_parser("areas", parents=[common], help="list district URLs for sharding")
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
        sync = {"export": cmd_export, "status": cmd_status, "reparse": cmd_reparse, "validate": cmd_validate}
        if args.cmd in sync:
            return sync[args.cmd](args, db)
        return asyncio.run(run_async(args, db))
    except RobotsDisallowed as e:
        log.error("%s", e)
        return 2
    except BlockedError as e:
        log.error("BLOCKED: %s", e)
        log.error("Progress is saved. Options: wait and re-run (it resumes), lower --rps (try 0.3), or use --proxy with an "
                  "Egyptian exit. If the site keeps blocking, ask them for a data feed instead of escalating.")
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