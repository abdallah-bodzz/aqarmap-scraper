# Scraper Handoff Spec — Aqarmap Listing Detail Page

**Target:** `https://aqarmap.com.eg/en/listing/7416775-for-rent-alexandria-el-mandara-alex-el-mandara-bahri-sidi-kamal-st/`

---

## 1. Page Overview

| Item | Value |
|---|---|
| Page type | Single listing detail page ("unit listing") |
| Site framework | **Next.js (App Router, React Server Components)** — proven by `<script>self.__next_f.push([1,"..."])</script>` streaming payload, `$L*` lazy refs, `__next-route-announcer`, `data-nnext="afterInteractive"` GTM script, `data-nimg="1"` on `<img>`. |
| Rendering | **Server-rendered.** Full listing payload is embedded in the initial HTML response inside `__next_f` chunks + two JSON-LD `<script>` blocks. Client render occurs but data is already in the response. |
| Records on page | **1 primary listing.** Two *other* listings (ids `7298779`, `7287553`) appear in the "similar listings" module and are **not** the target — dedupe by `id`. |
| Pagination | N/A (detail page). Seller's other listings count is exposed via `topSellerData[0].numberOfActiveListing` = `"11"`, but no paginated list is rendered on this page. |
| Locale | `/en/` — English. `translations[]` array carries `ar` strings; top-level fields are English. |
| Currency | `EGP` (from JSON-LD `offers.priceCurrency`). |

**Decision:** `requests` + `lxml`/`selectolax` is sufficient. The phone number reveal requires JS (see §5).

---

## 2. Container Definition

This is a **detail page — 1 record, 1 container**. The container is *the payload object*, not a DOM node.

### Primary (recommended) — JSON-LD block
- Selector: `//script[@id="listing-details-schema"]`
- Extract `.text()`, `json.loads`, navigate `@graph` array, find `{"@type": "RealEstateListing"}`.
- **1 match.**

### Secondary — Next.js RSC payload (supplementary fields only)
- Regex: `<script>self\.__next_f\.push\((\[1,".*?"\])\)</script>` (DOTALL)
- Concatenate the decoded second element of each match → single RSC stream string.
- Locate `"listing":{"id":7416775` (or the largest `"listing":{...}` block via bracket counting) → that object is the target.
- **Multiple copies of the listing object exist in the RSC stream** (`4c:...header:props:listing`, `52` sticky header, `56` mobile sticky footer). Deduplicate on `id`.

### Fallback — DOM
- Breadcrumb `<li>` chain under `nav` (verify `id="Breadcrumb-schema"` script; DOM breadcrumb not reliably captured here).
- **Do not use DOM for primary fields** — most data is component-passed via props (`$L*` refs) and is not in the rendered HTML capture (e.g. `<h1>` title *is* rendered; price *is not* visible in the captured DOM — it's in a `$L5f` component whose render we don't have).

### Nested containers to avoid
- `$L6b` (similar listings): 2 other listing objects at `53:props:children:props:children:17:0:props:listings[0..1]`.
- `topSellerData` (seller profile card) — user object, not a listing.
- `$L67` (agent card): same user object repeated.

---

## 3. Fields Table

### Primary source: JSON-LD `<script id="listing-details-schema">` → `@graph[3]` (`@type: RealEstateListing`)

| Field | Selector / Location | Type | Extraction Notes | Warnings |
|---|---|---|---|---|
| `listing_id` | `.url` → regex `/listing/(\d+)` | int | Cast to int. | Also at `itemOffered.@id` suffix. Prefer URL regex — matches the visible URL slug. |
| `title` | `.name` | str | — | Truncated variant of the Arabic title; English source is short by design (see `translations[]` below). |
| `url` | `.url` | url | — | — |
| `date_posted` | `.datePosted` | datetime | ISO 8601 UTC → `datetime.fromisoformat(dt.replace("Z","+00:00"))`. | **Value is `2026-11-17T19:41:27Z` — future-dated.** Do not assert `date <= now`. |
| `category` | `.category` | str | Enum: `ForRent` \| `ForSale`. | — |
| `price` | `.offers.price` | int | int. | Matches `payment_plans[duration=30].price`, **not** `duration=1`. See §5. |
| `currency` | `.offers.priceCurrency` | str | — | — |
| `availability` | `.offers.availability` | str | Strip `https://schema.org/`. | — |
| `property_type` | `.itemOffered.@type` | str | — | Matches `property_type.title` in payload. |
| `rooms` | `.itemOffered.numberOfRooms` | int | — | Also in `attributes[name=rooms].value` (str). Cross-check. |
| `baths` | `.itemOffered.numberOfBathroomsTotal` | int | — | Also in `attributes[name=baths].value` (str). |
| `area_m2` | `.itemOffered.floorSize.value` | int | — | `unitCode` = `MTK` (m²). |
| `street` | `.itemOffered.address.streetAddress` | str | — | Contains `"Sidi Kamal St., El Mandara Bahri"` — comma-separated, do not split unless you need the components. |
| `country` | `.itemOffered.address.addressCountry` | str | — | Always `"EG"`. |
| `lat` | `.itemOffered.geo.latitude` | float | — | — |
| `lng` | `.itemOffered.geo.longitude` | float | — | — |
| `amenities_jsonld` | `.itemOffered.amenityFeature[*].name` | list[str] | Filter on `.value == true`. | This list is broader than `.amenities` in the RSC payload — see §5. |
| `images` | `.image` | list[url] | — | 9 URLs; all `slider-photo-watermarked-logo-large` variant. Use as-is; **do not** attempt to reconstruct from `photos[].file.thumbnails` unless you want alternate sizes. |

### Primary source: RSC payload — `"listing": { ... }` (largest object, id must match)

| Field | Location | Type | Extraction Notes | Warnings |
|---|---|---|---|---|
| `id` | `listing.id` | int | — | — |
| `slug` | `listing.slug` | str | e.g. `for-rent-alexandria-el-mandara-alex-el-mandara-bahri-sidi-kamal-st`. | Combined with `listing.id` forms canonical URL. |
| `section_slug` | `listing.section.slug` | str | `for-rent` \| `for-sale`. | — |
| `property_type_slug` | `listing.property_type.slug` | str | e.g. `furnished-apartment`. | — |
| `property_type_title` | `listing.property_type.title` | str | — | — |
| `price` | `listing.price` | int | — | Same as JSON-LD. |
| `area_m2` | `listing.area` | int | — | — |
| `address` | `listing.address` | str | — | Same as JSON-LD `streetAddress`. |
| `description_en` | `listing.description` | str | — | Short (one sentence). See `translations` for a fuller Arabic description. |
| `center_lat` | `listing.center_lat` | float | — | — |
| `center_lng` | `listing.center_lng` | float | — | — |
| `status` | `listing.status` | int | `2` = live (enum inferred — mapping not in static DOM). | — |
| `payment_method` | `listing.payment_method` | int | `1` = `from_owner_cash` (from `paymentMethodLabel`). | — |
| `reference_number` | `listing.reference_number` | str | `"123"` in this record. | May be `null` on other listings. |
| `is_featured` | `listing.is_featured` | bool | — | — |
| `was_featured` | `listing.was_featured` | bool | — | — |
| `is_hold` | `listing.is_hold` | bool | — | — |
| `isCallRequest` | `listing.isCallRequest` | bool | If `true`, headline price is hidden ("Call for price"). | This listing is `false` → price shown. |
| `accepts_messages` | `listing.accepts_messages` | bool | — | — |
| `has_whatsapp` | `listing.has_whatsapp` | bool | — | — |
| `phone_number` | `listing.phones[0].number` | str | **MASKED** — value is `"+20115399xxxx"`. | See §5 (blocker). |
| `whatsapp_number` | `listing.whatsAppNumber` | str | **MASKED** — same pattern. | Same as above. |
| `country_code` | `listing.phones[0].countryCode` | str | `"+20"`. | — |
| `seller.id` | `listing.user.id` | int | — | — |
| `seller.full_name` | `listing.user.full_name` | str | — | — |
| `seller.type` | `listing.user.user_type` | int | `1` = agency (inferred). | Numeric code; mapping not exposed. |
| `seller.active_listings` | `listing.user.activeListingsCount` | int | — | Note: differs from `topSellerData[0].numberOfActiveListing` (`60` vs `"11"`) — **two different counters** exist. Prefer `user.activeListingsCount`. |
| `seller.score` | `listing.user.score` | int | — | Seller score, not listing score. |
| `seller.logo` | `listing.user.logo.thumbnails.large` | url | — | — |
| `seller.personal_photo` | `listing.user.personal_photo.thumbnails.large` | url | — | — |
| `attributes.rooms` | `attributes[custom_field.name=="rooms"].value` | int | `custom_field.name` is ASCII (`rooms`); `custom_field.label` is Arabic (`الغرف`). **Index by `name`, not `label`.** | Cast str→int. |
| `attributes.baths` | `attributes[custom_field.name=="baths"].value` | int | Same. | — |
| `attributes.floor` | `attributes[custom_field.name=="floor"].value` | int | Floor = `12`. | Not present in JSON-LD. |
| `attributes.finish_type` | `attributes[custom_field.name=="finish-type"].value` | str | `SUPER_LUX` (enum). | Not present in JSON-LD. |
| `amenities_codes` | `listing.amenities` | list[str] | `["SECURITY","ELEVATOR"]`. **Only 2 entries here**, whereas `property_type.amenities` lists all 15 possible codes. | Do not confuse `listing.amenities` (actual) with `property_type.amenities` (allowed). |
| `payment_plans` | `listing.payment_plans` | list[obj] | Each: `{total_down_payment, total_remaining_amount, duration (days), price, security_deposit, over_price, down_payment, down_payment_percentage}`. | **Two plans present.** `[0]`: `duration=30, price=20000` (= headline). `[1]`: `duration=1, price=800`. See §5. |
| `price_per_meter` | `listing.price_per_meter` | float | `142.857...`. | — |
| `video_url` | `listing.video_url` | url\|null | — | `null` here. |
| `brochure` | `listing.brochure` | url\|null | — | `null`. |
| `meta_title` | `listing.meta_title` | str\|null | — | `null`. |
| `meta_description` | `listing.meta_description` | str\|null | — | `null`. |
| `property_view` | `listing.property_view` | int | `2` (main street); label key = `label.property_view.main_street`. | Label is **i18n key**, unresolved. |
| `translations` | `listing.translations` | list[obj] | Each `{locale, title, description, address}`. `locale` ∈ `{ar, en}`. | Arabic description is **longer** than English (§5). |
| `updated_at` | `listing.updated_at` | datetime | ISO 8601 with tz offset (`+02:00`). | — |
| `created_at` (listing) | `listing.updated_at` (only timestamp present) | datetime | — | No `created_at` for listing itself in payload. |

### Secondary source: RSC payload — `sections` array (for enum resolution)

| Field | Location | Type | Extraction Notes | Warnings |
|---|---|---|---|---|
| `section_map` | `"sections":[{...}]` | dict | Build `{section.id: {title, slug}}`. Used to resolve `listing.section.id`. | Also confirms `1=For sale`, `2=For rent`. |

### Secondary: `<script id="Breadcrumb-schema">`

- Presents `BreadcrumbList` with 7 `ListItem`s — use for breadcrumb path (city / neighborhood / street). Each `ListItem.item.@id` is a *relative* path; join with `https://aqarmap.com.eg`.
- **Do not** use for field extraction; use only if you need breadcrumb text.

---

## 4. Dynamic / Hidden Data

**All key fields are present in the initial HTML response.** No lazy-loaded data for the listing itself. Specifically:

| Source | Path | Status |
|---|---|---|
| JSON-LD `RealEstateListing` | `<script id="listing-details-schema">` → `@graph[3]` | ✅ Available in initial HTML. |
| Full listing object | `__next_f` payload → `"listing":{"id":7416775,...}` | ✅ Available in initial HTML. |
| Section enum | `__next_f` → `"sections":[...]` | ✅ Available. |
| Breadcrumbs | `<script id="Breadcrumb-schema">` | ✅ Available. |
| Phone number (unmasked) | — | ❌ **NOT IN STATIC DOM — REQUIRES JS / API POST.** Value is masked server-side as `+20115399xxxx`. |
| WhatsApp number (unmasked) | — | ❌ Same. |
| Rating score for the listing | `listing.score` | `null` on this record. If populated, would be in payload, not DOM. |
| Dynamic "view count" / analytics | — | ❌ Not present. |
| Similar listings | `$L6b` → `listings[]` in RSC payload (2 items) | ✅ Available; **exclude** from primary record. |
| Seller's other listings (paginated) | — | ❌ Not on this page. `numberOfActiveListing` gives the count only. |

**Implementation choice:** Parse **JSON-LD first** (stable, spec-backed schema), then **regex/bracket-parse the RSC stream** for the supplementary fields (attributes, payment plans, translations, flags). Use `requests` unless you need the unmasked phone — then `Playwright`.

---

## 5. Practical Warnings (page-specific)

- **Price is ambiguous — two plans exist.** `payment_plans[0]` = `{duration: 30, price: 20000}`, `payment_plans[1]` = `{duration: 1, price: 800}`. The headline `listing.price` = `20000` corresponds to the **30-day** plan. Do not assume the first plan is monthly — check `duration` field: `30` = monthly, `1` = daily. Some listings may only have one plan. Do not crash when `payment_plans == []`.
- **Phone / WhatsApp are masked as `+20115399xxxx`.** The trailing `xxxx` is literal. Same masking in both `phones[0].number`, `listing_phones[0].number`, `listing.user.phone_number`, `listing.user.whatsApp_number`, `listing.whatsAppNumber`. Do not `int()`-cast, do not `regex` extract digits and expect 12 digits — you'll get 11 with 4 x's. Route to `phone_raw` and mark `phone_masked=True`.
- **`datePosted` is future-dated** (`2026-11-17`). This is what the site returned; treat as opaque date. Do not assert `<= now`, do not filter by date freshness on this dataset without re-checking.
- **`custom_field.label` is Arabic even on `/en/` pages.** `attributes[*].custom_field.label` = `"الغرف"`, `"الحمامات"`, etc. Index `attributes` by `custom_field.name` (`rooms`, `baths`, `floor`, `finish-type`). Never key on `label`.
- **Two different `activeListings` counters.** `listing.user.activeListingsCount` = `60`; `topSellerData[0].numberOfActiveListing` = `"11"` (string!). Different semantics — the first is likely total, the second "active". Pick one and document; do not use both.
- **`.name` (title) differs from `.translations[1].title`.** Top-level `title` = `"For rent - Apartment 140 M² Super lux"`; `translations[locale=en].title` = same. But `translations[locale=ar].title` = `"شقة مفروشة للايجار اليومي بالمندرة بحري"` — **do not conflate**. If you need English, use top-level; if you need Arabic, use `translations[locale=ar]`.
- **English `description` is truncated relative to Arabic.** English: `"Apartments For rent in Sidi Kamal St. with size 140 M² View Main Street"` (one sentence). Arabic (`translations[locale=ar].description`): `"ريسبشن قطعتين \n3غرف \n2حمام \nمطبخ \nاجهزة كهربائية كامله \nمكيف \nانترنت \nامن للعقار \n"` (multi-line feature list). This is intentional on Aqarmap — English descriptions are often templated. Capture both locales.
- **`listing.amenities` (2 items: `SECURITY`, `ELEVATOR`) ≠ `listing.property_type.amenities` (15 possible codes).** The former is the *actual* amenity set for this unit; the latter is the *allowed* set for the property type. Do not union them.
- **`status_label`, `property_view_label`, `paymentMethodLabel` are i18n keys**, not resolved strings — e.g. `"label.status.live"`, `"label.property_view.main_street"`, `"label.payment_method.from_owner_cash"`. Do not surface directly; store raw + resolve via `status`/`property_view`/`payment_method` numeric enums.
- **Duplicate `listing` objects in the RSC stream.** The same listing object is serialized inside `header.props.listing`, `stickyHeader`, and possibly as a `header.props.children` reference. Deduplicate on `listing.id`. If you naively regex-all `"listing":{`, you will double-count.
- **`$L6b` similar-listings are NOT the target.** Two other records (`7298779`, `7287553`) exist in the payload as recommendations. Filter on `id == listing_id` before any list operations.
- **`category: 5`** — numeric code, mapping NOT IN STATIC DOM. If you need `category` as text, read `property_type.title` or `section.title` instead.
- **`seller_role: 1`** — numeric code (1 = agent/agency, likely; not confirmed). Treat as opaque int.
- **`updated_at` uses `+02:00` offset, `datePosted` uses `Z`** — both are the same instant here (`19:41:27Z` == `21:41:27+02:00`). Use `dateutil.parser.parse` or equivalent; do not assume uniform suffix.
- **`topSellerSuccessModal` is present in the captured DOM** ("Your request was sent successfully", "Back to Listing Details") — the captured state is post-lead-submission. **Do not** use `dialog[open]` presence as a page-state signal; it is stateful per user session.
- **Image URLs contain a watermark segment** (`slider-photo-watermarked-logo-large/2309/64f42fb007a19559357053.png`) — this is the seller's logo path, not a stable constant. Treat the full URL as opaque.
- **`phones[0].id` may be `null`** for some records (`phones` in the similar-listings block has empty `phones: []`). Guard against `phones[0]` IndexError.
- **`eligibleForMortgageData: []`** — always array; do not assume dict.
- **`paymentMethodLabel`** in payload is always present even when `payment_method` = `1`; but the DOM does not render it for value `1`. Do not parse payment method from DOM.

---

## 6. Pagination & Rate Limits

- **Pagination:** N/A — detail page. Single record.
- **Adjacent-list pagination:** The `similar listings` module is hard-capped at 2 items on this page; no "load more" is rendered. Seller's full listing list is at `/en/user/1819928` (URL from the `topSellerSuccessModal` modal) — **not crawled here**.
- **Rate limits:** Not observable from static HTML. **`robots.txt` not in the capture — fetch it once manually before production.** No CAPTCHA/challenge script is present in this HTML.
- **Session state:** The `topSellerSuccessModal` indicates a lead was submitted in the session that captured this HTML; other sessions may see a `topSellerContactModal` or similar. Do not depend on dialog presence.

---

## 7. Anti-Scraping / Robustness Signals

- **No Cloudflare / Akamai / DataDome challenge** in this HTML. No `cf-chl`, no `_Incapsula`, no `window.__CF$cv$params`.
- **No honeypot links** (`display:none` `<a>` with hidden values) observed.
- **No `noscript` fallback** containing data — the `<noscript>` content, if any, would render duplicate static markup; not observed here.
- **Auto-generated class names:** Yes, extensively. `navbar-constrained`, `layout-boundary__inner`, `element-center`, `bg-green_light` are not semantic IDs — they're utility classes with hash-like suffixes in other parts of the site. **Do not target these as primary selectors.**
- **Stable `id` attributes present:** `#listing-details-schema`, `#Breadcrumb-schema`, `#navbar`, `#nprogress`, `#listingRateModal`, `#topSellerSuccessModal`, `#_next-gtm`. Use these for anchors; do not rely on Tailwind classes.
- **`data-nimg`, `data-nscript`** — Next.js framework markers, stable across pages on same framework major version. Safe to use as hints, not as primary selectors.
- **GTM id `GTM-WSHP5Q`** — noise.
- **Recommended client:** `requests` + `httpx` fallback. **Reason from HTML:** entire listing payload + both JSON-LD blocks are in the initial response; no observable JS challenge. **Only reach for `Playwright` if you need the unmasked phone number** (see §5). If you do, a single tab navigation + wait for `#listing-details-schema` is sufficient — no SPA scroll/pagination to trigger.
- **Cookie / auth:** Not observable. The phone reveal endpoint is almost certainly session-authenticated and out of scope here.

---

## 8. Validation Checklist

Encode as post-parse assertions (raise or quarantine on failure):

```python
assert result["listing_id"] == 7416775                       # primary key
assert isinstance(result["listing_id"], int)

assert result["price"] == 20000                               # JSON-LD offers.price
assert isinstance(result["price"], int) and result["price"] > 0
assert result["currency"] == "EGP"

assert result["area_m2"] == 140                               # floorSize.value
assert result["rooms"] == 3
assert result["baths"] == 2

assert result["date_posted"].tzinfo is not None               # tz-aware
assert result["url"].startswith("https://aqarmap.com.eg/en/listing/7416775-")

assert -90 <= result["lat"] <= 90
assert -180 <= result["lng"] <= 180

assert len(result["images"]) == 9                             # for this ID; do not hard-code across IDs
assert all(u.startswith("https://img-") for u in result["images"])

assert result["property_type_slug"] == "furnished-apartment"
assert result["section_slug"] == "for-rent"
assert result["category"] == "ForRent"

# Plan sanity
assert any(p["duration"] == 30 and p["price"] == 20000 for p in result["payment_plans"])
assert any(p["duration"] == 1  and p["price"] == 800   for p in result["payment_plans"])

# Attributes indexed by `name`, not `label`
attr = {a["custom_field"]["name"]: a["value"] for a in result["attributes"]}
assert attr["rooms"] == "3" and attr["baths"] == "2" and attr["floor"] == "12"

# Seller
assert result["seller"]["id"] == 1819928
assert result["seller"]["full_name"] == "Ahmed Zain"

# Phone is masked — do not attempt to int() or len()==12
assert result["phone_raw"].endswith("xxxx")
assert result["phone_masked"] is True

# Dedupe check
listing_ids_found = {rec["id"] for rec in all_parsed_listing_objects}
assert listing_ids_found == {7416775, 7298779, 7287553}       # primary + 2 similar
assert sum(1 for r in all_parsed_listing_objects if r["id"] == 7416775) >= 1
# ...and after dedupe on id, the primary record is the one with most fields populated.
```

Field fill rates expected for this record: **100%** for all fields in §3 except `video_url`, `brochure`, `meta_*`, `listing.score`, `propertyRegistrationStatus`, `compound_field`, `parent` (all `None` here).

---

## 9. Open Questions / Blockers

Ranked by risk:

| # | Risk | Blocker | Resolution path |
|---|---|---|---|
| 1 | **High** | **Unmasked phone/WhatsApp number is not in the static HTML.** Server returns `+20115399xxxx`. | Inspect the "Show number" button's network call in a browser trace (`POST /api/...` or `GET /listing/{id}/phone`). May require session cookie + `X-CSRF-Token`. Not resolvable from this capture. |
| 2 | Med | **Category / seller_role / status numeric codes** have no exposed mapping. `category: 5` and `seller_role: 1` are inferred from context (`property_type.title`, i18n label keys), not from an explicit enum table. | Crawl multiple listing pages to build the mapping, or find the site's public enum dictionary. |
| 3 | Med | **`__next_f` stream structure is Next.js-internal.** RSC IDs (`4b:`, `4c:`, `1f:`, `$L51`) will change across framework versions. Do not hard-code `4c:props:children:2:props:header:props:listing` as a path. | Parse by **content signature** (`"listing":{"id":`), not path. Pin `requests` parser + JSON-LD-only as degradation path. |
| 4 | Low | **Arabic vs English description length asymmetry** may reflect a UI optimization we can't verify (English UI may truncate for brevity, but source data may be richer). | Compare against an `ar` page scrape (`/ar/listing/7416775-...`) before assuming `translations[ar].description` is canonical. |
| 5 | Low | **`robots.txt` and rate-limit headers are not in this capture.** | Fetch `https://aqarmap.com.eg/robots.txt` and inspect response headers once manually. |
| 6 | Low | **`similar listings` cap of 2** may be responsive/AB-tested; on mobile or other locales it may differ. | Verify with a second capture at a different viewport, or accept 0–N in validation. |