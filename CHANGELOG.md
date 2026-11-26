# Changelog

All notable changes to this project are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Version to schema mapping:

| App version | `__version__` | `SCHEMA_VERSION` |
|---|---|---|
| v1.0.0 | 1.0.0 | n/a (pre-schema) |
| v2.0.0 | 2.0.0 | 2 |
| v3.0.0 | 3.0.0 | 3 |
| v3.1.0 | 3.1.0 | 4 (operational only) |
| v4.0.0 | 4.0.0 | 4 (full detail-page schema) |

v3.1.0 and v4.0.0 both target schema 4 but with different migrations. Read the v4.0.0
notes before upgrading a v3.0.0 database.

## [Unreleased]

## [4.0.0] - 2026-11-25

Detail pages become a first-class source. The RSC payload extractor, the detail-page
`RealEstateListing` JSON-LD, and the breadcrumb schema all feed the same row. The
database schema stays at version 4 but adds 37 new columns. A repository scaffold ships
with the code: tests, fixtures, docs, scripts, `pyproject.toml`.

### Added

- **Detail-page payload extraction.** `extract_payload(html, target_id)` no longer raises
  when `"initialListings":` is absent. It falls back to `decode_listing_objects`, which
  finds every `"listing":{...}` object in the React Server Components stream by content
  signature (the key, then a string-aware bracket match). Duplicates are grouped by
  `id` and deduplicated by field count (`_populated`); the richest copy is kept.
  Target id resolution: the URL, then the JSON-LD `id`, then the most repeated id in
  the stream.
- **Detail-page JSON-LD.** `extract_detail_extras` reads `<script id="listing-details-schema">`
  and `<script id="Breadcrumb-schema">`. `parse_detail_ld` decodes the `RealEstateListing`
  node into a flat dict: `id`, `name`, `url`, `datePosted`, `category`, `offers.price`,
  `offers.priceCurrency`, `offers.availability`, `itemOffered.@type`, `numberOfRooms`,
  `numberOfBathroomsTotal`, `floorSize.value`, `address.*`, `geo.*`,
  `amenityFeature[*].name`, `image[*]`. `parse_breadcrumb_names` handles both `item`
  shapes (dict with `name`, or URL string) sorted by `position`.
- **37 new parsed columns.** `section_slug`, `address`, `description_en`, `status`,
  `status_label`, `reference_number`, `is_featured`, `was_featured`, `is_hold`,
  `accepts_messages`, `has_whatsapp`, `video_url`, `brochure`, `meta_title`,
  `meta_description`, `price_per_meter`, `seller_type`, `seller_active_listings`,
  `seller_score`, `seller_logo_url`, `seller_photo_url`, `amenities_codes`,
  `payment_plans_json`, `translations_ar_title`, `translations_ar_description`,
  `translations_ar_address`, `property_view_label`, `property_type_amenities_json`,
  `date_posted_iso`, `availability`, `country`, `amenities_jsonld`, `images_jsonld`,
  `category_ld`, `property_type_ld`, `breadcrumb_path`, `payload_kind`.
- **Four supporting columns.** `raw_aux_json` (compressed JSON-LD + breadcrumbs +
  detail payload, so `reparse` can redo the merge offline), `detail_reference`,
  `detail_property_type`, `detail_reception`.
- **`crawl --url <listing URL>`.** Fetches a single listing. New listings are inserted.
  Known listings are enriched in place (same row, no duplicate).
- **`probe --url <listing URL>`.** Probes a detail page and prints a spec checklist.
- **`spec_checks`.** Twelve structural assertions a detail record must satisfy: id is
  an int, url contains the listing id, price positive (or call-for-price), currency set,
  `date_posted_iso` is a tz-aware UTC timestamp, lat/lng in range, JSON-LD images are
  HTTPS URLs, `section_slug` is `for-rent`/`for-sale`, payment plans keep durations,
  monthly price matches the 30-day plan, daily rate matches the 1-day plan, no phone
  number in `raw_json`.
- **`detail_canary`.** Structural checks a detail record must pass on `probe`/`crawl`:
  page parsed into exactly one record, payload-sourced, `title` and `area_sqm` present,
  price present or call-for-price, price period resolvable, payload price equals JSON-LD
  price.
- **`merge_rows`, `merge_detail_record`, `enrich_from_detail`, `aux_detail_row`.**
  Combine a results-page row and a detail-page row of the same listing. v3 columns
  (payload / card) prefer the base row and the detail row only fills NULLs. v4 columns
  prefer the detail row. `enrich_from_detail` records the detail payload, JSON-LD and
  breadcrumbs in `raw_aux_json` so `reparse` can rebuild the merge offline.
- **`ld_*` disagreement fields.** Every JSON-LD vs payload mismatch on `price`, `area`,
  `beds`, `baths` is recorded in the `disagreements` table alongside the existing
  payload-vs-card rows, distinguishable by the `ld_` prefix on the field name.
- **`details --plan`.** Prints the exact counts of missing fields per active listing and
  the estimated cost of a full detail pass. No network.
- **`details --fields jsonld|amenities|plans|translations`.** Restricts the pass to
  listings missing the named fields. Rejects unknown values with a clear message listing
  the valid keys.
- **`details --redo`.** Re-fetches every active listing, even ones already marked
  finished. Used to back-fill rows fetched by an older version.
- **`_migrate_3_to_4`.** Adds the 37 new columns and back-fills them from the payload
  already stored in `raw_json`. Existing columns are not touched. A backup is made first
  (`<db>.bak-v3-<timestamp>`). The migration logs a reminder to run `reparse` for the
  columns that v4 improves on existing rows (coordinates, `daily_rate`, `price_monthly`).
- **Repository scaffold.** `.gitignore`, `README.md`, `CHANGELOG.md`, `LICENSE`,
  `pyproject.toml`, `requirements.txt`, `requirements-dev.txt`. Directories: `tests/`,
  `docs/`, `scripts/`, `data/`, `samples/`, `legacy/`.
- **Tests.** `tests/test_spec_checks.py` (the twelve spec assertions, one function
  each), `tests/test_parsing.py` (results pages, failure paths, detail-page merge,
  v3 → v4 migration), `tests/conftest.py` (fixture loaders). Fixtures:
  `tests/fixtures/detail_7416775.html`, `tests/fixtures/results_page_1.html`.
- **Scripts.** `scripts/gen_fixtures.py` regenerates the two fixtures from a spec
  description. `scripts/run_full_pipeline.ps1` runs a first-time Windows pipeline
  (`probe` → `crawl` → `reparse` → `details --limit 5` → `validate` → `export`) with a
  `-DryRun` switch that prints the commands without executing them.
- **Docs.** `docs/SPEC.md` (the detail-page handoff spec), `docs/FIELDS.md` (spec field
  to database column), `docs/RUNBOOK.md` (every subcommand, flags, exit codes, failure
  recovery, migration steps).
- **Atomic exports.** `atomic_write` writes to a temp file and `os.replace`s.
  `PermissionError` (Excel holds the file on Windows) raises `ExportLocked`, exit code 6.
- **Grouped XLSX data dictionary.** The `Data dictionary` sheet in every XLSX export
  gets a `Group` column (Identity, Price, Property, Location, Advertiser, Promotion,
  Media, Dates, Translations/SEO, Crawl/flags).

### Changed

- **`extract_payload` falls back to the detail-page listing object.** If
  `"initialListings":` is absent and the RSC stream carries `"listing":{...}`, the
  function returns a single-item payload instead of raising `PayloadError`. Results pages
  still take the `initialListings` path unchanged.
- **`_LAT_KEYS = ("lat", "latitude", "center_lat")`, `_LNG_KEYS = ("lng", "lon",
  "longitude", "center_lng")`.** Payload coordinates under `center_*` were previously
  skipped. `in_bbox` still rejects anything outside the Alexandria box.
- **`RESIDENTIAL_TYPES` includes `furnished-apartment`, `furnished-villa`,
  `furnished-duplex`, `furnished-penthouse`, `furnished-studio`, `furnished-chalet`.**
  The detail-page slug is `furnished-apartment`; without these entries, `beds` and
  `baths` came out NULL.
- **`has_whatsapp` removed from `_PHONE_KEYS`.** It is a boolean, kept as data. Phone
  numbers themselves are still scrubbed.
- **`daily_rate` from `payment_plans`.** `plan_price(plans, 1)` reads the plan whose
  `duration` is exactly 1. The card-derived value is only a fallback for pages without
  a payload. On detail-only rows (no card), the daily plan was previously dropped;
  it is now stored.
- **`price_monthly` from `payment_plans`.** `plan_price(plans, 30)` reads the plan whose
  `duration` is exactly 30. `monthly_equiv` (day x30, week x52/12, year /12) is the
  fallback. This matches how the site itself labels the headline price.
- **`_upsert_sql`.** `payload_kind` is preserved as `'detail'` when a row was first
  seen as a detail page. v4 columns keep their detail values when a later results crawl
  re-sees the row. v3 columns keep the fresh sighting's values.
- **`normalize_type_slug(pd_.get("propertyTypeSlug") or pt.get("slug"))`.** Accepts
  both the results-page shape (`previewDetails.propertyTypeSlug`) and the detail-page
  shape (`property_type.slug`). `g(o, *keys)` is a general cross-shape getter.
- **`norm_attrs`** handles both shapes: a plain dict (results page) and a list of
  `{custom_field: {name, label}, value}` (detail page). Indexes by `custom_field.name`
  (`rooms`, `baths`, `floor`, `finish-type`), never by the Arabic `label`.
- **`_plans`, `plan_price`, `DURATION_PERIOD`.** Pull monthly and daily prices from
  `payment_plans` (or `paymentPlans`) by `duration`. `DURATION_PERIOD = {1: "day",
  7: "week", 30: "month", 365: "year"}`.
- **`translation(p, locale)`** retrieves the Arabic translation block. `_thumb` and
  `_amenity_codes` are helpers.
- **`_populated`** counts non-empty leaves. Used to pick the richest duplicate copy of
  the listing object when it is serialized more than once in the RSC stream.
- **XLSX export grows a grouped `Data dictionary` sheet.** Every export column gets a
  row: key, header, arrow type, source, meaning, NULL meaning, group.

### Fixed

- **`<script id="listing-details-schema">` RealEstateListing JSON-LD was never read.**
  v3.1 could only pull the posting date from `<time datetime>`. v4 decodes the whole
  node: price, currency, availability, rooms, baths, area, geo, images, date posted,
  amenities, property type, address. JSON-LD wins over the payload for these specific
  fields.
- **`center_lat` / `center_lng` were skipped by `_find_key`.** Now read.
- **`daily_rate` was empty on detail-only rows.** It came from the card period, which
  does not exist on a detail page. Now derived from `payment_plans[duration=1].price`.
- **`payment_plans[1]` (the daily plan) was dropped.** In v3, only `payment_plans[0]
  .duration` was stored. v4 stores the full `payment_plans` list in
  `payment_plans_json` and derives `daily_rate` from it.
- **`listing.amenities` (actual) vs `property_type.amenities` (allowed) vs
  `amenities_jsonld` (JSON-LD `amenityFeature`) were conflated.** Three separate
  columns now: `amenities_codes`, `property_type_amenities_json`, `amenities_jsonld`.
- **`"initialListings": [` with whitespace after the colon failed to parse.** The
  marker scan now tolerates whitespace.
- **XLSX `Data dictionary` sheet was missing from earlier exports.** Added.
- **`add_params` duplicated query parameters.** `?page=N` no longer appears twice.

### Notes

- **v4.0.0 was developed from the v3.0.0 codebase, not v3.1.0.** The v3.1.0 changes
  (`DbLock` per-database lock, `--assume-period`, one-miss-per-UTC-day rule,
  `RESUMABLE_STOPS`, `description`/`amenities` from the payload) are documented under
  v3.1.0. They are **not present in v4.0.0** unless merged separately. If you need the
  per-database lock or `--assume-period` on v4, re-apply from the v3.1.0 branch.
- **Schema migration conflict.** Both v3.1.0 and v4.0.0 declare `SCHEMA_VERSION = 4` but
  with different `_migrate_3_to_4` implementations. v3.1.0's migration only drops the
  old `ux_raw_pages` index. v4.0.0's migration adds 37 columns and back-fills. If you
  jump v3.0.0 → v4.0.0 directly (as the sample database did), v4's migration runs and
  v3.1's does not. Do not mix the two on the same database file.
- **`spec_checks` runs only in `probe`.** `crawl` and `details` do not invoke it. A
  crawl of live data will not fail on a bad field value even if the spec would reject
  it. Consider calling `spec_checks` from `crawl --url <listing>` in a later version.
- **`--contact` flag from v3.1.0's module docstring is not present in v4.0.0.** The
  docstring mention was premature. The CLI accepts `--user-agent` only.
- **No live test.** v4.0.0 was verified against local fixtures generated from the spec.
  Running `probe --url <live listing URL>` against Aqarmap is the first real test.

## [3.1.0] - 2026-11-21

Operational hardening, parallel to v4.0.0. One lock per database, one miss per UTC day,
an explicit `--assume-period` for pages that imply monthly without labelling it. Reads
the posting date from `<time datetime>` on the detail page. Adds `description` and
`amenities` columns from the payload.

### Added

- **`--assume-period {month,day,week,year}`.** When a priced listing has no period text,
  the crawler can be told to assume one. Rows built this way get `period_assumed = 1`
  and the `assumed_period` flag. `crawl` prints a warning on page 1 when under 50% of
  priced cards yield a `price_period`, naming the flag.
- **`DbLock`.** Exclusive OS-level advisory lock next to the database file (msvcrt on
  Windows, fcntl elsewhere). `crawl`, `details`, `reparse`, `validate` take it. A second
  process gets `AlreadyRunning` and exit code 8.
- **`EXIT_LOCKED = 8`.** Documented in the module docstring.
- **One miss per UTC day.** `finalize_cycle` records at most one miss per source per
  UTC day (`meta:miss_day:<source>`). Back-to-back complete cycles cannot delist
  anything on their own. `miss_counted` and a note say whether a miss was recorded.
- **`RESUMABLE_STOPS`.** Only `reached --max-pages` and `3 consecutive page errors`
  (plus `running` / `interrupted` status) are resumable. Any other finished cycle
  starts a fresh snapshot on the next `crawl`, complete or not.
- **`description`, `amenities` columns.** From the payload only when it carries them
  (`description`, `previewDetails.description`; `amenities`, `previewDetails.amenities`,
  `features`). Description is scrubbed and capped at 30,000 chars. Amenities stored as a
  JSON list of names. Both NULL when the payload has nothing. `payload_survey` reports
  which keys it found (`description_keys_seen`, `amenity_keys_seen`).
- **`url_slug` on every row.** `slug_from_url` runs on `url` when the card is absent.
- **`period_assumed` column.** Records whether the price period was assumed rather than
  read from the page.
- **Soft cap on expected pages.** `expected + max(5, expected // 10)` is the hard stop.
  Exceeding it stops as **not natural** (exit 7).
- **Wrap-by-neighbour detection.** If page N is identical to page N-1, the crawler
  treats it as end of results. Page 1 wrap detection kept.
- **`detail_plan` now takes `--rps`** and reports two ETAs: full pending pass, and the
  `--fields posted_date,furnished,coords,payment` target-fields-only pass.

### Changed

- **`_migrate_3_to_4` drops the old `ux_raw_pages` unique index.** `raw_pages` is now
  keyed by `(run_id, source, page)`. The previous cycle's pages survive a new cycle and
  stay available for `reparse`.
- **`begin_cycle` keeps raw pages of the previous cycle.** It deletes `pages` (the
  current-cycle tracker) but only removes `raw_pages` older than the previous run.
- **`pending_detail_ids` raises a clear `SystemExit` when `--fields` contains an
  unknown key**, listing the valid values.
- **`stats`** uses a parameterised `IN (...)` for the residential-type count.
- **`HttpFetcher.get`** decodes `r.content.decode("utf-8", "replace")` explicitly.
- **`parse_detail` reads `<time datetime>`** inside `<main>` as the posting date. A
  strict ISO date from that attribute overrides any label-matched text.
- **`build_row`** uses `to_str` for stringification and `_d(...)` for dict coercion.
  Every `get` on a possibly-missing nested object is safe.
- **`parse_page` and `build_row`** accept `assume_period`. `parse_page` threads it
  through every row on the page.
- **`finalize_cycle` returns `notes` and `miss_counted`.** `build_run_report` surfaces
  them under `report["notes"]` and `report["miss_counted"]`.

### Fixed

- **Two overlapping `crawl` runs could corrupt cycle bookkeeping.** The per-database
  lock stops this. Second process exits with code 8.
- **`raw_pages` of the previous cycle were deleted on a new cycle.** They are now kept
  for `reparse`.
- **`_crawl` wrap detection.** A page that just repeats the previous page is not fresh
  results. Page 1 wrap check kept and page N-1 check added.
- **`detail_plan`** used the string `act` where it should have used the number of active
  listings. Now takes `rps` and formats with `fmt_eta`.

### Notes

- v3.1.0 declares `SCHEMA_VERSION = 4`, but the migration for `_migrate_3_to_4` is
  limited to dropping the old `ux_raw_pages` index. v4.0.0 declares the same schema
  version but its migration adds 37 columns. **They are not interchangeable.** Pick one
  branch and stay on it.
- The `<script id="listing-details-schema">` RealEstateListing JSON-LD is still not
  parsed. v3.1.0 only extracts `<time datetime>` from the detail page.

## [3.0.0] - 2026-11-19

Parsing moves from HTML cards to the JSON the page embeds. One source of truth per
listing, cards kept as a cross-check. Crawl becomes a snapshot with explicit
completeness rules. New `diff` command. New `details --plan` / `--fields` and a
`reparse` that reads stored raw pages. Exit codes become part of the contract.

### Added

- **Embedded payload extraction.** `rsc_text` joins every `self.__next_f.push([1,
  "..."])` chunk from the React Server Components stream. `extract_payload` decodes the
  `"initialListings":` array into a `Payload` dataclass. Listings are read from the
  payload first; cards are only a fallback.
- **JSON-LD `ItemList`.** `extract_ld` reads `<script type="application/ld+json">` and
  pulls `ItemList.numberOfItems` plus `ItemList.itemListElement[*].url`. Used as a total
  cross-check and as the total when the header text is missing.
- **Disagreements table.** When the payload and the card disagree on `price`, `area`,
  `beds`, `baths`, the row goes to the `disagreements` table with both values.
  `build_row` returns them; `parse_page` collects them.
- **Payload structural validation.** `payload_problems` flags a missing `id` on any
  listing or a `HARD_KEYS` key missing on more than 20% of listings. `schema_canary`
  returns a list of failures for page 1 and `crawl` aborts with exit code 5 (structure
  changed) instead of writing empty rows. The page 1 HTML is saved to
  `samples/canary_failed_page1.html`.
- **`MAX_PAYLOAD_FAIL_STREAK = 5`.** Five pages in a row without a usable payload
  raises `SchemaChanged` (exit 5).
- **Multi-table schema.** New `raw_pages` table (gzip HTML per page plus `run_id`),
  `disagreements` table, `meta` schema version. `price_history` gains `price_amount`,
  `price_period`, `price_monthly`. `listings` gains `slug`, `url_slug`,
  `property_type_raw`, `property_type_title`, `is_call_request`, `beds_raw`, `baths_raw`,
  `finish_type`, `view_code`, `district_id`, `area_id`, `area_parent_id`,
  `location_slug`, `in_compound`, `advertiser_id`, `has_paid_logo`, `featured_type`,
  `photos`, `updated_at`, `payment_method`, `payment_method_label`, `payment_duration`,
  `category`, `has_map`, `raw_json` (zlib of the scrubbed payload), `parse_source`,
  `delisted_run`, `flags`, and the `detail_*` family.
- **Cycle completeness rules.** `MIN_SITE_COVERAGE = 0.97` and `MIN_PREV_RATIO = 0.90`.
  A cycle counts as complete only if it fetched every expected page, saw at least 97% of
  the site's stated total, and at least 90% of the previous complete cycle. Only
  complete cycles count misses. `finalize_cycle` returns the reasons it failed.
- **`diff` command.** New, delisted, and price-changed listings between two `crawl` runs
  (defaults to the last two complete cycles). Reads only the database, no network.
- **`details --plan`.** Prints the exact counts of missing fields per active listing and
  the estimated cost of a full detail pass. No network.
- **`details --fields`.** Limits the pass to listings missing the named fields.
- **`reparse` from `raw_pages`.** Re-runs the parser over stored page HTML, not just the
  card HTML fragments. `reparse` is idempotent on `raw_json` too: it re-derives rows from
  the stored payload.
- **`write_run_report` / `payload_survey`.** Every `crawl` writes `runs/<run_id>.json`
  with field coverage, disagreement stats, engine metrics, and a survey of the payload
  keys (always / sometimes present, observed values, coordinate / date / furnished key
  candidates). This is the file to read when the site changes.
- **Phone scrubbing.** `PHONE_RE` matches masked (`+20...xxxx`) and unmasked numbers.
  `scrub_phones`, `scrub_payload`, `scrub_card_html`. Card HTML stored scrubbed, payload
  stored scrubbed. Contact `href="tel:"/"sms:"/"wa.me"` links replaced with `href="#"`.
- **Property type taxonomy.** `RESIDENTIAL_TYPES`, `NON_RESIDENTIAL_TYPES`,
  `CANONICAL_TYPES`, `_TYPE_ALIASES`. `normalize_type_slug` returns
  `(canonical_or_None, raw_or_None)`. `type_from_title` is a fallback only, and only if
  the title starts with a canonical type word. A slug not in the set is stored as NULL,
  flagged `unknown_property_type`, and preserved in `property_type_raw`.
- **`parse_location`** parses `title_full_path` and `parent` into `district`, `area_name`,
  `district_id`, `area_id`, `area_parent_id`, `location_slug`, `in_compound`.
- **`sort_hints`** lists `sort` / `order` anchors, `<select>` elements, and sort-shaped
  RSC keys. Printed by `probe`.
- **Exit codes.** 0 ok, 1 error, 2 robots disallows, 3 blocked, 4 missing dependency,
  5 structure changed, 6 export locked, 7 crawl ended incomplete, 130 interrupted.
  `_check_dependencies()` raises exit 4 before anything runs.
- **Atomic exports.** `atomic_write` writes to a temp file and `os.replace`s. On
  `PermissionError` it raises `ExportLocked` with a clear message; `cmd_export` returns
  exit 6.
- **Typed Parquet export.** `export_parquet` uses a pyarrow schema with `int64`,
  `float64`, `string`, `bool` types.
- **XLSX `Data dictionary` sheet** with key, header, arrow type, source, meaning, NULL
  meaning for every export column.
- **XLSX `Price changes` sheet** listing every listing whose `price_history` has more
  than one row.
- **`_migrate_2_to_3`.** Automatic database backup (`<db>.bak-v3-<timestamp>`) before any
  ALTER. Renames the v2 fields that moved to `detail_*`. Clears the old `property_type`
  (it came from seller-written titles) and asks for a re-crawl.

### Changed

- **The card is a cross-check.** The payload is the source of truth. `parse_source`
  records which one built the row.
- **`details` no longer runs on every listing by default.** Targets active listings whose
  `detail_status` is NULL, or (with `--fields`) those missing the requested fields.
- **`reparse` reads `raw_pages` first,** then falls back to `raw_json` plus `card_html`
  per listing.
- **`build_row` derives type from `previewDetails.propertyTypeSlug` only.** Title text is
  a fallback and only if the slug is missing.
- **`posted_date`** is read from a fixed list of payload keys (`createdAt`, `created_at`,
  `postedAt`, `publishedAt`, `creationDate`, `created`). It is never filled from
  `updatedAt`.
- **`lat` / `lng`** must be inside `BBOX`; anything outside is NULL. The `shared_coords`
  flag removes lat/lng from the export view when 15 or more listings share a 4-decimal
  point.
- **`details` marks 404/410 as `deactivated`** with `delisted_run` set. A redirect away
  from `/listing/` marks `gone` but leaves `active=1`.
- **`photo_count`, `photos`, `raw_json`** come from the payload. Cards only contribute
  `image_url` and `badges`.
- **`stats`** grows: `residential`, `residential_with_beds`, `residential_with_baths`,
  `card_only_rows`, `raw_pages`, `pages_ok`, `pages_error`.
- **`compute_flags`** adds `call_for_price`, `card_only`, `delisted`,
  `unknown_property_type`, `no_property_type`. Duplicate detection requires an
  `advertiser_id` and separates `possible_dup_compound` from `possible_dup`.
- **`print_quality`** shows `beds(residential)` / `baths(residential)` per-row coverage.
- **CSV is UTF-8 with BOM** (`utf-8-sig`) so Excel reads Arabic titles.

### Fixed

- **`_icon_value` no longer leaks a neighbour's number.** Stops at the enclosing `<li>`
  after at most two ancestor hops.
- **Empty `<data value>` fallback.** If the `<data>` element has no `value` attribute,
  `first_number(text)` runs on its text content.
- **`pick_prices` no longer mixes periods.** `daily_rate` is the explicit per-day price.
- **`parse_detail` drops "similar listings" sections first.** `_drop_similar_sections`
  removes `<section>` / `<aside>` / `<div>` / `<ul>` whose class, id, or aria-label
  matches `similar|related|recommend|you may also|more listings|people also`. Stops a
  suggested listing's `posted_date` or `area_sqm` from landing in the row.

### Notes

- Detail pages still contribute only a few fields (`detail_*`). The detail page's
  `<script id="listing-details-schema">` RealEstateListing JSON-LD is not parsed in v3.
- A cycle is a snapshot. v3 does not add cross-cycle heuristics beyond the delist rule.

## [2.0.0] - 2026-11-16

Crawl becomes a proper snapshot cycle. Result cards are parsed by their real DOM
structure, not by generic keyword search. Detail-page parser hardened. `validate` and
`diff` were added; the SQLite schema is rebuilt around card and detail columns.

### Added

- **Cycle-based crawl.** A cycle is one pass over a source. An unfinished cycle is
  resumed on the next `crawl`. A completed cycle records a full snapshot in `runs`.
  Listings missed twice in a row after a complete cycle are marked `active=0`.
- **`crawl --passes N`** runs N full cycles back to back. `crawl --refresh` discards an
  unfinished cycle. `crawl --delist-after N` controls the miss threshold (default 2).
- **`runs` table** records every crawl and details pass: kind, source, started_at,
  ended_at, status, complete, site_total, new_listings, stopped_reason.
- **`validate` command.** Flags `no_price`, `unknown_price_period`, `daily_price`,
  `weekly_price`, `yearly_price`, `no_area`, `area_mismatch`, `ppsm_high`, `ppsm_low`,
  `dup_xN`, `shared_coords`. Flags are computed as a pure function of a loaded row set.
- **`export --format parquet`.** `export --active-only` skips delisted rows.
  `export --with-text` includes the stored detail-page text column.
- **`probe --deep`.** Fetches a few deep pages and checks whether the last expected page
  returns fresh listings or a wrapped or empty page. Reports a verdict on pagination cap.
- **`inspect_embedded`.** On page 1, searches for `__NEXT_DATA__`, RSC flight markers,
  `ld+json`, and `window.__*` globals. Looks for listing ids inside `<script>` tags.
  Printed by `probe`.
- **`crawl --passes`** loop with a per-pass summary line: stopped reason, new count,
  in-DB count, site-reported total, cycle-complete flag, delisted count.
- **First-page sanity check** on card field coverage (price, area, location, title).
  Warns when coverage is below 80%.
- **`status`** lists recent runs with kind, status, complete, started_at, new_listings,
  stopped_reason.
- **`areas`** builds its list from `district_url` values already in the database, not
  just from page 1 HTML.
- **`DB.begin_cycle`, `DB.finalize_cycle`, `DB.new_run`, `DB.end_run`** for resume and
  snapshot machinery.
- **`RateLimiter`** with `raise_floor` (Crawl-delay), `penalize` (429 / 5xx), and
  `reward` (successful 200).
- **`parse_card` rewritten.**
    - Primary selector `article.listing-card`. `_card_id` reads `aria-labelledby`
      (`listing-<id>-title`) with a fallback to the first `/listing/<id>` anchor.
    - `_icon_value` reads the associated `<span>` at most two ancestors up, staying
      inside the enclosing `<li>`.
    - Prices come from `<data value="...">` first; the text regex is the fallback.
    - `pick_prices` returns `(price_amount, price_period, price_monthly, daily_rate)` and
      never mixes periods in one column.
    - Breadcrumbs split into `district` (first) and `area_name` (last), both URLs
      absolute.
    - `advertiser` from `aside.listing-user-logo a[aria-label]` or `img[alt]`.
    - `badges` from `ul[class*="start-2x"] li`.
    - `photo_count` from `<i class="image-icon">`.
- **`parse_detail` hardened.** Coordinates outside the Alexandria `BBOX` are rejected.
  Images from `og:image` plus `<img data-src>` with logo/avatar/icon/sprite/placeholder
  filtered out, deduped, capped at 40. Detail-page rows and `<dt>/<dd>` pairs feed
  `attrs_json`. The surviving text is sliced to 60 KB in `detail_text`.
- **`DB` schema builder** migrated to `LISTING_COLS` plus `OTHER_TABLES` plus
  `STATIC_SCHEMA`. An ALTER TABLE ADD COLUMN migration path exists for older databases.
- **`DB._first_page_sanity`** warns when price / area / location / title coverage drops
  below 80% on page 1.

### Changed

- **`price_history`** stores `run_id`, and a new row is written only when
  `price_monthly` actually changed.
- **CSV is written with a UTF-8 BOM** (`utf-8-sig`).
- **`export_xlsx`** gains a `Price changes` sheet and a `Summary` sheet with the current
  `DB.stats()` and flag histogram. Column widths, number formats, freeze panes and
  auto-filter updated.
- **`parse_listing_page`** drops `article.listing-card` nodes before searching the body
  text for the "N Properties" total.
- **`area_sqm`, `beds`, `baths`** are merged from `detail_*` into the export view when
  the card has nothing for them.
- **`load_rows`** decompresses `detail_text` (zlib) and re-joins `images` from JSON.
- **`load_sources`** reads `--url-file` (`#` comments allowed) in addition to repeated
  `--url`.
- **`--proxy` and `--fetcher playwright`** wired through to the fetcher.

### Fixed

- **`parse_card_fragment`** no longer assumes the fragment starts with an `<article>`.
- **`parse_listing_page` fallback** climbs from each `/listing/<id>` anchor to the card
  root, so a renamed card class does not break the parse entirely.
- **`parse_detail`** no longer keeps coordinates from a generic header or company
  address.

### Notes

- The scraper is still HTML-only on result pages. `inspect_embedded` runs during `probe`
  to look for a JSON payload in `__NEXT_DATA__` / RSC / `ld+json`, but the parser does
  not consume one yet.
- Parquet is optional (`pip install pyarrow`).

## [1.0.0] - 2026-11-15

First working version. Result pages only, HTML parsing. Detail pages contribute a few
extra fields via DOM label matching.

### Added

- **`crawl`**: walk paginated result pages, one row per listing.
- **`probe`**: fetch page 1 and one detail page, save HTML under `samples/`, print field
  coverage.
- **`details`**: fetch each listing page for extra fields. DOM label matching only.
- **`reparse`**: rebuild parsed columns from stored HTML (no network).
- **`export`**: CSV, XLSX, JSONL.
- **`status`**: progress and data-quality counts.
- **`areas`**: list sub-area URLs from page 1, for sharding when pagination caps.
- **SQLite with WAL journal mode.** Every page commits; Ctrl-C loses nothing.
- **`robots.txt` fetched and enforced** (Allow/Disallow with wildcards, Crawl-delay). No
  override flag.
- **Global rate limiter** with jitter, adaptive backoff, and a stop on repeated blocks.
- **HTML cards parsed with `selectolax`** (lexbor). Title, monthly and daily price,
  area, beds, baths, location, thumbnail, property type.
- **Beds and baths found by keyword** in element attributes, not by fixed class names.
- **Arabic-Indic digits normalised.** EGP / ج.م and m² / م² recognised.
- **Detail page fields parsed by label match** from tables, definition lists and text
  lines. Coordinates from JSON-LD `latitude` / `longitude`.
- **Card HTML and detail text stored zlib-compressed**, so `reparse` needs no network.
- **Phone numbers never stored.**
- **`httpx` fetcher with HTTP/2.** Optional `Playwright` fetcher for JS-rendered pages
  (not exercised).
- **CSV export is UTF-8 with BOM.** XLSX has a Summary sheet and pre-set column formats.