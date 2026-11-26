# Runbook

Every command is `python aqarmap_scraper.py <command> [flags]`. Examples below use `$DB` for the database path.

```bash
DB=data/aqarmap.db
DETAIL=https://aqarmap.com.eg/en/listing/7416775-for-rent-alexandria-el-mandara-alex-el-mandara-bahri-sidi-kamal-st/
RESULTS=https://aqarmap.com.eg/en/for-rent/property-type/alexandria/
```

## Contents

- [Exit codes](#exit-codes)
- [Flags shared by every command](#flags-shared-by-every-command)
- [probe](#probe)
- [crawl](#crawl)
- [details](#details)
- [reparse](#reparse)
- [validate](#validate)
- [export](#export)
- [diff](#diff)
- [status](#status)
- [areas](#areas)
- [What overwrites or deletes data](#what-overwrites-or-deletes-data)
- [Migrating a v3 database to v4](#migrating-a-v3-database-to-v4)
- [Settings for the first live run](#settings-for-the-first-live-run)

## Exit codes

| Code | Meaning |
|---|---|
| 0 | OK |
| 1 | Error |
| 2 | robots.txt forbids the URLs |
| 3 | Blocked by the site |
| 4 | Missing dependency |
| 5 | Page structure changed (schema canary failed) |
| 6 | Export file locked or not writable |
| 7 | Crawl ended incomplete |
| 130 | Interrupted |

## Flags shared by every command

No flag is required. All have defaults.

| Flag | Default | Notes |
|---|---|---|
| `--db` | `aqarmap_alexandria_rent.db` | SQLite file. Use `data/aqarmap.db` so it stays out of git. |
| `--url` | Alexandria for-rent results URL | Repeatable. A results URL or a single listing URL. |
| `--url-file` | none | One URL per line. Runs from a file never delist. |
| `--param KEY=VALUE` | none | Extra query parameter on every results URL. |
| `--rps` | 1.0 | Requests per second, global. |
| `--concurrency` | 3 | Parallel workers, used by `details`. |
| `--timeout` | 30.0 | Seconds. |
| `--max-retries` | 5 | |
| `--backoff-base` | 2.0 | Seconds, doubles per retry. |
| `--block-limit` | 4 | Stop after this many blocked responses in a row. |
| `--proxy` | none | Plain HTTP(S) proxy. No rotation. |
| `--fetcher` | `httpx` | `playwright` is also accepted. |
| `--samples-dir` | `samples` | Saved HTML goes here. |
| `--runs-dir` | `runs` | Run reports, `runs/<id>.json`. |
| `--log-file` | `aqarmap_scraper.log` | |
| `-v` | off | Verbose logging. |

robots.txt is fetched and enforced. There is no override flag.

## probe

Fetches a page, runs the schema canary and prints what it parsed. Nothing is written to the database.

Detail page:

```bash
python aqarmap_scraper.py probe --db $DB --url $DETAIL
```

- Required: none. `--url` is the listing URL.
- Exit code: 0.
- Success: `records: 1   parse_source: payload`, `SCHEMA CANARY: OK`, every spec check shows `PASS`. HTML saved to `samples/detail.html`.
- On failure: exit 5 and `samples/detail.html` holds the page. Open it and check that `"listing":{` and `#listing-details-schema` are still there. Exit 3 means the site blocked the request. Wait, then retry with a lower `--rps`.

Results page:

```bash
python aqarmap_scraper.py probe --db $DB --url $RESULTS
```

- Optional: `--no-detail` skips the extra detail fetch. `--listing-url` picks the detail page to test. `--deep` fetches deep pages to test for a pagination cap.
- Exit code: 0.
- Success: `SCHEMA CANARY: OK`, `payload ids == card ids: True`. HTML saved to `samples/page1.html`.
- On failure: exit 5 and the problems are printed under `SCHEMA CANARY: FAILED`. The page is in `samples/`.

## crawl

Walks result pages and writes one row per listing. Also accepts a single listing URL.

Results URL:

```bash
python aqarmap_scraper.py crawl --db $DB --url $RESULTS --rps 0.5 --max-pages 3 --passes 1
```

- Optional: `--max-pages` (default 1000), `--passes` (default 2), `--refresh`, `--delist-after` (default 2), `--save-raw` / `--no-save-raw` (default on).
- Exit code: 0 when the cycle completes, and also when it stops at `--max-pages`. 7 when it ends incomplete for any other reason, for example an HTTP error mid-crawl.
- Success: `source done: ... cycle complete: yes` and a `flags:` line. With `--max-pages` you will see `stopped: reached --max-pages` and `cycle complete: NO`, which is correct for a smoke test.
- On failure: exit 5 means the page structure changed and nothing was written. Exit 3 means blocked. Progress is saved. Re-run and it resumes.

Single listing URL, new listing:

```bash
python aqarmap_scraper.py crawl --db $DB --url $DETAIL
```

- Exit code: 0.
- Success: `listing-detail run #N` then `stopped: single listing page | new: 1`.
- On failure: exit 5 if the page has no decodable listing object. The page is saved to `samples/canary_failed_detail.html`. Nothing is written.

Single listing URL, listing already in the database:

```bash
python aqarmap_scraper.py crawl --db $DB --url $DETAIL
```

- Exit code: 0.
- Success: `new: 0`. The row count does not change. The row now has `payload_kind = detail`.
- A 404 or 410 for the listing marks that one listing inactive.

## details

Opens listing pages for rows already in the database and fills the v4 columns plus the `detail_*` fallbacks. It uses the host of `--url` (or the default URL), so pass the same `--url` you crawled with.

Show the numbers, no network:

```bash
python aqarmap_scraper.py details --db $DB --plan
```

- Exit code: 0.
- Success: JSON with `missing_*` counts, for example `missing_amenities` and `missing_payment_plans`.

Smoke test:

```bash
python aqarmap_scraper.py details --db $DB --url $RESULTS --limit 5 --rps 0.5
```

- Optional: `--limit N`, `--fields`, `--retry-errors`, `--redo`.
- Exit code: 0.
- Success: `details finished: {... 'enriched': N ...}`.
- On failure: exit 5 if several detail pages in a row have no decodable listing object. Exit 3 if blocked. Re-run to resume.

Fill only missing groups:

```bash
python aqarmap_scraper.py details --db $DB --url $RESULTS --limit 10 --fields jsonld,amenities,plans,translations
```

- `--fields` accepts `posted_date`, `furnished`, `coords`, `payment`, `jsonld`, `amenities`, `plans`, `translations`. It selects rows that are still missing at least one of them.

Back-fill after an upgrade:

```bash
python aqarmap_scraper.py details --db $DB --url $RESULTS --redo --fields jsonld
```

- `--redo` is needed because rows fetched by an older version are already marked as done.
- Success: `enriched` equals the number of listings processed.

## reparse

Rebuilds every parsed column from stored data. No network.

```bash
python aqarmap_scraper.py reparse --db $DB
```

- Optional: `--what all|pages|listings|payload|cards|details` (default `all`).
- Exit code: 0.
- Success: `re-parsed N listing(s) ...` and a `flags:` line. Run it twice. The second run changes nothing.
- `pages` reads `raw_pages`, which `crawl` fills when `--save-raw` is on. `listings`, `payload` and `cards` read `raw_json`, `card_html` and `raw_aux_json` from each row. `details` re-maps stored detail text.
- On failure: `stored page ... no longer parses` is a warning for that page only. Check the parser change that caused it.

## validate

Data-quality flags and coverage by property type.

```bash
python aqarmap_scraper.py validate --db $DB --out data/flagged.csv
```

- Optional: `--out` writes flagged listings to a CSV.
- Exit code: 0.
- Success: `N listings, M with at least one flag`, then a count per flag and a coverage table.
- New in v4: `no_amenities`, `no_translations_ar`, `no_seller_score`, `no_payment_plans`, `no_date_posted`, `no_reference_number`. Rows from results pages will carry several of these until `details` has run.

## export

Writes files. Atomic: a temp file is written, then renamed.

```bash
python aqarmap_scraper.py export --db $DB --format xlsx --out data/listings
```

- Optional: `--format csv|xlsx|jsonl|parquet|all` (default `all`), `--out` (path without extension, default `aqarmap_alexandria_rent`), `--with-text`, `--active-only`.
- Exit code: 0.
- Success: `wrote data/listings.xlsx (N rows)`. Sheets: `Listings`, `Summary`, `Data dictionary`.
- `xlsx` needs `openpyxl`. `parquet` needs `pyarrow`.
- On failure: exit 6 means the file is open in Excel or the folder is not writable. Close the file and re-run.

## diff

New, delisted and price-changed listings between two crawl runs.

```bash
python aqarmap_scraper.py diff --db $DB --from 1 --to 2 --out data/diff.csv
```

- Optional: `--from`, `--to` (default: the last two complete crawls), `--out`.
- Exit code: 0 on success. 1 if fewer than two complete crawl runs exist and you did not pass `--from` and `--to`.
- Success: counts of new, delisted and price-changed listings.
- Run ids come from `status`. A single-listing crawl counts as a run, so pass explicit ids if you mix them with results crawls.

## status

Progress, runs and data quality.

```bash
python aqarmap_scraper.py status --db $DB
```

- Exit code: 0.
- Success: a JSON block of counts (`listings`, `active`, `detail_ok`, `with_amenities` and others), then one line per source and a `recent runs:` list with run ids.
- Any v3 database is migrated the first time a v4 command opens it. `status` triggers that too.

## areas

Lists district URLs for sharding. Fallback only. Pagination is not capped on the live site, and sharded runs never delist.

```bash
python aqarmap_scraper.py areas --db $DB
```

- Exit code: 0 if it found URLs. 1 if it found none.
- Success: one URL per line, then `N district URL(s)`.
- It uses stored district URLs if the database has any. Otherwise it fetches page 1.

## What overwrites or deletes data

- Every `crawl` overwrites the parsed columns of the listings it sees.
- A detail-enriched row keeps its detail values when a later results crawl sees it.
- Starting a new crawl cycle deletes the `pages` and `raw_pages` rows for that source. Listings are kept.
- `reparse` overwrites parsed columns from stored data. It never touches crawl-state columns.
- `details --redo` fetches again and rewrites the `detail_*` columns.
- `export` replaces the output file.
- No command deletes listing rows.

## Migrating a v3 database to v4

There is no migration command. The first v4 command that opens the file does it.

```bash
python aqarmap_scraper.py status --db data/old.db
```

- Backup: `data/old.db.bak-v3-YYYYMMDD-HHMMSS`, a copy made before anything changes.
- Log lines to expect:
  1. `database backed up to ... before migrating v3 -> v4`
  2. `migrated: added listings.<column>` for each new column
  3. `migrated v3 -> v4: 37 new columns, back-filled N listing(s) from stored payloads`
  4. A warning to run `reparse`
- The migration adds columns and fills only the new ones from stored `raw_json`. Existing columns are not touched.
- Follow-up. Run `reparse` once. It recomputes `lat`, `lng`, `daily_rate` and `price_monthly` on old rows, using `center_lat`, `center_lng` and the payment plans.

```bash
python aqarmap_scraper.py reparse --db data/old.db
python aqarmap_scraper.py validate --db data/old.db
```

- A database from a newer version stops with `database schema vN is newer than this script`.

## Settings for the first live run

```bash
python aqarmap_scraper.py crawl --db $DB --url $RESULTS --rps 0.5 --concurrency 1 --timeout 30 --max-pages 3 --passes 1
```

- `--rps 0.5`, `--concurrency 1`, `--timeout 30`, `--max-pages 3`, `--passes 1`.
- I have not run any command against the live site yet. Start with `probe`.
