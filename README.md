# aqarmap-scraper

A resumable scraper for Aqarmap listing pages. It stores everything in one SQLite file and exports to CSV, XLSX, JSONL and Parquet.

Version 4.0.0. One Python file, `aqarmap_scraper.py`. Author: Abdallah A Khames.

## Contents

- [Why this exists](#why-this-exists)
- [Status](#status)
- [Known limits](#known-limits)
- [Install](#install)
- [Quick start](#quick-start)
- [Commands](#commands)
- [What gets stored](#what-gets-stored)
- [Tests](#tests)
- [Repository layout](#repository-layout)
- [Docs](#docs)
- [License](#license)

## Why this exists

I am building part of **Stay_Hub** (**Sahelly**), an AI-powered vacation-rental marketplace for Egypt's coastal properties, North Coast and Red Sea. It is a team graduation project at the Faculty of Computers and Data Science, Alexandria University, 2026/2027.

My assigned feature is **Feature 1: Dynamic Pricing Suggestion**. It is a classical ML model. It needs a training set of property features and historical nightly prices per area and season. I scrape Aqarmap to build that set.

This repo is only the data-collection part of my feature. It is one component of the larger team repo. The model code is not here.

What the scraper gives me per listing:

- Property features: type, area in square metres, rooms, baths, floor, finish, amenities, location, coordinates.
- Prices: the headline price, the monthly price and the daily rate when the listing has a one-day payment plan.
- Price history: every crawl records a price change in the `price_history` table.

The scraper only sees prices as they are today. Seasonal history comes from running `crawl` repeatedly over time and using `diff` and `price_history`. A single crawl has no seasons in it.

## Status

- v4 is a single file. I did not split it into modules.
- 22 tests pass. They run on synthetic HTML fixtures that I generated from the handoff spec.
- I have not run v4 against live Aqarmap yet. The first live run is the real test. Start with `probe`.
- v4 adds detail pages as a first-class source. See [CHANGELOG.md](CHANGELOG.md).

## Known limits

- **Coordinates are limited to Alexandria.** `BBOX` in `aqarmap_scraper.py` is a box around the Alexandria governorate. Listings with coordinates outside it get `lat` and `lng` set to NULL. North Coast and Red Sea listings will fall outside it. I have not changed this yet. It needs a decision before I crawl those areas.
- The default `--url` is the Alexandria for-rent search. Pass `--url` for any other area.
- Phone numbers are never stored. The site masks them anyway.
- Numeric codes such as `status` and `seller_type` are stored as numbers. The site does not publish what they mean.
- The fixtures are synthetic. They match the spec, not a captured page.

## Install

Python 3.10 or newer.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`requirements.txt` includes `openpyxl` and `pyarrow`, which are only needed for XLSX and Parquet export. For the tests and linting:

```bash
pip install -r requirements-dev.txt
```

Or install from `pyproject.toml`:

```bash
pip install -e ".[export,dev]"
```

## Quick start

Check one detail page first. Nothing is written to the database.

```bash
python aqarmap_scraper.py probe --url "https://aqarmap.com.eg/en/listing/7416775-for-rent-alexandria-el-mandara-alex-el-mandara-bahri-sidi-kamal-st/"
```

Then a small crawl, the offline rebuild, a few detail pages, a check and an export:

```bash
python aqarmap_scraper.py crawl    --db data/aqarmap.db --rps 0.5 --max-pages 3 --passes 1
python aqarmap_scraper.py reparse  --db data/aqarmap.db
python aqarmap_scraper.py details  --db data/aqarmap.db --limit 5 --rps 0.5
python aqarmap_scraper.py validate --db data/aqarmap.db
python aqarmap_scraper.py export   --db data/aqarmap.db --format xlsx --out data/listings
```

`crawl --max-pages 3` stops after three pages and exits 0. The log says `cycle complete: NO`, which is correct for a smoke test. Exit code 7 means the crawl ended incomplete for another reason, such as an HTTP error.

`scripts/run_full_pipeline.ps1` runs the same sequence. `-DryRun` prints the commands without running them.

```powershell
.\scripts\run_full_pipeline.ps1 -DryRun
```

robots.txt is fetched and enforced. There is no flag to turn that off. The default rate is 1 request per second.

## Commands

All commands are `python aqarmap_scraper.py <command>`. Every flag is optional.

| Command | What it does | Network |
|---|---|---|
| `probe` | Fetches one page, runs the schema canary, prints what it parsed. Given a listing URL it probes that detail page and runs the spec checks. | yes |
| `crawl` | Walks result pages and writes one row per listing. Given a listing URL it fetches that one page and inserts or enriches the row. | yes |
| `details` | Opens listing pages for rows already in the database and fills the detail columns. `--plan` shows the numbers. | yes |
| `reparse` | Rebuilds every parsed column from stored raw data. | no |
| `validate` | Data-quality flags and coverage by property type. | no |
| `export` | Writes CSV, XLSX, JSONL or Parquet with a data dictionary. | no |
| `diff` | New, delisted and price-changed listings between two crawl runs. | no |
| `status` | Progress, runs and data-quality counts. | no |
| `areas` | Lists district URLs for sharding. Fallback only. | maybe |

Exit codes: 0 ok, 1 error, 2 robots.txt forbids, 3 blocked, 4 missing dependency, 5 page structure changed, 6 export file locked, 7 crawl incomplete, 130 interrupted.

Details for each command are in [docs/RUNBOOK.md](docs/RUNBOOK.md).

## What gets stored

- One row per listing in the `listings` table, keyed by the Aqarmap listing id.
- Results pages are read from the JSON the page embeds. The HTML cards are parsed too and compared against it.
- Detail pages are read from the embedded listing object and two JSON-LD blocks. Where the JSON-LD has a field, it wins over the payload.
- Raw payloads are stored compressed, so `reparse` can rebuild every column with no network.
- Databases from v3 migrate on first open. A backup is made first. See the [runbook](docs/RUNBOOK.md#migrating-a-v3-database-to-v4).

The field mapping is in [docs/FIELDS.md](docs/FIELDS.md).

## Tests

```bash
pytest
```

The tests use the HTML files in `tests/fixtures/` and never touch the network. `tests/test_spec_checks.py` holds the checklist from section 8 of the spec. `tests/test_parsing.py` covers results pages, failure paths, merging a detail page into an existing row and the v3 to v4 migration.

To regenerate the fixtures:

```bash
python scripts/gen_fixtures.py
```

## Repository layout

```text
aqarmap_scraper.py          the scraper, v4, single file
tests/                      pytest tests and synthetic HTML fixtures
docs/SPEC.md                the handoff spec for the detail page
docs/FIELDS.md              spec field to database column mapping
docs/RUNBOOK.md             every command, flags, exit codes, migration
scripts/gen_fixtures.py     builds the test fixtures
scripts/run_full_pipeline.ps1   first-time pipeline for Windows
data/                       databases and exports (ignored by git)
samples/                    saved HTML from probe (ignored by git)
legacy/                     old versions (ignored by git)
```

## Docs

- [docs/SPEC.md](docs/SPEC.md): what a detail page contains and where.
- [docs/FIELDS.md](docs/FIELDS.md): which database column each spec field lands in.
- [docs/RUNBOOK.md](docs/RUNBOOK.md): how to run each command and what to do when it fails.
- [CHANGELOG.md](CHANGELOG.md): what changed in v4.

## License

All rights reserved. See [LICENSE](LICENSE).
