"""Shared fixtures. The HTML files in tests/fixtures/ are synthetic, see scripts/gen_fixtures.py."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import aqarmap_scraper as scraper  # noqa: E402

DETAIL_URL = (
    "https://aqarmap.com.eg/en/listing/7416775-"
    "for-rent-alexandria-el-mandara-alex-el-mandara-bahri-sidi-kamal-st/"
)
RESULTS_URL = "https://aqarmap.com.eg/en/for-rent/property-type/alexandria/el-mandara/"


@pytest.fixture(scope="session")
def mod():
    return scraper


@pytest.fixture(scope="session")
def detail_html() -> str:
    return (FIXTURES / "detail_7416775.html").read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def results_html() -> str:
    return (FIXTURES / "results_page_1.html").read_text(encoding="utf-8")


@pytest.fixture(scope="session")
def detail_url() -> str:
    return DETAIL_URL


@pytest.fixture(scope="session")
def results_url() -> str:
    return RESULTS_URL


@pytest.fixture(scope="session")
def detail_parse(mod, detail_html, detail_url):
    """PageParse of the detail fixture, parsed once."""
    return mod.parse_detail_record(detail_html, detail_url)


@pytest.fixture(scope="session")
def row(detail_parse) -> dict:
    """The single listing row built from the detail fixture."""
    assert len(detail_parse.rows) == 1
    return detail_parse.rows[0]
