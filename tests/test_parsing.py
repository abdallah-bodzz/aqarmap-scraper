"""Extra tests beyond the spec checklist: results pages, failure paths, DB merge."""
import json

import pytest


def test_results_page_uses_initial_listings_path(mod, results_html, results_url):
    pp = mod.parse_page(results_html, results_url)
    assert pp.kind == "list"
    assert pp.parse_source == "payload"
    assert pp.id_match is True
    assert [r["id"] for r in pp.rows] == [7416775, 7298779]
    assert mod.schema_canary(pp) == []


def test_results_page_fills_new_columns(mod, results_html, results_url):
    first = mod.parse_page(results_html, results_url).rows[0]
    assert first["daily_rate"] == 800
    assert first["price_monthly"] == 20000
    assert json.loads(first["amenities_codes"]) == ["SECURITY", "ELEVATOR"]
    assert first["lat"] == pytest.approx(31.2765)
    assert first["translations_ar_title"].startswith("شقة")


def test_detail_canary_is_clean_on_fixture(mod, detail_parse):
    assert mod.detail_canary(detail_parse) == []
    assert all(ok for _, ok in mod.spec_checks(detail_parse.rows[0]))


def test_detail_page_without_listing_object_raises(mod, detail_html, detail_url):
    broken = detail_html.replace('\\"listing\\":', '\\"lst\\":')
    assert broken != detail_html
    with pytest.raises(mod.PayloadError):
        mod.parse_detail_record(broken, detail_url)


def test_results_payload_is_rejected_by_detail_parser(mod, results_html, results_url):
    with pytest.raises(mod.PayloadError):
        mod.parse_detail_record(results_html, results_url)


def test_phone_keys_are_scrubbed_but_has_whatsapp_is_kept(mod, row):
    raw = mod.unzjson(row["raw_json"])
    assert "has_whatsapp" in raw
    assert "phones" not in raw and "whatsAppNumber" not in raw
    assert row["has_whatsapp"] == 1


def test_detail_enriches_existing_row_without_duplicating(mod, tmp_path, results_html, results_url,
                                                          detail_parse):
    db = mod.DB(str(tmp_path / "t.db"))
    try:
        list_pp = mod.parse_page(results_html, results_url)
        run_id = db.new_run("crawl", results_url)
        db.upsert_rows(results_url, 1, list_pp.rows, list_pp.disagreements, run_id)
        assert db.count_listings() == 2
        assert db.enrich_from_detail(detail_parse.rows[0], detail_parse, None) is True
        assert db.count_listings() == 2
        got = db.conn.execute(
            "SELECT payload_kind, date_posted_iso, seller_score FROM listings WHERE id=7416775").fetchone()
        assert got["payload_kind"] == "detail"
        assert got["date_posted_iso"] == "2026-11-17T19:41:27Z"
        assert got["seller_score"] == 4
    finally:
        db.close()


def test_migration_v3_to_v4_adds_columns(mod, tmp_path):
    path = str(tmp_path / "old.db")
    db = mod.DB(path)
    db.conn.execute("UPDATE meta SET value='3' WHERE key='schema_version'")
    db.close()
    migrated = mod.DB(path)
    try:
        cols = migrated._columns("listings")
        assert set(mod.NEW_COL_NAMES) <= cols
        assert migrated.meta_get("schema_version") == "4"
    finally:
        migrated.close()
    assert list(tmp_path.glob("old.db.bak-v3-*"))
