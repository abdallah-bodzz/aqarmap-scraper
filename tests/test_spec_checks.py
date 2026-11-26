"""The validation checklist from docs/SPEC.md section 8, one test per assertion, run on the synthetic fixture.

Differences from the spec's snippet, all because of how the script stores data:
  * `result["phone_raw"]` / `phone_masked` do not exist. The script never stores phone numbers.
    `test_phone_numbers_are_not_stored` checks that instead.
  * The spec's dedupe check lists all parsed listing objects, similar listings included. The script
    keeps only the page's own listing, so `test_dedupe_keeps_one_full_record` checks that one record
    comes back, it is the fullest copy, and the similar listings are not in it.
"""
import json
from datetime import datetime


def test_listing_id_is_7416775_int(row):
    assert row["id"] == 7416775
    assert isinstance(row["id"], int)


def test_price_is_20000_positive_int(row):
    assert row["price_amount"] == 20000
    assert row["price_amount"] > 0


def test_currency_is_egp(row):
    assert row["currency"] == "EGP"


def test_area_rooms_baths(row):
    assert row["area_sqm"] == 140
    assert row["beds"] == 3
    assert row["baths"] == 2


def test_date_posted_is_timezone_aware(row):
    assert row["date_posted_iso"] == "2026-11-17T19:41:27Z"
    parsed = datetime.fromisoformat(row["date_posted_iso"].replace("Z", "+00:00"))
    assert parsed.tzinfo is not None


def test_url_starts_with_listing_prefix(row):
    assert row["url"].startswith("https://aqarmap.com.eg/en/listing/7416775-")


def test_coordinates_in_range(row):
    assert -90 <= row["lat"] <= 90
    assert -180 <= row["lng"] <= 180
    assert abs(row["lat"] - 31.2765) < 1e-3
    assert abs(row["lng"] - 30.0065) < 1e-3


def test_nine_images_from_jsonld(row):
    images = json.loads(row["images_jsonld"])
    assert len(images) == 9
    assert all(u.startswith("https://img-") for u in images)


def test_type_section_and_category(row):
    assert row["property_type_raw"] == "furnished-apartment"
    assert row["section_slug"] == "for-rent"
    assert row["category_ld"] == "ForRent"


def test_both_payment_plans_present(row):
    plans = json.loads(row["payment_plans_json"])
    assert any(p["duration"] == 30 and p["price"] == 20000 for p in plans)
    assert any(p["duration"] == 1 and p["price"] == 800 for p in plans)
    assert row["price_monthly"] == 20000
    assert row["daily_rate"] == 800


def test_attributes_are_indexed_by_name_not_label(mod, detail_parse, row):
    attrs = mod.norm_attrs(detail_parse.payload.listings[0])
    assert attrs["rooms"] == "3"
    assert attrs["baths"] == "2"
    assert attrs["floor"] == "12"
    assert row["floor"] == "12"
    assert row["finish_type"] == "SUPER_LUX"


def test_seller_id_and_name(row):
    assert row["advertiser_id"] == 1819928
    assert row["advertiser_name"] == "Ahmed Zain"


def test_phone_numbers_are_not_stored(mod, row):
    stored = json.dumps(mod.unzjson(row["raw_json"]), ensure_ascii=False)
    assert "xxxx" not in stored
    assert '"phones"' not in stored
    for key, value in row.items():
        if isinstance(value, str):
            assert "+20115399" not in value, key


def test_dedupe_keeps_one_full_record(mod, detail_html, detail_parse):
    found = mod.decode_listing_objects(mod.rsc_text(detail_html))
    assert set(found) == {7416775}
    assert len(found[7416775]) == 4          # stripped copy, header, sticky header, mobile footer
    listings = detail_parse.payload.listings
    assert [x["id"] for x in listings] == [7416775]
    assert "translations" in listings[0]     # the full copy won, not the stripped one
