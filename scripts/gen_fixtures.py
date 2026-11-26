"""Generate the HTML fixtures used by the tests.

Writes two files, by default into tests/fixtures/:

    detail_7416775.html   One detail page for listing 7416775, built from the handoff spec (docs/SPEC.md).
    results_page_1.html   One results page with two cards and the matching initialListings payload.

Both files are synthetic. They are not captured from the live site. They follow the structure the spec
describes: the listing object repeated three times in the RSC stream (one stripped copy, two full copies),
two similar listings that are not the target, the RealEstateListing and BreadcrumbList JSON-LD blocks,
a masked phone number, and a stream chunk boundary that falls inside a JSON object.

The results page header says "12 Properties" because the scraper's total-count regex needs at least two
digits. The page itself holds two cards.

Usage:
    python scripts/gen_fixtures.py [output_dir]
"""
import json
import sys
from pathlib import Path

LID = 7416775
SLUG = "for-rent-alexandria-el-mandara-alex-el-mandara-bahri-sidi-kamal-st"
IMG = "https://img-1.aqarmap.com.eg/slider-photo-watermarked-logo-large/2309/64f42fb007a19559357053"
MASK = "+20115399xxxx"
AMENITIES_ALLOWED = [
    "SECURITY", "ELEVATOR", "PARKING", "GARDEN", "POOL", "GYM", "BALCONY", "MAIDS_ROOM", "CENTRAL_AC",
    "INTERNET", "LANDLINE", "PETS", "CLUBHOUSE", "KITCHEN_APPLIANCES", "NATURAL_GAS",
]


def detail_page() -> str:
    user = {
        "id": 1819928, "full_name": "Ahmed Zain", "user_type": 1, "activeListingsCount": 60, "score": 4,
        "logo": {"thumbnails": {"large": "https://img-1.aqarmap.com.eg/logo/2309/logo-large.png"}},
        "personal_photo": {"thumbnails": {"large": "https://img-1.aqarmap.com.eg/user/photo-large.png"}},
        "phone_number": MASK, "whatsApp_number": MASK, "has_active_logo_service": True,
        "created_at": "2020-01-01T00:00:00+02:00",
    }
    title = "For rent - Apartment 140 M² Super lux"
    listing = {
        "id": LID, "slug": SLUG, "title": title, "price": 20000, "area": 140,
        "address": "Sidi Kamal St., El Mandara Bahri",
        "description": "Apartments For rent in Sidi Kamal St. with size 140 M² View Main Street",
        "center_lat": 31.2765, "center_lng": 30.0065, "status": 2, "status_label": "label.status.live",
        "payment_method": 1, "paymentMethodLabel": "label.payment_method.from_owner_cash",
        "reference_number": "123", "is_featured": False, "was_featured": True, "is_hold": False,
        "isCallRequest": False, "accepts_messages": True, "has_whatsapp": True,
        "phones": [{"id": None, "number": MASK, "countryCode": "+20"}], "whatsAppNumber": MASK,
        "listing_phones": [{"number": MASK}], "user": user, "category": 5, "seller_role": 1,
        "attributes": [
            {"custom_field": {"name": "rooms", "label": "الغرف"}, "value": "3"},
            {"custom_field": {"name": "baths", "label": "الحمامات"}, "value": "2"},
            {"custom_field": {"name": "floor", "label": "الدور"}, "value": "12"},
            {"custom_field": {"name": "finish-type", "label": "التشطيب"}, "value": "SUPER_LUX"},
        ],
        "amenities": ["SECURITY", "ELEVATOR"],
        "payment_plans": [
            {"total_down_payment": 0, "total_remaining_amount": 0, "duration": 30, "price": 20000,
             "security_deposit": 0, "over_price": 0, "down_payment": 0, "down_payment_percentage": 0},
            {"duration": 1, "price": 800, "security_deposit": 0, "over_price": 0},
        ],
        "price_per_meter": 142.857142857, "video_url": None, "brochure": None, "meta_title": None,
        "meta_description": None, "property_view": 2, "property_view_label": "label.property_view.main_street",
        "translations": [
            {"locale": "en", "title": title, "description": "Apartments For rent in Sidi Kamal St.",
             "address": "Sidi Kamal St."},
            {"locale": "ar", "title": "شقة مفروشة للايجار اليومي بالمندرة بحري",
             "description": ("ريسبشن قطعتين \n3غرف \n2حمام \nمطبخ \nاجهزة كهربائية كامله \n"
                             "مكيف \nانترنت \nامن للعقار \n"),
             "address": "شارع سيدي كمال، المندرة بحري"},
        ],
        "updated_at": "2026-11-17T21:41:27+02:00",
        "property_type": {"id": 9, "slug": "furnished-apartment", "title": "Furnished Apartment",
                          "amenities": AMENITIES_ALLOWED},
        "section": {"id": 2, "slug": "for-rent", "title": "For rent"},
        "location": {"id": 3011, "slug": "sidi-kamal-st",
                     "title_full_path": "Alexandria / El Mandara / El Mandara Bahri", "parent": {"id": 3000}},
        "previewDetails": {"largePhotos": [{"src": f"{IMG}-{i}.png"} for i in range(6)]},
        "photos": [{"file": {"thumbnails": {"small": f"{IMG}-{i}-s.png"}}} for i in range(9)],
        "score": None,
    }
    stripped = {"id": LID, "slug": SLUG, "title": title, "price": 20000, "area": 140}
    similar = [
        {"id": 7298779, "slug": "for-rent-other-1", "title": "Other 1", "price": 9000, "area": 90, "phones": []},
        {"id": 7287553, "slug": "for-rent-other-2", "title": "Other 2", "price": 7000, "area": 70, "phones": []},
    ]
    rows = [
        "1f:" + json.dumps(["$", "div", None, {"children": ["$", "header", None, {"props": {"listing": stripped}}]}]),
        "4c:" + json.dumps(["$", "div", None, {"header": {"props": {"listing": listing}}}], ensure_ascii=False),
        "52:" + json.dumps(["$", "div", None, {"stickyHeader": {"listing": listing}}], ensure_ascii=False),
        "56:" + json.dumps(["$", "div", None, {"mobileFooter": {"listing": dict(listing)}}], ensure_ascii=False),
        "53:" + json.dumps(["$", "div", None, {"props": {"listings": similar}}]),
        "60:" + json.dumps({"sections": [{"id": 1, "title": "For sale", "slug": "for-sale"},
                                         {"id": 2, "title": "For rent", "slug": "for-rent"}]}),
    ]
    stream = "\n".join(rows) + "\n"
    cut = len(stream) // 2 + 7   # the split falls inside a JSON object
    push = "".join(
        f"<script>self.__next_f.push([1,{json.dumps(c)}])</script>" for c in (stream[:cut], stream[cut:])
    )
    ld_listing = {
        "@type": "RealEstateListing", "url": f"https://aqarmap.com.eg/en/listing/{LID}-{SLUG}/",
        "name": title, "datePosted": "2026-11-17T19:41:27Z", "category": "ForRent",
        "offers": {"@type": "Offer", "price": 20000, "priceCurrency": "EGP",
                   "availability": "https://schema.org/InStock"},
        "itemOffered": {
            "@type": "Apartment", "@id": f"https://aqarmap.com.eg/en/listing/{LID}", "numberOfRooms": 3,
            "numberOfBathroomsTotal": 2,
            "floorSize": {"@type": "QuantitativeValue", "value": 140, "unitCode": "MTK"},
            "address": {"@type": "PostalAddress", "streetAddress": "Sidi Kamal St., El Mandara Bahri",
                        "addressCountry": "EG"},
            "geo": {"@type": "GeoCoordinates", "latitude": 31.2765, "longitude": 30.0065},
            "amenityFeature": [{"@type": "LocationFeatureSpecification", "name": a, "value": True}
                               for a in AMENITIES_ALLOWED],
        },
        "image": [f"{IMG}-{i}.png" for i in range(9)],
    }
    graph = {"@context": "https://schema.org", "@graph": [
        {"@type": "Organization", "name": "Aqarmap"}, {"@type": "WebSite", "url": "https://aqarmap.com.eg"},
        {"@type": "WebPage", "name": "listing"}, ld_listing]}
    names = ["Home", "For rent", "Egypt", "Alexandria", "El Mandara", "El Mandara Bahri", "Sidi Kamal St."]
    crumbs = {"@context": "https://schema.org", "@type": "BreadcrumbList", "itemListElement": [
        {"@type": "ListItem", "position": i + 1, "item": {"@id": f"/en/crumb-{i}", "name": n}}
        for i, n in enumerate(names)]}
    return f"""<!DOCTYPE html><html><head><title>{title}</title>
<script id="listing-details-schema" type="application/ld+json">{json.dumps(graph, ensure_ascii=False)}</script>
<script id="Breadcrumb-schema" type="application/ld+json">{json.dumps(crumbs)}</script></head>
<body style="overflow: auto;"><main><h1>{title}</h1>
<ul><li>Floor: 12</li><li>Bedrooms: 3</li><li>Bathrooms: 2</li><li>Reference number: 123</li></ul>
<section class="similar-listings"><a href="/en/listing/7298779-for-rent-other-1/">Other 1</a>
<a href="/en/listing/7287553-for-rent-other-2/">Other 2</a></section></main>
<dialog id="topSellerSuccessModal">Your request was sent successfully</dialog>
{push}</body></html>"""


def results_page() -> str:
    def card(lid, title, price, area, beds, baths):
        return f"""<article class="listing-card" aria-labelledby="listing-{lid}-title">
<a href="/en/listing/{lid}-for-rent-x/"><h2 id="listing-{lid}-title">{title}</h2></a>
<data value="{price}">EGP {price:,} /month</data>
<ul><li><i class="size-icon"></i><span>{area}</span></li><li><i class="bedroom-icon"></i><span>{beds}</span></li>
<li><i class="bathroom-icon"></i><span>{baths}</span></li></ul>
<a href="/en/for-rent/property-type/alexandria/el-mandara/">El Mandara</a>
<a href="/en/for-rent/property-type/alexandria/el-mandara-bahri/">El Mandara Bahri</a></article>"""

    def payload(lid, slug, title, price, area, extra=None):
        d = {
            "id": lid, "slug": slug, "title": title, "price": price, "area": area,
            "location": {"id": 3011, "slug": "el-mandara-bahri",
                         "title_full_path": "Alexandria / El Mandara / El Mandara Bahri", "parent": {"id": 3000}},
            "previewDetails": {"propertyTypeSlug": "apartment", "propertyTypeTitle": "Apartment",
                               "updatedAt": "2026-11-16T10:00:00+02:00",
                               "largePhotos": [{"src": f"{IMG}-L{lid}.png"}],
                               "paymentMethodLabel": "label.payment_method.from_owner_cash"},
            "attributes": {"rooms": "3", "baths": "2", "floor": "12", "finishType": "SUPER_LUX"},
            "user": {"id": 1819928, "fullName": "Ahmed Zain", "has_active_logo_service": True,
                     "phones": [{"number": MASK}]},
            "photosCount": 9, "isCallRequest": False, "featuredType": None, "category": 5, "payment_method": 1,
        }
        d.update(extra or {})
        return d

    first = payload(LID, SLUG, "For rent - Apartment 140 M² Super lux", 20000, 140, {
        "payment_plans": [{"duration": 30, "price": 20000}, {"duration": 1, "price": 800}],
        "amenities": ["SECURITY", "ELEVATOR"], "center_lat": 31.2765, "center_lng": 30.0065,
        "is_featured": False, "reference_number": "123",
        "translations": [{"locale": "ar", "title": "شقة مفروشة", "description": "وصف", "address": "عنوان"}],
    })
    second = payload(7298779, "for-rent-other-1", "Other 1", 9000, 90)
    stream = "9:" + json.dumps({"initialListings": [first, second], "initialQuery": "section=for-rent"},
                               ensure_ascii=False, separators=(",", ":")) + "\n"
    return f"""<!DOCTYPE html><html><head><title>Results</title></head><body><h1>12 Properties for rent</h1>
{card(LID, "For rent - Apartment 140 M² Super lux", 20000, 140, 3, 2)}{card(7298779, "Other 1", 9000, 90, 3, 2)}
<script>self.__next_f.push([1,{json.dumps(stream)}])</script></body></html>"""


def main() -> None:
    default = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else default
    out.mkdir(parents=True, exist_ok=True)
    (out / "detail_7416775.html").write_text(detail_page(), encoding="utf-8")
    (out / "results_page_1.html").write_text(results_page(), encoding="utf-8")
    print(f"wrote 2 fixtures to {out}")


if __name__ == "__main__":
    main()
