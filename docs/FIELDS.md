# Field mapping

Spec field (from [SPEC.md](SPEC.md) section 3) to database column in the `listings` table.

Source values: `payload` (RSC stream object), `jsonld` (`#listing-details-schema`), `breadcrumb` (`#Breadcrumb-schema`), `detail_dom` (labels read from the page body, stored in `detail_*` columns), `card` (results page HTML card), `derived`, `not stored`.

When two sources exist, the first one listed wins. The `detail_*` fallbacks are applied at export time. They do not overwrite the parsed columns in the database.

## JSON-LD fields

| Spec field | DB column | Source | Notes |
|---|---|---|---|
| `listing_id` | `id` | jsonld, payload | From the URL in `.url`. Payload `id` must match or the JSON-LD block is ignored. |
| `title` | `title` | jsonld, payload, card | |
| `url` | `url` | card, jsonld | Detail pages use the JSON-LD URL. Results pages use the card link. |
| `date_posted` | `date_posted_iso` | jsonld | ISO 8601 UTC, for example `2026-11-17T19:41:27Z`. Future dates are kept. |
| `date_posted` | `posted_date` | payload, jsonld, detail_dom | Payload key if present, else the JSON-LD value, else `detail_posted_date`. |
| `category` | `category_ld` | jsonld | `ForRent` or `ForSale`. |
| `price` | `price_amount` | jsonld, payload, card | Headline price in its own period. |
| `price` | `price_monthly` | payload, derived | The 30-day payment plan if present, else `price_amount` converted to a month. |
| `currency` | `currency` | jsonld | Defaults to `EGP`. |
| `availability` | `availability` | jsonld | `https://schema.org/` prefix removed. |
| `property_type` | `property_type_ld` | jsonld | The schema.org `@type`, for example `Apartment`. |
| `property_type` | `property_type` | payload | Canonical slug. Free text from titles is never used. |
| `rooms` | `beds`, `beds_raw` | jsonld, payload, card | `beds` is filled for residential types only. `beds_raw` is always filled. |
| `baths` | `baths`, `baths_raw` | jsonld, payload, card | Same rule as rooms. |
| `area_m2` | `area_sqm` | jsonld, payload, card, detail_dom | |
| `street` | `address` | payload, jsonld | Comma separated, not split. |
| `country` | `country` | jsonld | |
| `lat` | `lat` | jsonld, payload, detail_dom | Payload keys `lat`, `latitude`, `center_lat`. Dropped if outside the bounding box. |
| `lng` | `lng` | jsonld, payload, detail_dom | Payload keys `lng`, `lon`, `longitude`, `center_lng`. |
| `amenities_jsonld` | `amenities_jsonld` | jsonld | JSON list of names. The site marks every allowed amenity true, so this list is broad. |
| `images` | `images_jsonld` | jsonld | JSON list of the gallery URLs. |
| `images` | `photos` | payload, jsonld | JSON list. Payload `largePhotos` first, JSON-LD if the payload has none. |

## Payload fields

| Spec field | DB column | Source | Notes |
|---|---|---|---|
| `id` | `id` | payload | |
| `slug` | `slug` | payload | |
| `section_slug` | `section_slug` | payload, derived | `section.slug`, else parsed from the URL, else from the JSON-LD category. |
| `property_type_slug` | `property_type_raw` | payload | Slug as the site sent it. |
| `property_type_title` | `property_type_title` | payload | |
| `address` | `address` | payload | |
| `description_en` | `description_en` | payload | |
| `center_lat`, `center_lng` | `lat`, `lng` | payload | See the JSON-LD table. |
| `status` | `status` | payload | Integer. The meaning of each value is not published. |
| `status_label` | `status_label` | payload | i18n key, stored raw. |
| `payment_method` | `payment_method` | payload | |
| `paymentMethodLabel` | `payment_method_label` | payload | i18n key, stored raw. |
| `reference_number` | `reference_number` | payload, detail_dom | Falls back to `detail_reference` at export. |
| `is_featured` | `is_featured` | payload | 1 or 0. |
| `was_featured` | `was_featured` | payload | 1 or 0. |
| `is_hold` | `is_hold` | payload | 1 or 0. |
| `isCallRequest` | `is_call_request` | payload | 1 means no price is stored. |
| `accepts_messages` | `accepts_messages` | payload | 1 or 0. |
| `has_whatsapp` | `has_whatsapp` | payload | 1 or 0. The number is not stored. |
| `phone_number` | none | not stored | Phone numbers are scrubbed from everything stored. |
| `whatsapp_number` | none | not stored | Same. |
| `country_code` | none | not stored | Lives inside the scrubbed `phones` object. |
| `seller.id` | `advertiser_id` | payload | |
| `seller.full_name` | `advertiser_name` | payload, card | |
| `seller.type` | `seller_type` | payload | Numeric `user_type`. No mapping is published. |
| `seller.active_listings` | `seller_active_listings` | payload | `user.activeListingsCount`. The other counter on the page (`topSellerData`) is not stored. |
| `seller.score` | `seller_score` | payload | |
| `seller.logo` | `seller_logo_url` | payload | `logo.thumbnails.large`. |
| `seller.personal_photo` | `seller_photo_url` | payload | `personal_photo.thumbnails.large`. |
| `attributes.rooms` | `beds_raw`, `beds` | payload | JSON-LD value wins when both exist. |
| `attributes.baths` | `baths_raw`, `baths` | payload | Same. |
| `attributes.floor` | `floor` | payload, detail_dom | Falls back to `detail_floor` at export. |
| `attributes.finish_type` | `finish_type` | payload, detail_dom | Falls back to `detail_finishing` at export. |
| `amenities_codes` | `amenities_codes` | payload | JSON list of the codes this unit has. |
| `property_type.amenities` | `property_type_amenities_json` | payload | JSON list of codes the type allows. Context only, never merged with the unit's list. |
| `payment_plans` | `payment_plans_json` | payload | Full list as JSON. |
| `payment_plans[duration=30]` | `price_monthly` | payload | |
| `payment_plans[duration=1]` | `daily_rate` | payload, card | |
| `payment_plans[0].duration` | `payment_duration` | payload | |
| `price_per_meter` | `price_per_meter` | payload | The site's own value. Not the same as the exported `price_per_sqm`. |
| `price_per_sqm` | `price_per_sqm` | derived | Export only. `price_monthly / area_sqm`. |
| `video_url` | `video_url` | payload | |
| `brochure` | `brochure` | payload | |
| `meta_title` | `meta_title` | payload | |
| `meta_description` | `meta_description` | payload | |
| `property_view` | `view_code` | payload | |
| `property_view_label` | `property_view_label` | payload | i18n key, stored raw. |
| `translations[ar].title` | `translations_ar_title` | payload | |
| `translations[ar].description` | `translations_ar_description` | payload | |
| `translations[ar].address` | `translations_ar_address` | payload | |
| `translations[en]` | none | not stored | English values are the top-level fields. |
| `updated_at` | `updated_at` | payload | ISO 8601 with offset. |
| `sections` | none | not stored | |
| `category` (number) | `category` | payload | Raw code, meaning unconfirmed. |
| `seller_role` | none | not stored | |

## Other sources

| Spec field | DB column | Source | Notes |
|---|---|---|---|
| Breadcrumb names | `breadcrumb_path` | breadcrumb | JSON list, outermost first. Not used for any other field. |
| Location path | `district`, `area_name`, `district_id`, `area_id`, `area_parent_id`, `location_slug`, `location`, `in_compound` | payload | From `location.title_full_path` and `location.parent`. |
| Card fields | `card_advertiser`, `badges`, `image_url`, `district_url`, `area_url`, `card_text`, `card_html` | card | Results pages only. |
| Furnished | `furnished` | payload, derived, detail_dom | Payload flag, else a `furnished-*` type slug, else `detail_furnished`. |
| Year built | `year_built` | payload, detail_dom | |
| Detail page labels | `detail_posted_date`, `detail_updated_date`, `detail_area_sqm`, `detail_beds`, `detail_baths`, `detail_floor`, `detail_view`, `detail_year_built`, `detail_finishing`, `detail_furnished`, `detail_seller_type`, `detail_payment`, `detail_reference`, `detail_property_type`, `detail_reception`, `detail_lat`, `detail_lng` | detail_dom | Fallback values only. |
| Detail page text | `detail_text`, `detail_attrs_json` | detail_dom | Phone-like strings scrubbed. `detail_text` is exported only with `--with-text`. |
| Raw payload | `raw_json` | payload | Compressed, phones scrubbed. Source for `reparse`. |
| Detail bundle | `raw_aux_json` | jsonld, breadcrumb, payload | Compressed. JSON-LD, breadcrumbs and the detail payload. Source for `reparse`. |
| `payload_kind` | `payload_kind` | derived | `list` or `detail`. |
