# Banner Performance — Plan

## Goal

Analyze how users behave after viewing or clicking on website banners.
Metrics: impressions, clicks, CTR, post-click funnel (add to cart, purchase), revenue attribution.

---

## Phase 1: Data Discovery — DONE

**Events confirmed:** `view_promotion` and `select_promotion` exist in GA4.

**Key finding:** promotion parameters are in the `items` array, not in `event_params`.

**Available fields (from `items` array):**
- `promotion_name` — banner name (main identifier)
- `promotion_id` — always `(not set)`, not usable
- `creative_name` — image URL
- `creative_slot` — position on page (1, 2, 3)

**Banner placement type** can be derived from `creative_name` URL path:
- `/poster/top_full_width/` — full-width top banner
- `/poster/home/` — banner in main content area
- `/images/index/ru/` or `/images/index/de/` — static blocks (language-specific)

**Known banners (March 2026, normalized):**

| promotion_name (canonical) | Duplicate name | Placement |
|----------------------------|----------------|-----------|
| промо кедр | promo ked | home slider |
| РотФронт | RotFront | home |
| гематоген | hematogen | home |
| наурыз | nauryz | home, slots 1–3 |
| пахта | pachta | home, slots 1–3 |
| плов | plov | home, slots 1–3 |
| марафон продукты | marathon lebenshtel | top_full_width + home |
| 8 марта | 8 marta | top_full_width |
| Бери больше 30% | — | top_full_width |
| BB | — | top_full_width |
| промо рошен | — | home |
| Probierpaket | — | static, slot 1 |
| Freund | — | static, slot 2 |
| Gutschein | — | static, slot 3 |

**Data quality issue:** same banner tracked under two names (Russian + Latin/German).
Needs normalization in dbt (e.g., `case when promotion_name in ('promo ked', 'промо кедр') then 'промо кедр'`).

---

## Phase 2: dbt Models

### Model plan

| Model | Type | Description | Status |
|-------|------|-------------|--------|
| `stg_ga4__events_promotion` | staging | raw promotion events with items unpacked | pending |
| `int_ga4__banner_sessions` | intermediate | sessions with banner clicks + downstream funnel events | pending |
| `fct_banner_performance` | mart | aggregated: impressions, clicks, CTR, post-click conversions | pending |

### `stg_ga4__events_promotion`
- Source: `analytics_298705553.events_*`
- Filter: `event_name in ('view_promotion', 'select_promotion')`
- Unpack from `items` array: `promotion_name`, `creative_name`, `creative_slot`
- Unpack from `event_params`: `ga_session_id`, `page_location`
- Also extract: `user_pseudo_id`, `event_timestamp`, `event_date`
- Derive `placement_type` from `creative_name` URL prefix:
  - `top_full_width` if contains `/poster/top_full_width/`
  - `home` if contains `/poster/home/`
  - `static` if contains `/images/index/`
- Normalize `promotion_name` duplicates via `case when`

### `int_ga4__banner_sessions`
- For each session with a `select_promotion` event — find downstream events in same session: `add_to_cart`, `begin_checkout`, `purchase`
- Join with `fct_events_real_sm` or base GA4 events on `ga_session_id`
- Output: one row per (session, banner_click), with flags for downstream funnel steps and revenue

### `fct_banner_performance`
- Grain: `promotion_name` + `placement_type` + `creative_slot` + `event_date`
- Metrics:
  - `impressions` — count of `view_promotion`
  - `clicks` — count of `select_promotion`
  - `ctr` — clicks / impressions
  - `click_sessions` — distinct sessions with a click
  - `add_to_cart_after_click` — sessions with add_to_cart after banner click
  - `purchases_after_click` — sessions with purchase after banner click
  - `revenue_after_click` — revenue from sessions with banner click
  - `click_to_purchase_rate` — purchases_after_click / clicks

---

## Phase 3: PBI Page — Banners

**Slicers:**
- Date range
- Banner name (`promotion_name`)
- Placement type (`top_full_width` / `home` / `static`)
- Slot (1 / 2 / 3)

**KPI cards:**
`Impressions | Clicks | CTR | Post-click Purchases | Revenue after click`

**Visuals:**
- Bar chart: impressions vs clicks by banner (sorted by impressions)
- Funnel: Impression → Click → Add to Cart → Purchase
- Line: CTR trend over time by banner
- Table: full breakdown (all metrics, sortable)

---

## Decisions

1. **Attribution:** strict — revenue counted only if purchase event occurred AFTER the banner click within the same session (ordered by `event_timestamp`)
2. **Impression deduplication:** 1 unique impression per (`user_pseudo_id`, `ga_session_id`, `promotion_name`) — deduplicate in `stg_ga4__events_promotion`
3. **Static banners** (Probierpaket, Freund, Gutschein): included in the model, filterable via `placement_type = 'static'` slicer in PBI

---

## Next Action

Build `stg_ga4__events_promotion` — start with the items unpack and name normalization.
