# stg_crm__supplies — Logic & Design Decisions

## Source Data Behavior

The source table `mv3_data_supplies` is append-only. Each time a procurement order
is uploaded from the ERP system, all its current lines are written as a new batch
with a fresh `loaded_at` timestamp.

### Multi-wave delivery scenario

A single procurement order can arrive in multiple waves:

```
procurement_nr = 525, ordered 25 items

Wave 1 (loaded_at = 2026-05-01):  15 items arrive at warehouse → full snapshot of 15 lines uploaded
Wave 2 (loaded_at = 2026-05-20):  remaining 10 items arrive   → full snapshot of 25 lines uploaded
```

Key point: **wave 2 is a full re-snapshot of the entire order**, not just the delta.
The source will contain 15 rows with `loaded_at ≈ 2026-05-01` and 25 rows with
`loaded_at ≈ 2026-05-20` for the same `procurement_nr`.

Gaps between waves can range from a few days to over a month.

---

## Deduplication Logic

Grain: one row per `(procurement_nr, product_sku)`.

| Column | Source |
|---|---|
| `procurement_date` | `date(procurement_at)` from the **earliest** batch (`having min loaded_at`) |
| all other fields | from the **latest** batch (`having max loaded_at`) |
| `first_loaded_at` | `min(loaded_at)` — when this line was first seen |
| `last_loaded_at` | `max(loaded_at)` — when this line was last updated |

`procurement_date` is taken from the first batch because it reflects the original order
date. Later batches may carry a different `procurement_at` (delivery date), which would
distort timeline analysis.

Orders where `first_loaded_at != last_loaded_at` were supplemented at least once.
Use this to audit merge correctness or analyze partial-delivery patterns.

---

## Why merge, not insert_overwrite

`insert_overwrite` replaces entire date partitions. The table is partitioned by
`procurement_date`. If two orders share the same `procurement_date` and only one
of them gets a new wave, `insert_overwrite` would replace the whole partition —
losing all data for the unaffected order.

`merge` on `(procurement_nr, product_sku)` updates only the rows that actually
changed, leaving everything else untouched.

---

## Incremental Strategy

On each daily run:

1. Find all `procurement_nr` values that had any activity in the last 5 days
   (i.e., any row with `date(loaded_at) >= current_date - 5 days`).
2. Re-fetch **all rows** from the source for those procurement_nrs (not just
   the recent batch) — this ensures correct deduplication across all waves.
3. Merge the result into the target table on `(procurement_nr, product_sku)`.

This means if order #525 gets wave 2 on May 20, the run will re-read both the
May 1 and May 20 batches for that order and produce the correct final row.

---

## Non-product Lines

Not all rows represent real goods. The source mixes in logistics costs, catalogs,
and labeling fees under special SKU codes:

| SKU pattern | Meaning |
|---|---|
| `text`, `text1`, `deleted`, `7777` | transport / misc costs |
| `^9[0-9]{2,3}$` (e.g. 9046, 9991) | printed catalogs, labeling, shipping fees |

Use `is_product = true` to filter to real goods. Non-product rows are kept for
full procurement cost visibility.

---

## SKU Join Quality: Supplies ↔ Orders

As of 2026-05-12, joining `stg_crm__supplies` to `stg_crm__order_products` on `product_sku`:

| Direction | Total SKUs | Matched | Unmatched |
|---|---|---|---|
| Supplies → Orders | 45 847 | 11 372 (24.8%) | 34 475 (75.2%) |
| Orders → Supplies | 15 113 | 11 372 (75.2%) | 3 741 (24.8%) |

SKU formats are consistent in both tables (8-digit strings, 99.9% of rows).

### Why orders SKUs are missing in supplies (3 741 SKUs)

Not a data quality problem — three structural reasons:

1. **Billing / logistics SKUs** — shipping fees, packaging, dry ice, catalogs, Amazon Prime
   surcharges (SKUs: 9993, 9996, 9994, 9998, 9981, 9982, 05110000, 05110800, 99101010, `text`).
   These are never procured.

2. **Composite sets** — e.g. SKU 08503866 "KazanoFF Ø49 + Kasan 22L set" (155 orders, ~€40K revenue).
   The set SKU itself is not in supplies; it is assembled from components that ARE procured
   separately (08403479 = oven, 08064645 / 08103192 = kazan — all active in supplies).

3. **Food & pharmacy products** — items like Валидол, Гематоген, Сгущёнка, Гречка appear in
   hundreds of orders but have no supply records. Likely sourced via a separate procurement
   channel not captured in `mv3_data_supplies`.

### Why supply SKUs are missing in orders (dead stock, 34 475 SKUs)

Two distinct patterns found:

**Pattern A — Brand replacement (true dead stock):**
The company switched from the Scharkoff brand (Olymp supplier) to KazanoFF brand
(PE DAVR METALL supplier) for fire ovens / учаги. Old stock was never sold.

| Dead SKU | Name | Units | Value |
|---|---|---|---|
| 08400264 | Учаг Scharkoff Ø40.8cm | 1 264 | €138 791 |
| 08035716 | Feuerofen Ø40.5cm | 1 196 | €91 780 |
| 08400263 | Учаг Scharkoff Ø37.4cm | 967 | €82 428 |

Replacement SKUs actively selling in orders: 08403477, 08403478, 08403479 (KazanoFF).

**Pattern B — SKU rename (phantom dead stock):**
Same physical product procured under two different SKU codes. Old SKU shows as dead
stock only because orders reference the new SKU.

Example: Мантоварка 28cm from Olymp
- Old SKU **08146548** (Russian name, first procured 2020) → 2 621 units, €115K, last
  procurement 2025-04-01 — **shows as dead stock**
- New SKU **08801788** (German name, first procured 2022) → same supplier (Olymp),
  avg price €49.87 — **actively sold** (68 orders)

Action needed: audit whether 08146548 and 08801788 are the same physical item.
If yes, the €115K is not true dead stock — it is procured inventory with a SKU mismatch.

### Total dead stock value (2026-05-12)

34 475 SKUs, 1 703 549 units, **€6 485 364** at procurement cost.
Pattern B (SKU renames) inflates this number — true dead stock is lower.
Exact split between Pattern A and Pattern B is not yet quantified.

Top suppliers by dead stock value:
1. Olymp Handels GmbH — €1 366 194 (21.1%)
2. REDMOND LV SIA — €472 399 (7.3%)
3. Sima Land Izida — €405 884 (6.3%)
4. Lackmann — €313 627 (4.8%)
5. Monolith Mitte — €285 051 (4.4%)
