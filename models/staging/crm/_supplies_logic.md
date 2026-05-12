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
