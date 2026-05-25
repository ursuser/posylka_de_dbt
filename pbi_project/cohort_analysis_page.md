# Cohort Analysis Page — Setup Guide

## Data model (fct_cohorts)

- `cohort_period` — date of first order for the cohort (month/week)
- `period_offset` — offset from cohort entry (0 = entry period, 1 = next, ...)
- `retention_rate` — % of customers who returned
- `cohort_size`, `retained_customers`, `revenue`, `orders`
- `period_type` — 'month' / 'week'
- acquisition dimensions: `acquisition_sm`, `acquisition_source`

---

## Slicers

1. `period_type` — month/week toggle (single select, button style)
2. `acquisition_sm` or `acquisition_source` — channel filter

---

## Main Visual: Matrix (heatmap)

| Setting | Value |
|---------|-------|
| Rows    | `cohort_label` (calculated column, see below) |
| Columns | `period_offset` |
| Values  | `retention_rate` |

**Steps:**
1. Insert → Matrix
2. Rows: drag `cohort_label` (NOT `cohort_period` directly — see hierarchy workaround below)
3. Columns: `period_offset`
4. Values: `retention_rate`, aggregation = **Average** (or Max if single row per cohort after filter)
5. Conditional formatting: right-click Values → Conditional formatting → Background color
   - Type: Gradient
   - From: white (0%) → To: dark green (100%)

---

## cohort_period Hierarchy Workaround

Power BI auto-expands date fields into Year→Quarter→Month hierarchy — the matrix becomes ugly.

**Fix — add calculated column in semantic model:**
```dax
cohort_label = FORMAT(fct_cohorts[cohort_period], "MMM YYYY")
```

Use `cohort_label` in Rows. Then set **Sort by Column** = `cohort_period` to preserve chronological order.

---

## Additional Visuals

### Line chart — retention curves by cohort
- X axis: `period_offset`
- Y axis: `retention_rate`
- Legend: `cohort_label`
- Shows how different cohorts behave over time

### Bar chart — cohort sizes
- X: `cohort_label`
- Y: `cohort_size`
- Quickly shows when large cohorts arrived

### KPI cards — avg retention at M1 / M3 / M6
```dax
Avg Retention M1 = CALCULATE(AVERAGE(fct_cohorts[retention_rate]), fct_cohorts[period_offset] = 1)
Avg Retention M3 = CALCULATE(AVERAGE(fct_cohorts[retention_rate]), fct_cohorts[period_offset] = 3)
Avg Retention M6 = CALCULATE(AVERAGE(fct_cohorts[retention_rate]), fct_cohorts[period_offset] = 6)
```

TODO: — KPI cards not yet built. All other visuals are done.
