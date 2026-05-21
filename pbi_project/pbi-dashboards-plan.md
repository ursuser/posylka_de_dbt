# Power BI Dashboards — Plan

## Next Steps — Stock Analytics Page

**Status:** KPI cards done (7 cards wired to M_supplies). Main visuals not yet built.

### To do

**1. Delete 2 stale cards** (still reference M_crm, don't belong on this page):

- `70623eaa070c7f28fcbe` — Refund Rate
- `97ebfa0f03873bdfad51` — Buyout Rate

→ User deletes manually in Power BI and rearranges remaining cards.

**2. Add slicers** (user creates in Power BI):

- Category (`fct_crm__supplies_sku[category_name]`)
- Supplier (`fct_crm__supplies_supplier[supplier_name]`)
- Dead stock toggle (`fct_crm__supplies_sku[is_dead_stock]`)
- Overstocked toggle (`fct_crm__supplies_sku[is_overstocked_at_procurement]`)

**3. Main visuals — configure in Power BI, Claude tweaks JSON if needed:**

| Visual | Type | X / Axis | Y / Values | Size / Legend |
| --- | --- | --- | --- | --- |
| SKU risk scatter | Scatter | `turnover_days` | `gross_margin_pct` | size = `stock_value`, legend = `is_dead_stock` |
| Stock by category | Bar (horizontal) | `category_name` | `stock_value` | color = `is_dead_stock` share |
| Top suppliers | Bar (horizontal) | `supplier_name` | `stock_value` | second bar = `dead_stock_value` |
| SKU detail table | Table | `product_name`, `category_name`, `stock_value`, `stock_units`, `turnover_days`, `gross_margin_pct`, `is_dead_stock`, `is_overstocked_at_procurement` | — | — |

**4. Relationship** (confirm wired in Power BI):

- `fct_crm__supplies_supplier[supplier_nr]` → `fct_crm__supplies_sku[primary_supplier_nr]` (1:many, filter direction supplier→sku)

---

### Technical notes

- Always close Power BI before Claude edits JSON/TMDL files
- `FILTER()` in DAX causes QuerySystemError on `fct_crm__supplies_sku` — use `CALCULATE()` or no filter (SUMX ignores NULLs automatically)
- `supplies_avg_gross_margin_pct` = revenue-weighted, formula: `DIVIDE(SUMX(table, gm_pct * revenue), SUM(revenue))`
- `is_packaging` and `is_promo` are filtered out at dbt level (not columns in final table)

---

## Report Structure (9 pages, priority order)

| # | Page | Data Source | Status |
| --- | --- | --- | --- |
| 1 | Traffic | GA4 | done |
| 2 | CRM / Sales | CRM (BigQuery) | done |
| 3 | Cohorts | CRM | done |
| 4 | RFM | CRM | pending |
| 5 | Products / SKU | CRM (BigQuery) | pending |
| 6 | Cross-sell | CRM (BigQuery) | done |
| 7 | Google Ads | Google Ads (BigQuery) | pending |
| 8 | Organic / SEO | Search Console (BigQuery) | pending |
| 9 | Overview | all sources | last |

Overview is built last — when all data sources are connected and pages are stable.

---

## Page 1: Traffic (refinements needed)

**Current state:** built, working. Needs the following changes.

**Changes:**

- [x] Remove scatter plot (orders vs AOV by channel) — belongs on Ads page
- [x] Add funnel visual (replacing scatter plot): Sessions → View Item → Add to Cart → Begin Checkout → Purchase
- [x] Add KPI cards: CR (buyers / users), ARPPU (revenue / buyers)
- Orders per Buyer — DAX measure added, KPI card not displayed (inferred from orders + buyers)

**KPI cards (final set):**
`users | buyers | orders | revenue | AOV | CR | ARPPU | orders/buyer`

**Notes:**

- GEO and device breakdowns are covered by existing slicers (country, device filters) — no separate visuals needed
- Sessions not added as KPI card — not informative standalone, already visible in source_medium table
- Add to Cart data unreliable before 2026-01-26 — annotate in funnel or filter start date

---

## Page 2: CRM / Sales

**Not available (no data in CRM):**

- Gross Margin — no cost_price in order_products table
- Porto / postal costs — no field in orders table
- Conversion Rate — needs GA4 join, handled on Traffic page

**Layout:**

```text
[ left panel: slicers ]  |  [ KPI cards row ]
                          |  [ combo chart: Revenue bars + AOV line ] [ stacked bar: New vs Returning by month ]
                          |  [ Revenue/Orders toggle ] [ donut: share by source ] [ bar: source ranked ]
                          |  [ line: Refund Rate trend ] [ line: Avg Delivery Days trend ]
```

**Slicers (left panel):**

- Date range (order_date)
- Source (source_name) — multi-select
- New / Returning customer
- Delivery type
- Payment type

**KPI cards (top row):**
`Revenue | Orders | AOV | ASP | New Customers % | Refund Rate % | Buyout Rate % | Avg Delivery Days`

**Visuals — implementation tasks:**

- [x] KPI cards (8 cards)
- [x] Slicers (left panel: date, source, new/returning, delivery type, payment type)
- [x] Combo chart: Revenue (bars) + AOV (line) with period toggle (Day/Week/Month/Year)
- [x] Period Field Parameter: Day / Week / Month / Year toggle on combo chart
- [x] Scatter/bubble chart: Orders (X) vs AOV (Y), sized by Revenue, by source_name
- [x] Stacked bar: Orders — New vs Returning by month (incorporated into combo chart as legend)
- [x] Field Parameter: Revenue / Orders toggle (shared by donut + bar + combo chart)
- [x] Donut chart: orders by source (with Revenue/Orders toggle)
- [x] Horizontal bar: source ranked by Revenue/Orders, switches via field parameter
- [x] Line chart: Refund Rate % trend by month
- [x] Line chart: Avg Delivery Days trend by month

**DAX measures needed:**

- `Revenue` = SUMX filtered on is_executed
- `Orders` = COUNTROWS filtered on is_executed
- `AOV` = Revenue / Orders
- `ASP` = Revenue / SUM(units_sold)
- `New Customers %` = DIVIDE(new orders, total orders)
- `Refund Rate %` = DIVIDE(refund orders, executed + refund)
- `Buyout Rate %` = DIVIDE(executed, executed + cancelled + refund)
- `Avg Delivery Days` = AVERAGE(delivery_days)

---

## Page 3: Cohorts

Status: done

**Built:**

- Cohort period slicer (Last N Months/Weeks)
- Source/campaign slicer
- Month / Week toggle
- Retention rate / Retained customers toggle
- Line chart: retention rate by period
- Bar chart: cohort size
- Cohort matrix table (heatmap)

**Data source:** `fct_cohorts` — includes acquisition dimensions from GA4 + CRM fallback

---

## Page 4: RFM

**Goal:** segment customers by purchase behavior.

**Dimensions:**

- R — Recency (days since last order)
- F — Frequency (number of orders)
- M — Monetary (total revenue per customer)

**Output:** RFM segments (Champions, Loyal, At Risk, Lost, etc.)

**Data source:** CRM orders table

**Implementation note:** RFM scoring likely computed in dbt as a mart model, then consumed in PBI.

---

## Page 5: Products / SKU

**Goal:** product-level performance — which SKUs sell best, and inventory health.

**Not available (no data in CRM):**

- Out of Stock — no inventory data in CRM
- Inventory turnover — no inventory data
- Margin by SKU — no cost_price in order_products table
- Stock level (units) — no inventory data
- Stock value (sum) — no inventory data
- Days of inventory — no inventory data

**KPI cards:**

- `Active Products` = count of distinct products with at least one executed order in period
- `Top SKU` = product name with highest revenue in period
- ~~Out of Stock~~ — not available
- ~~Inventory turnover~~ — not available

**Visuals:**

- Bar chart: Revenue by SKU (top N, with category slicer)
- Bar chart: Orders by SKU (top N)
- Table: Product performance — product name, orders, revenue, units sold, ASP
- ~~Margin by SKU~~ — not available

**Slicers:**

- Date range
- Category
- Product name (search/multi-select)

**DAX measures needed:**

- `Active Products` = DISTINCTCOUNT(product_id) filtered on executed orders
- `Top SKU` = product with MAX(Revenue)
- `Units Sold` = SUM(quantity) from order_products
- `ASP` = Revenue / Units Sold

**Data source:** `stg_crm__order_products` joined to `fct_crm_sales` / orders

**Implementation note:** may need a new mart model `fct_products` aggregating order_products with order status filter (executed only).

---

## Page 6: Cross-sell

Status: done (existing page `items_cross_sell`)

Kept as a separate page — different analytical question from SKU performance (pair relationships vs individual product metrics).

---

## Page 7: Google Ads (was page 5)

**Metrics:**

- Impressions, Clicks, CTR
- Cost, CPC
- Conversions, CPO (cost per order)
- ROAS (revenue from GA4 or CRM / ad spend)
- Campaign / Ad Group breakdown

**Data source:** Google Ads export in BigQuery

---

## Page 8: Organic / SEO

**Metrics:**

- Clicks, Impressions, CTR, Average Position
- By query (top keywords)
- By page (top landing pages)
- Trends over time

**Data source:** Search Console export in BigQuery

---

## Page 9: Overview

**Goal:** one-glance summary for the client — most important KPIs from all sources.

**Metrics:** top KPIs from each page (Revenue, Orders, CR, Sessions, ROAS, top organic queries)

Built last, after all pages are stable.

---

## dbt Models Needed

| Model | Source | Used For | Status |
| --- | --- | --- | --- |
| `stg_crm__orders` | CRM orders table | base staging | done |
| `stg_crm__order_products` | CRM order items | ASP | done |
| `stg_crm__order_statuses` | CRM statuses | Delivery Time, Buyout Rate, Refunds | done |
| `stg_crm__dict_status` | CRM status dict | status categories | done |
| `stg_crm__dict_source` | CRM source dict | source names | done |
| `stg_crm__dict_delivery` | CRM delivery dict | delivery type names | done |
| `stg_crm__dict_payment` | CRM payment dict | payment type names | done |
| `fct_crm_sales` | CRM | page 2: Sales (daily) | done |
| `fct_cohorts` | CRM | page 3: Cohorts — month + week (weekly) | done |
| `fct_rfm` | CRM | page 4: RFM segments (weekly) | done |
| `int_ga4_purchases` | GA4 | purchase events with attribution (last/first/lndc click) | done |
| `int_crm__order_acquisition` | CRM + GA4 | order-level acquisition source (view) | done |
| `stg_google_ads` | Google Ads BQ export | page 5 | pending |
| `stg_search_console` | SC BQ export | page 6 | pending |

GA4 models already exist (`fct_events_real_sm`).

## Notes from CRM modeling

- Gross Margin not available — no cost_price in order_products table
- Porto/postal costs not available — no field in orders table
- Conversion Rate (orders/sessions) to be calculated in PBI joining GA4 sessions
- `stg_crm__orders` deduplicates: sum(amount), min(created_at), latest loaded_at for other fields
- ~1.9% of executed orders have negative delivery_days (CRM data bug) — set to null
- RFM uses explicit frequency thresholds (not ntile) due to 80% single-order customers
- Dagster tags: `daily` for fct_crm_sales, `weekly` for fct_cohorts and fct_rfm
