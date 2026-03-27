# Power BI Dashboards — Plan

## Report Structure (7 pages, priority order)

| # | Page | Data Source | Status |
|---|------|-------------|--------|
| 1 | Traffic | GA4 | done |
| 2 | CRM / Sales | CRM (BigQuery) | next |
| 3 | Cohorts | CRM | pending |
| 4 | RFM | CRM | pending |
| 5 | Google Ads | Google Ads (BigQuery) | pending |
| 6 | Organic / SEO | Search Console (BigQuery) | pending |
| 7 | Overview | all sources | last |

Overview is built last — when all data sources are connected and pages are stable.

---

## Page 1: Traffic (refinements needed)

**Current state:** built, working. Needs the following changes:

**Changes:**
- [ ] Remove scatter plot (orders vs AOV by channel) — belongs on Ads page
- [ ] Add funnel visual (replacing scatter plot): Sessions → View Item → Add to Cart → Begin Checkout → Purchase
- [ ] Add KPI cards: CR (buyers / users), ARPPU (revenue / buyers), Orders per Buyer (orders / buyers)

**KPI cards (final set):**
`users | buyers | orders | revenue | AOV | CR | ARPPU | orders/buyer`

**Notes:**
- GEO and device breakdowns are covered by existing slicers (country, device filters) — no separate visuals needed
- Sessions not added as KPI card — not informative standalone, already visible in source_medium table
- Add to Cart data unreliable before 2026-01-26 — annotate in funnel or filter start date

---

## Page 2: CRM / Sales

**Metrics (from client requirements):**
- Revenue (выручка)
- Orders (количество заказов)
- AOV — Average Order Value (средняя корзина)
- ASP — Average Selling Price (средняя цена единицы = revenue / units sold)
- Conversion Rate — orders / sessions (GA4 sessions as denominator)
- Gross Margin — requires cost price in order items table (TBC)
- New Customers vs Returning Customers
- Refunds (количество возвратов)
- Refund Rate (%)
- Delivery Time Avg (среднее время доставки)
- Процент выкупа (delivered orders / total orders × 100%)
- Porto / postal costs — requires field in orders table (TBC)

**CRM tables in BigQuery:**
- `orders` — main orders table
- `order_items` — products inside orders (units, price, possibly cost)
- `order_statuses` — status history (used for delivery time, buyout rate, refunds)

**Open questions:**
- [ ] Does `order_items` have a cost price column? (needed for Gross Margin)
- [ ] Does `orders` have a Porto/postal costs column?

---

## Page 3: Cohorts

**Goal:** retention analysis — how many customers return after first purchase.

**Metrics:**
- Cohort month = month of first purchase
- Retention rate by month (month 0, 1, 2, ...)
- Revenue by cohort over time
- Avg orders per customer by cohort

**Data source:** CRM orders table (customer_id + order_date)

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

## Page 5: Google Ads

**Metrics:**
- Impressions, Clicks, CTR
- Cost, CPC
- Conversions, CPO (cost per order)
- ROAS (revenue from GA4 or CRM / ad spend)
- Campaign / Ad Group breakdown

**Data source:** Google Ads export in BigQuery

---

## Page 6: Organic / SEO

**Metrics:**
- Clicks, Impressions, CTR, Average Position
- By query (top keywords)
- By page (top landing pages)
- Trends over time

**Data source:** Search Console export in BigQuery

---

## Page 7: Overview

**Goal:** one-glance summary for the client — most important KPIs from all sources.

**Metrics:** top KPIs from each page (Revenue, Orders, CR, Sessions, ROAS, top organic queries)

Built last, after all pages are stable.

---

## dbt Models Needed

| Model | Source | Used For |
|-------|--------|----------|
| `stg_crm_orders` | CRM orders table | base staging |
| `stg_crm_order_items` | CRM order items | ASP, Gross Margin |
| `stg_crm_order_statuses` | CRM statuses | Delivery Time, Buyout Rate, Refunds |
| `fct_crm_sales` | CRM | page 2: Sales |
| `fct_cohorts` | CRM | page 3: Cohorts |
| `fct_rfm` | CRM | page 4: RFM |
| `stg_google_ads` | Google Ads BQ export | page 5 |
| `stg_search_console` | SC BQ export | page 6 |

GA4 models already exist (`fct_events_real_sm`).
