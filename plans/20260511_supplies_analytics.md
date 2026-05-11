# Supplies Analytics — What We Can Build

Source tables: `stg_crm__supplies` + `stg_crm__order_products` + `stg_crm__orders`

---

## 1. Product Margin

**What:** unit margin = sale price (order_products.unit_price) − purchase price (supplies.unit_price)

**How:** join on product_sku, take purchase price from the most recent procurement before the sale date

**Business value:** understand which products are actually profitable, not just high-revenue

**Complexity:** medium — need to match sale date to the closest preceding procurement date per SKU

---

## 2. Dead Stock / Dead Products

**What:** products procured but never sold (or not sold in the last N months)

**How:** left join supplies → order_products on product_sku, find SKUs with no matching orders

**Business value:** identify capital tied up in inventory that isn't moving

**Already found:** ~34K SKUs only in supplies, 7K matched with orders — big opportunity here

---

## 3. Supplier Performance

**What:** per supplier — volume, SKU count, average purchase price, price stability over time

**How:** aggregate stg_crm__supplies by supplier_nr

**Business value:** understand dependency on key suppliers, negotiate better terms

**Example insight:** top 3 suppliers (Olymp, REDMOND, Monolith) = 42% of total procurement value

---

## 4. Purchase Price Dynamics

**What:** how unit_price for the same SKU changes across procurement dates

**How:** group by product_sku + procurement_date, track unit_price over time

**Business value:** spot inflation per product category, validate price negotiations

**Note:** relevant for ~2K SKUs that have multiple suppliers — price can vary significantly

---

## 5. Time to First Sale

**What:** for each procurement, how many days until the procured SKUs started selling

**How:** join supplies procurement_date → min(order date) per SKU after procurement

**Business value:** understand inventory velocity — which products sell fast vs sit in warehouse

---

## 6. Procurement Coverage of Sales

**What:** what share of sold SKUs has a corresponding procurement record

**How:** join order_products → supplies, count matched vs unmatched SKUs

**Current state:** 47% of order SKUs matched — need remaining batches (2025-2026) to improve coverage

**Business value:** validate data completeness before building margin reports

---

## 7. Category-Level Procurement vs Sales

**What:** compare procured volume (quantity × unit_price) vs sold volume (revenue) by category

**How:** join supplies → order_products → category from order_products

**Business value:** which categories have healthy sell-through vs overstock

---

## Implementation Order

1. **Procurement coverage** — validate join quality first (depends on batch load completeness)
2. **Dead stock** — quick win, data already available
3. **Supplier performance** — straightforward aggregation, no sales join needed
4. **Product margin** — main goal, build after coverage is acceptable
5. **Purchase price dynamics** — add-on to margin analysis
6. **Time to first sale** — add-on to dead stock analysis
7. **Category procurement vs sales** — dashboard-level, build last

## Data Readiness

- Supplies data: 2020–May 2025 loaded, rest of 2025 + 2026 pending
- Margin analysis reliable only after full 2025 batch arrives
- Dead stock analysis possible now for 2020–2025 period
