-- Cross-sell analysis: three complementary approaches.
--
-- Query 1 (simple): which products appear most often in multi-item checkouts (2+ items in cart)?
-- Ranks products by frequency in multi-item sessions. Good first look, but shows no pairs.
-- Source: https://www.ga4bigquery.com/uncovering-cross-selling-gold-how-to-analyze-multi-item-purchase-patterns/
--
-- Query 2 (focused): for top-N popular products, what else do buyers purchase in the same transaction?
-- Shows explicit pairs: popular_item → co_purchased_item, ranked by times bought together.
-- Use for "customers who bought X also bought Y" recommendations.
-- NOTE: better to run against CRM orders data — more complete than GA4 (GA4 loses some transactions).
--
-- Query 3 (full pairs): all product pairs across all transactions, no filter.
-- Complete market basket analysis — shows every combination and how often it appears.
-- NOTE: same as Query 2 — prefer CRM data source over GA4 for purchase-based queries.
-- Source: https://www.ga4bigquery.com/ga4-bigquery-boost-cross-selling-with-market-basket-analysis/


-- ============================================================
-- Query 1: product frequency in multi-item checkouts
-- ============================================================

with begin_checkout_multi as (
    select
        evt.event_date,
        evt.event_timestamp,
        evt.user_pseudo_id,
        array_agg(distinct itm.item_name) as item_names
    from `posylka-de-478903.analytics_298705553.events_*` as evt,
        unnest(evt.items) as itm
    where evt.event_name = 'begin_checkout'
    group by
        evt.event_date,
        evt.event_timestamp,
        evt.user_pseudo_id
    having count(distinct itm.item_name) >= 2
)

select
    item,
    count(*) as event_count
from begin_checkout_multi,
    unnest(item_names) as item
group by item
order by event_count desc
limit 10;


-- ============================================================
-- Query 2: top-N popular items + what is bought with them
-- ============================================================

with purchases as (
    select
        ecommerce.transaction_id  as transaction_id,
        evt.user_pseudo_id,
        itm.item_id,
        itm.item_name,
        itm.quantity
    from `posylka-de-478903.analytics_298705553.events_*` as evt,
        unnest(evt.items) as itm
    where evt.event_name = 'purchase'
        and ecommerce.transaction_id is not null
),

top_items as (
    select
        item_id,
        item_name,
        sum(quantity) as total_quantity
    from purchases
    group by item_id, item_name
    order by total_quantity desc
    limit 20
)

select
    top.item_name                       as popular_item,
    oth.item_name                       as co_purchased_item,
    count(distinct prc.transaction_id)  as times_bought_together
from purchases prc
inner join top_items top
    on prc.item_id = top.item_id
inner join purchases oth
    on prc.transaction_id = oth.transaction_id
    and prc.item_id != oth.item_id
group by popular_item, co_purchased_item
order by popular_item, times_bought_together desc;


-- ============================================================
-- Query 3: all product pairs across all transactions (full market basket)
-- ============================================================

with purchase_items as (
    select
        ecommerce.transaction_id  as transaction_id,
        itm.item_name
    from `posylka-de-478903.analytics_298705553.events_*`,
        unnest(items) as itm
    where event_name = 'purchase'
        and ecommerce.transaction_id is not null
        and itm.item_name is not null
),

product_pairs as (
    select
        a.item_name       as item1_name,
        b.item_name       as item2_name,
        a.transaction_id
    from purchase_items a
    inner join purchase_items b
        on a.transaction_id = b.transaction_id
        and a.item_name < b.item_name
)

select
    item1_name,
    item2_name,
    count(distinct transaction_id)  as times_bought_together
from product_pairs
group by item1_name, item2_name
order by times_bought_together desc
