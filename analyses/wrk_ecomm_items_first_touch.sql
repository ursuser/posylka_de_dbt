-- Which product did the user see first, and did they eventually buy it?
-- Shows for each product: how many users saw it first, how many of them made any purchases,
-- and how often that first-viewed product ended up in the final transaction.
-- Use to identify products that act as entry points and actually convert vs. just attract views.
-- Source: https://www.ga4bigquery.com/ga4-first-touch-analysis-which-products-create-your-most-valuable-customers/

with first_viewed_product as (
    select
        user_pseudo_id,
        array_agg(item.item_name order by event_timestamp asc)[offset(0)] as first_viewed_name
    from `posylka-de-478903.analytics_298705553.events_*`,
        unnest(items) as item
    where event_name = 'view_item'
        and item.item_name is not null
    group by user_pseudo_id
),

user_purchases as (
    select
        user_pseudo_id,
        ecommerce.transaction_id as transaction_id,
        array_agg(item.item_name) as items_in_transaction
    from `posylka-de-478903.analytics_298705553.events_*`,
        unnest(items) as item
    where event_name = 'purchase'
        and item.item_name is not null
        and ecommerce.transaction_id is not null
    group by user_pseudo_id, transaction_id
),

joined as (
    select
        fv.first_viewed_name,
        fv.user_pseudo_id,
        up.transaction_id,
        up.items_in_transaction,
        case
            when up.transaction_id is not null
                 and fv.first_viewed_name in unnest(up.items_in_transaction)
            then 1 else 0
        end as first_viewed_in_transaction
    from first_viewed_product fv
    left join user_purchases up
        on fv.user_pseudo_id = up.user_pseudo_id
)

select
    first_viewed_name,
    count(distinct user_pseudo_id)                                                              as users_first_saw_product,
    count(distinct transaction_id)                                                              as total_transactions_by_those_users,
    count(distinct case when first_viewed_in_transaction = 1 then transaction_id end)           as transactions_including_first_viewed,
    round(
        safe_divide(
            count(distinct case when first_viewed_in_transaction = 1 then transaction_id end) * 100.0,
            count(distinct transaction_id)
        ),
        2
    )                                                                                           as pct_transactions_including_first_viewed
from joined
group by first_viewed_name
order by users_first_saw_product desc, pct_transactions_including_first_viewed desc
