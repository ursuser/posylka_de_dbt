-- How many days on average does it take users from each traffic source to add to cart and purchase?
-- For each source/medium: total users, avg days to first add_to_cart, avg days to first purchase.
-- Use to compare conversion velocity across channels — which sources bring fast buyers vs. slow ones.
-- Helps evaluate channel ROI more fairly: a slow-converting channel may still be valuable long-term.
-- Source: https://www.ga4bigquery.com/ga4-bigquery-analyzing-average-days-to-key-actions-by-traffic-source/

with first_touch_users as (
    select
        user_pseudo_id,
        timestamp_micros(min(event_timestamp))  as first_touch_timestamp,
        traffic_source.source                   as first_touch_source,
        traffic_source.medium                   as first_touch_medium
    from `posylka-de-478903.analytics_298705553.events_*`
    where traffic_source.source is not null
    group by user_pseudo_id, traffic_source.source, traffic_source.medium
),

first_add_to_cart as (
    select
        user_pseudo_id,
        timestamp_micros(min(event_timestamp))  as first_add_to_cart_timestamp
    from `posylka-de-478903.analytics_298705553.events_*`
    where event_name = 'add_to_cart'
    group by user_pseudo_id
),

first_purchase as (
    select
        user_pseudo_id,
        timestamp_micros(min(event_timestamp))  as first_purchase_timestamp
    from `posylka-de-478903.analytics_298705553.events_*`
    where event_name = 'purchase'
    group by user_pseudo_id
)

select
    ftu.first_touch_source,
    ftu.first_touch_medium,
    count(distinct ftu.user_pseudo_id)                                                              as total_users,
    avg(timestamp_diff(atc.first_add_to_cart_timestamp, ftu.first_touch_timestamp, day))            as avg_days_to_add_to_cart,
    avg(timestamp_diff(prc.first_purchase_timestamp, ftu.first_touch_timestamp, day))               as avg_days_to_purchase
from first_touch_users ftu
inner join first_add_to_cart atc
    on ftu.user_pseudo_id = atc.user_pseudo_id
inner join first_purchase prc
    on ftu.user_pseudo_id = prc.user_pseudo_id
where ftu.first_touch_timestamp <= atc.first_add_to_cart_timestamp
group by ftu.first_touch_source, ftu.first_touch_medium
having count(distinct ftu.user_pseudo_id) >= 10
order by avg_days_to_purchase asc
