-- How many users who abandoned checkout eventually returned and purchased? And how long did it take?
-- Finds first checkout abandonment per user, then tracks if/when they completed a purchase after.
-- Output: single row — total abandoners, recovery rate %, churn rate %, avg/min/max days to return.
-- Source: https://www.ga4bigquery.com/analyze-the-recovery-time-of-checkout-abandoners/
--
-- TODO: extend with breakdown by traffic source/medium and by time period (month/week).
-- Currently returns one aggregate row — too little to be actionable on its own.

with ga4_events as (
    select
        user_pseudo_id,
        event_name,
        timestamp_micros(event_timestamp)                                               as event_ts,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as ga_session_id
    from `posylka-de-478903.analytics_298705553.events_*`
    where event_name in ('begin_checkout', 'purchase')
        and exists (select 1 from unnest(event_params) where key = 'ga_session_id')
),

session_events as (
    select
        user_pseudo_id,
        ga_session_id,
        min(event_ts)                                                           as session_start_ts,
        max(case when event_name = 'begin_checkout' then 1 else 0 end)         as has_begin_checkout,
        max(case when event_name = 'purchase' then 1 else 0 end)               as has_purchase
    from ga4_events
    group by user_pseudo_id, ga_session_id
),

abandonment_sessions as (
    select
        user_pseudo_id,
        ga_session_id,
        session_start_ts as abandon_ts
    from session_events
    where has_begin_checkout = 1 and has_purchase = 0
),

first_abandonment as (
    select
        user_pseudo_id,
        ga_session_id   as abandon_session_id,
        abandon_ts
    from (
        select
            user_pseudo_id,
            ga_session_id,
            abandon_ts,
            row_number() over (partition by user_pseudo_id order by abandon_ts asc) as rn
        from abandonment_sessions
    )
    where rn = 1
),

purchase_sessions as (
    select
        user_pseudo_id,
        ga_session_id,
        session_start_ts as purchase_ts
    from session_events
    where has_purchase = 1
),

purchase_after_first_abandonment as (
    select
        fab.user_pseudo_id,
        fab.abandon_session_id,
        fab.abandon_ts,
        min(prc.purchase_ts) as purchase_ts
    from first_abandonment fab
    inner join purchase_sessions prc
        on fab.user_pseudo_id = prc.user_pseudo_id
        and prc.purchase_ts > fab.abandon_ts
    group by fab.user_pseudo_id, fab.abandon_session_id, fab.abandon_ts
),

user_days_diff as (
    select
        user_pseudo_id,
        abandon_ts,
        purchase_ts,
        date_diff(date(purchase_ts), date(abandon_ts), day) as days_between
    from purchase_after_first_abandonment
)

select
    (select count(*) from first_abandonment)                                                                    as total_abandoners,
    count(distinct user_pseudo_id)                                                                              as users_with_abandonment_and_return,
    round((count(distinct user_pseudo_id) * 100.0) / (select count(*) from first_abandonment), 2)              as recovery_rate_percent,
    100 - round((count(distinct user_pseudo_id) * 100.0) / (select count(*) from first_abandonment), 2)        as churn_rate_percent,
    avg(days_between)                                                                                           as avg_days_to_return_purchase,
    min(days_between)                                                                                           as min_days_to_return,
    max(days_between)                                                                                           as max_days_to_return
from user_days_diff
