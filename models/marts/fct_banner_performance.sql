{% set partitions_to_replace = [
    "date_sub(current_date, interval 1 day)",
    "date_sub(current_date, interval 2 day)",
    "date_sub(current_date, interval 3 day)",
    "date_sub(current_date, interval 4 day)",
    "date_sub(current_date, interval 5 day)",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "event_date", "data_type": "date"},
        partitions=partitions_to_replace,
    )
}}

with

-- impressions: deduplicated view_promotion per banner + date
impressions as (
    select
        event_date,
        promotion_name,
        placement_type,
        creative_slot,
        count(*) as impressions
    from {{ ref('stg_ga4__events_promotion') }}
    where event_name = 'view_promotion'

    {% if is_incremental() %}
        and event_date in ({{ partitions_to_replace | join(",") }})
    {% endif %}

    group by 1, 2, 3, 4
),

-- click sessions: one row per (session, banner) for joining with funnel data
click_sessions as (
    select
        event_date,
        session_id,
        promotion_name,
        placement_type,
        creative_slot
    from {{ ref('stg_ga4__events_promotion') }}
    where event_name = 'select_promotion'

    {% if is_incremental() %}
        and event_date in ({{ partitions_to_replace | join(",") }})
    {% endif %}

),

-- funnel events per session from the main session fact
session_funnel as (
    select
        session_id,
        add_to_cart,
        begin_checkout,
        purchase,
        total_revenue
    from {{ ref('fct_events_real_sm') }}

    {% if is_incremental() %}
        where event_date in ({{ partitions_to_replace | join(",") }})
    {% endif %}

),

-- join clicks with funnel data
click_funnel as (
    select
        clk.event_date,
        clk.promotion_name,
        clk.placement_type,
        clk.creative_slot,
        clk.session_id,
        coalesce(fun.add_to_cart, 0) as add_to_cart,
        coalesce(fun.begin_checkout, 0) as begin_checkout,
        coalesce(fun.purchase, 0) as purchase,
        coalesce(fun.total_revenue, 0) as total_revenue
    from click_sessions as clk
    left join session_funnel as fun using (session_id)
),

-- aggregate clicks + funnel by banner + date
clicks_agg as (
    select
        event_date,
        promotion_name,
        placement_type,
        creative_slot,
        count(*) as clicks,
        count(distinct session_id) as click_sessions,
        countif(add_to_cart > 0) as add_to_cart_sessions,
        countif(begin_checkout > 0) as checkout_sessions,
        countif(purchase > 0) as purchase_sessions,
        sum(total_revenue) as revenue
    from click_funnel
    group by 1, 2, 3, 4
),

final as (
    select
        coalesce(imp.event_date, clk.event_date) as event_date,
        coalesce(imp.promotion_name, clk.promotion_name) as promotion_name,
        coalesce(imp.placement_type, clk.placement_type) as placement_type,
        coalesce(imp.creative_slot, clk.creative_slot) as creative_slot,
        coalesce(imp.impressions, 0) as impressions,
        coalesce(clk.clicks, 0) as clicks,
        safe_divide(coalesce(clk.clicks, 0), coalesce(imp.impressions, 0)) as ctr,
        coalesce(clk.click_sessions, 0) as click_sessions,
        coalesce(clk.add_to_cart_sessions, 0) as add_to_cart_sessions,
        coalesce(clk.checkout_sessions, 0) as checkout_sessions,
        coalesce(clk.purchase_sessions, 0) as purchase_sessions,
        coalesce(clk.revenue, 0) as revenue
    from impressions as imp
    full outer join clicks_agg as clk
        on imp.event_date = clk.event_date
        and imp.promotion_name = clk.promotion_name
        and imp.placement_type = clk.placement_type
        and imp.creative_slot = clk.creative_slot
)

select * from final
