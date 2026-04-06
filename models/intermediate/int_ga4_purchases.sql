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

    ga4_source as (
        select *
        from {{ source("analytics_298705553", "events") }}
        where _table_suffix >= '{{ var("ga4_start_date", "20260301") }}'

        {% if is_incremental() %}
            and parse_date('%Y%m%d', regexp_extract(_table_suffix, '[0-9]+'))
                in ({{ partitions_to_replace | join(",") }})
        {% endif %}

    ),

    purchases_raw as (
        select
            parse_date('%Y%m%d', event_date) as event_date,
            event_timestamp,
            user_pseudo_id,
            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as session_id,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'transaction_id'
            ) as transaction_id,
            round(ecommerce.purchase_revenue, 2) as revenue
        from ga4_source
        where event_name = 'purchase'
    ),

    -- deduplicate: one row per transaction_id, keep earliest event
    -- in incremental runs also exclude transactions already recorded outside the replacement window
    -- (handles GA4 duplicate purchase events fired from different devices on different dates)
    prep as (
        select *
        from purchases_raw
        where transaction_id is not null

        {% if is_incremental() %}
            and transaction_id not in (
                select distinct transaction_id
                from {{ this }}
                where event_date < date_sub(current_date, interval 5 day)
            )
        {% endif %}

        qualify row_number() over (
            partition by transaction_id
            order by event_timestamp
        ) = 1
    ),

    sessions as (
        select * from {{ ref("int_session_real_sm") }}
    ),

    final as (
        select
            conv.event_date,
            conv.user_pseudo_id,
            conv.session_id,
            conv.transaction_id,
            conv.revenue,

            -- last-click: source/medium of the purchase session itself
            conv.event_date as last_click_event_date,
            conv.session_id as last_click_session_id,
            coalesce(
                concat(sess.source_lnd, ' / ', sess.medium_lnd),
                '(direct) / (none)'
            ) as last_click_sm,
            coalesce(sess.campaign_lnd, '(none)') as last_click_campaign,

            -- first-click: earliest non-direct purchase session for this user
            first_value(conv.event_date) over (
                partition by conv.user_pseudo_id
                order by conv.event_timestamp
            ) as first_click_event_date,
            coalesce(
                first_value(
                    nullif(concat(sess.source_lnd, ' / ', sess.medium_lnd), '(direct) / (none)')
                    ignore nulls
                ) over (
                    partition by conv.user_pseudo_id
                    order by conv.event_timestamp
                ),
                '(direct) / (none)'
            ) as first_click_sm,
            coalesce(
                first_value(
                    nullif(sess.campaign_lnd, '(none)') ignore nulls
                ) over (
                    partition by conv.user_pseudo_id
                    order by conv.event_timestamp
                ),
                '(none)'
            ) as first_click_campaign,

            -- last-non-direct-click: last non-direct purchase session up to current
            coalesce(
                last_value(
                    nullif(concat(sess.source_lnd, ' / ', sess.medium_lnd), '(direct) / (none)')
                    ignore nulls
                ) over (
                    partition by conv.user_pseudo_id
                    order by conv.event_timestamp
                    rows between unbounded preceding and current row
                ),
                '(direct) / (none)'
            ) as last_non_direct_click_sm,
            coalesce(
                last_value(
                    nullif(sess.campaign_lnd, '(none)') ignore nulls
                ) over (
                    partition by conv.user_pseudo_id
                    order by conv.event_timestamp
                    rows between unbounded preceding and current row
                ),
                '(none)'
            ) as last_non_direct_click_campaign

        from prep as conv
        left join sessions as sess on conv.session_id = sess.session_id
    )

select * from final
