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

    prev_prep as (
        select
            parse_date('%Y%m%d', event_date) as event_date,
            user_pseudo_id,

            device.category as device_category,
            geo.country,
            geo.city,

            concat(
                coalesce(traffic_source.source, '(direct)'),
                ' / ',
                coalesce(traffic_source.medium, '(none)')
                    ) as user_first_sm,
            traffic_source.name as user_first_campaign,

            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = "ga_session_id"
                )
            ) as session_id,

            (
                select value.int_value
                from unnest(event_params)
                where key = 'ga_session_number'
            ) as session_number,

            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'session_engaged'
                )
            ) as session_engaged,

            max(
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'engagement_time_msec'
                )
            ) as engagement_time_msec,

            countif(event_name = 'page_view') as page_view,
            countif(event_name = 'first_visit') as first_visit,

            countif(event_name = 'view_promotion') as view_promotion,
            countif(event_name = 'select_promotion') as select_promotion,
            countif(event_name = 'view_item_list') as view_item_list,
            countif(event_name = 'view_item') as view_item,
            countif(event_name = 'select_item') as select_item,
            countif(event_name = 'add_to_cart') as add_to_cart,
            countif(event_name = 'view_cart') as view_cart,
            countif(event_name = 'remove_from_cart') as remove_from_cart,
            countif(event_name = 'add_to_wishlist') as add_to_wishlist,
            countif(event_name = 'begin_checkout') as begin_checkout,
            countif(event_name = 'add_shipping_info') as add_shipping_info,
            countif(event_name = 'add_payment_info') as add_payment_info,
            countif(event_name = 'purchase') as purchase,
            countif(event_name = 'view_search_results') as view_search_results,
            sum(ecommerce.purchase_revenue) as total_revenue

        from ga4_source as events
        group by
            event_date,
            user_pseudo_id,
            device_category,
            country,
            city,
            user_first_sm,
            user_first_campaign,
            session_id,
            session_number

    ),

    prep_for_join as (
        select * from {{ ref("int_session_real_sm") }}
    ),

    final as (

        select
            events.event_date as event_date,
            events.user_pseudo_id,

            user_first_sm,
            user_first_campaign,

            device_category,
            country,
            city,
            session_id,

            concat(source_lnd, ' / ', medium_lnd) as source_medium_lnd,
            campaign_lnd,
            term_lnd,
            content_lnd,
            gclid_lnd,

            session_number,
            session_engaged,
            engagement_time_msec,
            page_view,
            first_visit,

            view_promotion,
            select_promotion,
            view_item_list,
            view_item,
            select_item,
            add_to_cart,
            view_cart,
            remove_from_cart,
            add_to_wishlist,
            begin_checkout,
            add_shipping_info,
            add_payment_info,
            purchase,
            view_search_results,
            total_revenue

        from prev_prep as events
        left join prep_for_join as sessions using (session_id)
    )

select * from final
