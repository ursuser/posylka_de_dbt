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

    prep as (
        select
            parse_date('%Y%m%d', event_date) as event_date,
            user_pseudo_id,

            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = "ga_session_id"
                )
            ) as session_id,

            device.category as category,
            device.operating_system as operating_system,
            device.operating_system_version as operating_system_version,
            device.web_info.browser as browser,
            device.web_info.browser_version as browser_version,
            device.mobile_brand_name as mobile_brand_name,
            device.mobile_model_name as mobile_model_name,
            device.mobile_marketing_name as mobile_marketing_name,

            max(
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_number'
                )
            ) as session_number,

            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'session_engaged'
                )
            ) as session_engaged,

            sum(
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
            session_id,
            category,
            operating_system,
            operating_system_version,
            browser,
            browser_version,
            mobile_brand_name,
            mobile_model_name,
            mobile_marketing_name
    ),

    final as

    (
        select
            event_date,
            category,
            operating_system,
            operating_system_version,
            browser,
            browser_version,
            mobile_brand_name,
            mobile_model_name,
            mobile_marketing_name,

            count(distinct user_pseudo_id) as users,

            count(
                distinct case when session_number = 1 then user_pseudo_id else null end
            ) as new_users,

            count(distinct session_id) as sessions,

            count(
                distinct case when session_engaged = '1' then concat(session_id) end
            ) as engaged_sessions,

            sum(engagement_time_msec / 1000 / 60) as engagement_time_min,

            sum(page_view) as page_view,
            sum(first_visit) as first_visit,

            sum(view_promotion) as view_promotion,
            sum(select_promotion) as select_promotion,
            sum(view_item_list) as view_item_list,
            sum(view_item) as view_item,
            sum(select_item) as select_item,
            sum(add_to_cart) as add_to_cart,
            sum(view_cart) as view_cart,
            sum(remove_from_cart) as remove_from_cart,
            sum(add_to_wishlist) as add_to_wishlist,
            sum(begin_checkout) as begin_checkout,
            sum(add_shipping_info) as add_shipping_info,
            sum(add_payment_info) as add_payment_info,
            sum(purchase) as purchases,
            sum(view_search_results) as view_search_results,
            ifnull(sum(total_revenue), 0) as total_revenue

        from prep

        group by
            event_date,
            category,
            operating_system,
            operating_system_version,
            browser,
            browser_version,
            mobile_brand_name,
            mobile_model_name,
            mobile_marketing_name
    )

select *
from final
