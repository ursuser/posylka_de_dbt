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
            device.category as device_category,
            geo.city as city,
            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_id'
                )
            ) as session_id,
            min(parse_date('%Y%m%d', event_date)) as date,
            max(
                case
                    when
                        (
                            select value.int_value
                            from unnest(event_params)
                            where event_name = 'page_view' and key = 'entrances'
                        )
                        = 1
                    then
                        (
                            select value.string_value
                            from unnest(event_params)
                            where event_name = 'page_view' and key = 'page_location'
                        )
                end
            ) as landing_page,
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
            max(
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'ga_session_number'
                )
            ) as session_number,
            countif(event_name = 'purchase') as conversions,
            sum(
                case when event_name = 'purchase' then ecommerce.purchase_revenue end
            ) as purchase_revenue,
        from ga4_source

        group by
            event_date,
            user_pseudo_id,
            session_id,
            device_category,
            city
    ),

    final as

    (
        select
            event_date,
            regexp_replace(
                landing_page, r'^(https?://)?(www\.)?([^?#]*)[?#]?.*$', '\\3'
            ) as landing_page,
            device_category,
            city,
            count(distinct user_pseudo_id) as users,
            count(distinct session_id) as sessions,

            count(
                distinct case when session_number = 1 then user_pseudo_id end
            ) as new_users,

            safe_divide(
                count(distinct case when session_number = 1 then session_id end),
                count(distinct session_id)
                ) as percentage_new_sessions,

            safe_divide(
                count(distinct case when session_number = 1 then user_pseudo_id end),
                count(distinct user_pseudo_id)
                ) as percentage_new_users,

            safe_divide(
                sum(engagement_time_msec / 1000),
                count(distinct case when session_engaged = '1' then session_id end)
            ) as average_engagement_time_per_session_seconds,

            safe_divide(
                count(distinct case when session_engaged = '1' then session_id end),
                count(distinct session_id)
            ) as engagement_rate,

            sum(conversions) as conversions,
            sum(purchase_revenue) as purchase_revenue,
            safe_divide(sum(conversions), count(distinct session_id)) as conversion_rate

        from prep
        where landing_page is not null
        group by
            event_date,
            landing_page,
            device_category,
            city
    )

select * from final
