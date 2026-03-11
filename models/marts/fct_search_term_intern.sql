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
            event_timestamp,
            user_pseudo_id,
            device.category as device_category,
            geo.country as country,
            geo.city as city,
            (
                select value.string_value
                from unnest(event_params)
                where key = 'search_term'
            ) as search_term,
        from ga4_source
        where event_name = 'view_search_results'
    ),

    final as (

        select
            event_date,
            search_term,
            device_category,
            country,
            city,
            count(event_timestamp) as search_events,
            count(distinct user_pseudo_id) as users
        from prep
        where search_term is not null
        group by
            event_date,
            search_term,
            device_category,
            country,
            city
    )

select *
from final
where trim(search_term) <> ''
