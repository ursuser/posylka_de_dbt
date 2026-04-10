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
    where
        event_name in ('view_promotion', 'select_promotion')
        and _table_suffix >= (
            {% if target.name == 'dev' %}
                format_date('%Y%m%d', date_sub(current_date(), interval 30 day))
            {% else %}
                '{{ var("ga4_start_date", "20260201") }}'
            {% endif %}
        )

    {% if is_incremental() %}
        and parse_date('%Y%m%d', regexp_extract(_table_suffix, '[0-9]+'))
            in ({{ partitions_to_replace | join(",") }})
    {% endif %}

),

name_mapping as (
    select * from {{ ref('promotion_name_mapping') }}
),

-- unnest items to get promotion fields; skip items without promotion_name
events_unnested as (
    select
        parse_date('%Y%m%d', event_date) as event_date,
        event_timestamp,
        event_name,
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
            where key = 'page_location'
        ) as page_location,
        item.promotion_name,
        item.creative_name,
        item.creative_slot
    from ga4_source
    cross join unnest(items) as item
    where item.promotion_name is not null
),

-- derive placement_type from creative_name url + normalize duplicate promotion names via seed
enriched as (
    select
        evt.event_date,
        evt.event_timestamp,
        evt.event_name,
        evt.user_pseudo_id,
        evt.session_id,
        evt.page_location,
        case
            when contains_substr(evt.creative_name, '/poster/top_full_width/') then 'top_full_width'
            when contains_substr(evt.creative_name, '/poster/home/') then 'home'
            when contains_substr(evt.creative_name, '/images/index/') then 'static'
            else 'other'
        end as placement_type,
        evt.creative_name,
        evt.creative_slot,
        case
            when mp.canonical_name is not null then mp.canonical_name
            when regexp_contains(evt.promotion_name, r'\p{Cyrillic}') then concat('[unmapped] ', evt.promotion_name)
            else evt.promotion_name
        end as promotion_name
    from events_unnested as evt
    left join name_mapping as mp on evt.promotion_name = mp.raw_name
),

-- deduplicate view_promotion: 1 impression per (user_pseudo_id, session_id, promotion_name)
impressions_deduped as (
    select *
    from enriched
    where event_name = 'view_promotion'
    qualify
        row_number() over (
            partition by user_pseudo_id, session_id, promotion_name
            order by event_timestamp
        ) = 1
),

-- clicks: no deduplication — each select_promotion is a real click
clicks as (
    select *
    from enriched
    where event_name = 'select_promotion'
),

final as (
    select * from impressions_deduped
    union all
    select * from clicks
)

select * from final
