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
        partition_by={"field": "date", "data_type": "date"},
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

    prep_ga4 as (
        select
            parse_date('%Y%m%d', event_date) as date,
            concat(
                user_pseudo_id,
                '_',
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = "ga_session_id"
                )
            ) as session_id,
            max(
                case when device.category = 'mobile' then 'mobile' else 'desktop' end
            ) as device_category,
            max(session_traffic_source_last_click.manual_campaign.source) as source,
            max(session_traffic_source_last_click.manual_campaign.medium) as medium,
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
            countif(event_name = 'purchase') as conversions

        from ga4_source
        group by date, user_pseudo_id, session_id
        having source = 'google' and medium = 'organic'
    ),

    aggregate_ga4 as (
        select
            date,
            landing_page,
            device_category,
            count(distinct session_id) as sessions,
            count(
                distinct case when session_engaged = '1' then session_id end
            ) as engaged_sessions,
            sum(conversions) as conversions
        from prep_ga4
        where landing_page is not null
        group by
            date,
            landing_page,
            device_category
    ),

    searchconsole_source as (
        select *
        from {{ ref("stg_search_console") }}

        {% if is_incremental() %}
            where data_date in ({{ partitions_to_replace | join(",") }})
        {% endif %}

    ),

    variables as (
        select
            [
                'posylka',
                'посылка',
                'posylka.de',
                'посылка де',
                'posylka de',
                'posylkade'
            ] as branded_queries
    ),

    prep_gsc as (
        select
            data_date as date,
            case
                when device = 'MOBILE' then 'mobile' else 'desktop'
            end as device_category,
            url as page,
            query,

            case
                when query is null
                then null
                when
                    exists (
                        select 1
                        from
                            unnest(
                                (select branded_queries from variables)
                            ) as branded_query
                        where query like '%' || branded_query || '%'
                    )
                then 'branded'
                else 'non-branded'
            end as query_type,

            round((sum(sum_position) / sum(impressions) + 1), 2) as avg_position,
            sum(impressions) as impressions,
            sum(clicks) as clicks
        from searchconsole_source
        group by date, device_category, page, query, query_type
    )

select
    ga4.date,
    ga4.device_category,
    ga4.landing_page,
    array_agg(
        struct(gsc.query_type, gsc.query, gsc.clicks, gsc.impressions, gsc.avg_position)
        order by gsc.clicks desc
    ) as search_terms,
    ga4.sessions,
    ga4.engaged_sessions,
    ga4.conversions,
    sum(gsc.clicks) as total_clicks,
    sum(gsc.impressions) as total_impressions,
    avg(gsc.avg_position) as avg_position,
from aggregate_ga4 as ga4
left join
    prep_gsc as gsc
    on ga4.date = gsc.date
    and ga4.landing_page = gsc.page
    and ga4.device_category = gsc.device_category
where gsc.query is not null
group by
    ga4.date,
    ga4.device_category,
    ga4.landing_page,
    ga4.sessions,
    ga4.engaged_sessions,
    ga4.conversions
