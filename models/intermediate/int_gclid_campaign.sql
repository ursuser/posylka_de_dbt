{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="gclid",
    )
}}

with

    source as (
        select
            cs.click_view_gclid as gclid,
            cs.campaign_id,
            c.campaign_name
        from {{ source("google_ads_386_715_7751", "p_ads_ClickStats_3867157751") }} cs
        left join (
            select distinct campaign_id, campaign_name
            from {{ source("google_ads_386_715_7751", "p_ads_Campaign_3867157751") }}
        ) c
            on cs.campaign_id = c.campaign_id
        where cs.click_view_gclid is not null

        {% if is_incremental() %}
            and cs._PARTITIONTIME >= timestamp_sub(current_timestamp(), interval 3 day)
        {% endif %}

        qualify row_number() over (partition by cs.click_view_gclid order by cs._PARTITIONTIME desc) = 1
    )

select * from source
