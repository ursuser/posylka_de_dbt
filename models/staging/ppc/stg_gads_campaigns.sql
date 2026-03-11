with

    gads_source as (
        select *
        from {{ source("google_ads_386_715_7751", "p_ads_Campaign_3867157751") }}
    ),

    campaigns as (
        select
            distinct
                campaign_id,
                campaign_name
        from gads_source)

select * from campaigns
