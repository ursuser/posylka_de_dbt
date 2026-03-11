with

    gads_source as (
        select *
        from {{ source("google_ads_386_715_7751", "p_ads_CampaignBasicStats_3867157751") }}
    ),

    campaigns as (
        select
            segments_date,
            campaign_id,
            metrics_impressions,
            metrics_clicks,
            metrics_cost_micros
        from gads_source)

select * from campaigns
