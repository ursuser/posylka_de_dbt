with source as (
        select *
        from {{ source("searchconsole", "searchdata_url_impression") }}),

prep_data as (
    select
            data_date,
            device,
            url,
            query,
            sum_position,
            impressions,
            clicks
        from source
)

select * from prep_data
