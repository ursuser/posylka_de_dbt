with raw as (
    select * from {{ source('mv3_data', 'dict_source') }}
),

final as (
    select
        source_id,
        source_name
    from raw
)

select * from final
