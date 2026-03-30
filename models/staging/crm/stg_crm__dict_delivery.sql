with raw as (
    select * from {{ source('mv3_data', 'dict_delivery') }}
),

final as (
    select
        delivery_id,
        delivery_type
    from raw
)

select * from final
