with raw as (
    select * from {{ source('mv3_data', 'dict_payment') }}
),

final as (
    select
        payment_id,
        payment_type
    from raw
)

select * from final
