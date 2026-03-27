with raw as (
    select * from {{ source('mv3_data', 'mv3_data_orders') }}
),

final as (
    select
        order_nr,
        created_at,
        user_nr,
        amount,
        cast(source as int64)        as source_id,
        cast(payment_type as int64)  as payment_type_id,
        cast(delivery_type as int64) as delivery_type_id,
        promo_code,
        loaded_at
    from raw
)

select * from final
