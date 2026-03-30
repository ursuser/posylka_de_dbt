with raw as (
    select * from {{ source('mv3_data', 'mv3_data_orders') }}
),

-- deduplicate: one row per order_nr
-- amount: sum (nets out refund rows with negative amounts)
-- created_at: min (earliest creation date)
-- other fields: from the latest loaded_at row
deduped as (
    select
        order_nr,
        sum(amount)                                                     as amount,
        min(created_at)                                                 as created_at,
        any_value(user_nr having max loaded_at)                         as user_nr,
        cast(any_value(source having max loaded_at) as int64)           as source_id,
        cast(any_value(payment_type having max loaded_at) as int64)     as payment_type_id,
        cast(any_value(delivery_type having max loaded_at) as int64)    as delivery_type_id,
        any_value(promo_code having max loaded_at)                      as promo_code,
        max(loaded_at)                                                  as loaded_at
    from raw
    group by order_nr
)

select * from deduped
