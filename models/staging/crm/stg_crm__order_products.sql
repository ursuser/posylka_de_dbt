with raw as (
    select * from {{ source('mv3_data', 'mv3_data_products') }}
),

final as (
    select
        order_nr,
        product_sku,
        product_name,
        quantity,
        unit_price,
        category_id,
        category_name,
        is_cool,
        is_freeze,
        loaded_at
    from raw
)

select * from final
