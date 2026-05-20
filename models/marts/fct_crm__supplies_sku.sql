with

source as (
    select * from {{ ref('int_crm__supplies_stock') }}
),

final as (
    select
        product_sku,
        product_name,
        category_id,
        category_name,
        primary_supplier_nr,
        primary_supplier_name,

        total_procured_units,
        total_procured_cost,
        total_sold_units,
        total_sold_revenue,
        stock_units,
        stock_value,

        avg_purchase_price,
        avg_selling_price,
        safe_divide(
            avg_selling_price - avg_purchase_price,
            avg_selling_price
        )                                                    as gross_margin_pct,

        avg_daily_sales,
        turnover_days,

        last_procurement_date,
        stock_days_at_last_procurement,

        total_sold_units = 0                                 as is_dead_stock,
        coalesce(stock_days_at_last_procurement > 60, false) as is_overstocked_at_procurement

    from source
    where not is_packaging
        and not is_promo
)

select * from final
