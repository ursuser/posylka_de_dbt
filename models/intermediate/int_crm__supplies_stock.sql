{{
    config(
        materialized='table'
    )
}}

with

supplies as (
    select
        product_sku,
        product_name,
        supplier_nr,
        supplier_name,
        quantity,
        unit_price,
        procurement_date
    from {{ ref('stg_crm__supplies') }}
    where is_product = true
        and procurement_date >= date('2025-06-01')
),

executed_sales as (
    select
        op.product_sku,
        op.quantity,
        op.unit_price,
        s.order_date
    from {{ ref('stg_crm__order_products') }} as op
    inner join {{ ref('fct_crm_sales') }} as s on s.order_nr = op.order_nr
    where s.is_executed = true
        and s.order_date >= date('2025-06-01')
),

supplier_totals as (
    select
        product_sku,
        supplier_nr,
        supplier_name,
        sum(quantity * unit_price) as total_value
    from supplies
    group by product_sku, supplier_nr, supplier_name
),

primary_supplier as (
    select
        product_sku,
        supplier_nr   as primary_supplier_nr,
        supplier_name as primary_supplier_name
    from supplier_totals
    qualify row_number() over (partition by product_sku order by total_value desc) = 1
),

supplies_agg as (
    select
        product_sku,
        any_value(product_name having max procurement_date) as product_name,
        sum(quantity)                                       as total_procured_units,
        sum(quantity * unit_price)                          as total_procured_cost,
        max(procurement_date)                               as last_procurement_date
    from supplies
    group by product_sku
),

supplies_before_last as (
    select
        s.product_sku,
        sum(s.quantity) as units_procured_before
    from supplies as s
    inner join supplies_agg as sa on sa.product_sku = s.product_sku
    where s.procurement_date < sa.last_procurement_date
    group by s.product_sku
),

sales_agg as (
    select
        product_sku,
        sum(quantity)                                        as total_sold_units,
        sum(case when unit_price > 0 then quantity * unit_price end) as total_sold_revenue,
        sum(case when unit_price > 0 then quantity end)              as units_with_price
    from executed_sales
    group by product_sku
),

sales_before_last as (
    select
        es.product_sku,
        sum(es.quantity) as units_sold_before
    from executed_sales as es
    inner join supplies_agg as sa on sa.product_sku = es.product_sku
    where es.order_date < sa.last_procurement_date
    group by es.product_sku
),

analysis_days as (
    select date_diff(current_date, date('2025-06-01'), day) as days
),

final as (
    select
        sa.product_sku,
        sa.product_name,
        ps.primary_supplier_nr,
        ps.primary_supplier_name,

        sa.total_procured_units,
        sa.total_procured_cost,
        coalesce(sl.total_sold_units, 0)   as total_sold_units,
        coalesce(sl.total_sold_revenue, 0) as total_sold_revenue,

        safe_divide(sa.total_procured_cost, sa.total_procured_units)        as avg_purchase_price,
        safe_divide(sl.total_sold_revenue, sl.units_with_price)             as avg_selling_price,

        sa.total_procured_units - coalesce(sl.total_sold_units, 0)         as stock_units,
        (sa.total_procured_units - coalesce(sl.total_sold_units, 0))
            * safe_divide(sa.total_procured_cost, sa.total_procured_units)  as stock_value,

        safe_divide(coalesce(sl.total_sold_units, 0), ad.days)              as avg_daily_sales,
        safe_divide(
            sa.total_procured_units - coalesce(sl.total_sold_units, 0),
            safe_divide(coalesce(sl.total_sold_units, 0), ad.days)
        )                                                                   as turnover_days,

        sa.last_procurement_date,
        safe_divide(
            coalesce(sbl.units_procured_before, 0) - coalesce(sbls.units_sold_before, 0),
            safe_divide(coalesce(sl.total_sold_units, 0), ad.days)
        )                                                                   as stock_days_at_last_procurement

    from supplies_agg              as sa
    left join sales_agg            as sl   on sl.product_sku  = sa.product_sku
    left join primary_supplier     as ps   on ps.product_sku  = sa.product_sku
    left join supplies_before_last as sbl  on sbl.product_sku = sa.product_sku
    left join sales_before_last    as sbls on sbls.product_sku = sa.product_sku
    cross join analysis_days       as ad
)

select * from final
