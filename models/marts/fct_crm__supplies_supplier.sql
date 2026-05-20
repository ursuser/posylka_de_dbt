with

sku_data as (
    select * from {{ ref('fct_crm__supplies_sku') }}
),

procurement_dates as (
    select distinct
        supplier_nr,
        procurement_date
    from {{ ref('stg_crm__supplies') }}
    where procurement_date >= date('2025-06-01')
),

procurement_gaps as (
    select
        supplier_nr,
        date_diff(
            procurement_date,
            lag(procurement_date) over (partition by supplier_nr order by procurement_date),
            day
        ) as gap_days
    from procurement_dates
),

procurement_freq as (
    select
        supplier_nr,
        round(avg(gap_days), 1) as avg_days_between_procurements
    from procurement_gaps
    where gap_days is not null
    group by supplier_nr
),

total_cost as (
    select sum(total_procured_cost) as grand_total
    from sku_data
),

final as (
    select
        s.primary_supplier_nr                                       as supplier_nr,
        max(s.primary_supplier_name)                                as supplier_name,

        count(*)                                                    as total_skus,
        countif(not s.is_dead_stock)                                as active_skus,
        countif(s.is_dead_stock)                                    as dead_stock_skus,

        round(sum(s.total_procured_cost), 2)                        as total_procured_cost,
        round(sum(s.stock_value), 2)                                as total_stock_value,

        safe_divide(
            sum(s.gross_margin_pct * s.total_sold_revenue),
            sum(s.total_sold_revenue)
        )                                                           as avg_gross_margin_pct,

        safe_divide(
            sum(s.total_procured_cost),
            max(tc.grand_total)
        )                                                           as procurement_share_pct,

        max(pf.avg_days_between_procurements)                       as avg_days_between_procurements

    from sku_data                as s
    cross join total_cost        as tc
    left join procurement_freq   as pf on pf.supplier_nr = s.primary_supplier_nr
    group by s.primary_supplier_nr
)

select * from final
