with

executed_orders as (
    select
        order_nr,
        order_date
    from {{ ref('fct_crm_sales') }}
    where is_executed
),

order_items as (
    select
        prd.order_nr,
        prd.product_sku,
        prd.unit_price,
        prd.quantity,
        date_trunc(ord.order_date, month) as order_month
    from {{ ref('stg_crm__order_products') }} as prd
    inner join executed_orders as ord
        on ord.order_nr = prd.order_nr
    where prd.product_sku is not null
),

-- canonical name per sku: cyrillic names first, then most frequent
item_names as (
    select
        product_sku,
        any_value(product_name having max sort_key) as product_name,
        any_value(category_name having max sort_key) as category_name
    from (
        select
            product_sku,
            product_name,
            category_name,
            count(*) as cnt,
            case
                when regexp_contains(product_name, '[А-Яа-яЁё]') then count(*) + 1000000
                else count(*)
            end as sort_key
        from {{ ref('stg_crm__order_products') }}
        where product_sku is not null
        group by product_sku, product_name, category_name
    )
    group by product_sku
),

item_order_counts as (
    select
        product_sku,
        count(distinct order_nr) as total_orders,
        avg(unit_price)          as avg_price
    from order_items
    group by product_sku
),

product_pairs as (
    select
        a.order_nr,
        a.order_month,
        a.product_sku                                               as item1_sku,
        b.product_sku                                               as item2_sku,
        a.unit_price * a.quantity + b.unit_price * b.quantity       as order_pair_revenue
    from order_items as a
    inner join order_items as b
        on  a.order_nr = b.order_nr
        and a.product_sku < b.product_sku
),

pair_counts_monthly as (
    select
        item1_sku,
        item2_sku,
        order_month,
        count(distinct order_nr) as times_bought_together,
        sum(order_pair_revenue)  as pair_revenue
    from product_pairs
    group by item1_sku, item2_sku, order_month
),

-- global filter: keep only pairs with >= 5 total across all time
valid_pairs as (
    select item1_sku, item2_sku
    from pair_counts_monthly
    group by item1_sku, item2_sku
    having sum(times_bought_together) >= 5
),

final as (
    select
        pcm.item1_sku,
        nm1.product_name                as item1_name,
        nm1.category_name               as item1_category_name,
        pcm.item2_sku,
        nm2.product_name                as item2_name,
        nm2.category_name               as item2_category_name,
        pcm.order_month,
        pcm.times_bought_together,
        pcm.pair_revenue,
        cnt1.total_orders               as item1_total_orders,
        cnt1.avg_price                  as item1_avg_price,
        cnt2.total_orders               as item2_total_orders,
        cnt2.avg_price                  as item2_avg_price
    from pair_counts_monthly as pcm
    inner join valid_pairs          as vld on vld.item1_sku = pcm.item1_sku and vld.item2_sku = pcm.item2_sku
    left join item_names            as nm1 on nm1.product_sku = pcm.item1_sku
    left join item_names            as nm2 on nm2.product_sku = pcm.item2_sku
    left join item_order_counts     as cnt1 on cnt1.product_sku = pcm.item1_sku
    left join item_order_counts     as cnt2 on cnt2.product_sku = pcm.item2_sku
)

select * from final
