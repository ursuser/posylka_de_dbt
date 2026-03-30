with

all_orders as (
    select * from {{ ref('stg_crm__orders') }}
),

customer_first_order as (
    select
        user_nr,
        min(created_at) as first_order_at
    from all_orders
    group by user_nr
),

orders as (
    select *
    from all_orders
    where date(created_at) >= date_sub(current_date(), interval {{ var('crm_lookback_years', 3) }} year)
),

order_products_agg as (
    select
        order_nr,
        sum(quantity) as units_sold
    from {{ ref('stg_crm__order_products') }}
    group by order_nr
),

latest_status as (
    select
        order_nr,
        status_id,
        row_number() over (partition by order_nr order by changed_at desc) as rn
    from {{ ref('stg_crm__order_statuses') }}
),

first_executed_at as (
    select
        order_nr,
        min(changed_at) as executed_at
    from {{ ref('stg_crm__order_statuses') }}
    where status_id in (37, 38, 113, 118)
    group by order_nr
),

final as (
    select
        ord.order_nr,
        date(ord.created_at)                as order_date,
        ord.user_nr,

        ord.created_at = cfo.first_order_at as is_new_customer,

        ord.amount,
        coalesce(prd.units_sold, 0)         as units_sold,

        ord.source_id,
        src.source_name,
        ord.payment_type_id,
        pmt.payment_type,
        ord.delivery_type_id,
        dlv.delivery_type,
        ord.promo_code,

        sts.status_id                       as current_status_id,
        dst.status_name                     as current_status_name,
        dst.status_category                 as current_status_category,

        dst.status_category = 'executed'                            as is_executed,
        dst.status_category in ('return_full', 'return_partial')    as is_refund,
        dst.status_category = 'cancelled'                           as is_cancelled,

        case
            when dst.status_category = 'executed'
                 and date_diff(date(fex.executed_at), date(ord.created_at), day) >= 0
            then date_diff(date(fex.executed_at), date(ord.created_at), day)
            else null
        end                                 as delivery_days

    from orders as ord
    left join customer_first_order  as cfo on cfo.user_nr = ord.user_nr
    left join order_products_agg    as prd on prd.order_nr = ord.order_nr
    left join latest_status         as sts on sts.order_nr = ord.order_nr and sts.rn = 1
    left join {{ ref('stg_crm__dict_status') }}   as dst on dst.status_id = sts.status_id
    left join first_executed_at     as fex on fex.order_nr = ord.order_nr
    left join {{ ref('stg_crm__dict_source') }}   as src on src.source_id = ord.source_id
    left join {{ ref('stg_crm__dict_payment') }}  as pmt on pmt.payment_id = ord.payment_type_id
    left join {{ ref('stg_crm__dict_delivery') }} as dlv on dlv.delivery_id = ord.delivery_type_id
)

select * from final
