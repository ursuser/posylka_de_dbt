with

orders as (
    select
        user_nr,
        order_date,
        amount,
        is_executed
    from {{ ref('fct_crm_sales') }}
),


-- MONTHLY COHORTS

customer_cohort_month as (
    select
        user_nr,
        date_trunc(min(order_date), month) as cohort_period
    from orders
    group by user_nr
),

cohort_size_month as (
    select
        cohort_period,
        count(distinct user_nr) as cohort_size
    from customer_cohort_month
    group by cohort_period
),

orders_offset_month as (
    select
        ord.user_nr,
        coh.cohort_period,
        date_diff(
            date_trunc(ord.order_date, month),
            coh.cohort_period,
            month
        )               as period_offset,
        ord.amount
    from orders as ord
    inner join customer_cohort_month as coh on coh.user_nr = ord.user_nr
    where ord.is_executed
),

agg_month as (
    select
        cohort_period,
        period_offset,
        count(distinct user_nr) as retained_customers,
        count(*)                as orders,
        round(sum(amount), 2)   as revenue
    from orders_offset_month
    group by cohort_period, period_offset
),

monthly as (
    select
        'month'                 as period_type,
        agg.cohort_period,
        agg.period_offset,
        siz.cohort_size,
        agg.retained_customers,
        round(agg.retained_customers * 100.0 / siz.cohort_size, 1) as retention_rate,
        agg.orders,
        agg.revenue
    from agg_month as agg
    inner join cohort_size_month as siz on siz.cohort_period = agg.cohort_period
),


-- WEEKLY COHORTS

customer_cohort_week as (
    select
        user_nr,
        date_trunc(min(order_date), week(monday)) as cohort_period
    from orders
    group by user_nr
),

cohort_size_week as (
    select
        cohort_period,
        count(distinct user_nr) as cohort_size
    from customer_cohort_week
    group by cohort_period
),

orders_offset_week as (
    select
        ord.user_nr,
        coh.cohort_period,
        date_diff(
            date_trunc(ord.order_date, week(monday)),
            coh.cohort_period,
            week
        )               as period_offset,
        ord.amount
    from orders as ord
    inner join customer_cohort_week as coh on coh.user_nr = ord.user_nr
    where ord.is_executed
),

agg_week as (
    select
        cohort_period,
        period_offset,
        count(distinct user_nr) as retained_customers,
        count(*)                as orders,
        round(sum(amount), 2)   as revenue
    from orders_offset_week
    group by cohort_period, period_offset
),

weekly as (
    select
        'week'                  as period_type,
        agg.cohort_period,
        agg.period_offset,
        siz.cohort_size,
        agg.retained_customers,
        round(agg.retained_customers * 100.0 / siz.cohort_size, 1) as retention_rate,
        agg.orders,
        agg.revenue
    from agg_week as agg
    inner join cohort_size_week as siz on siz.cohort_period = agg.cohort_period
)

select * from monthly
union all
select * from weekly
order by period_type, cohort_period, period_offset
