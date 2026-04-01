with

orders as (
    select
        order_nr,
        user_nr,
        order_date,
        amount,
        is_executed
    from {{ ref('fct_crm_sales') }}
),

-- acquisition source of each user's first order
first_order_acquisition as (
    select
        ord.user_nr,
        acq.acquisition_sm,
        acq.acquisition_source,
        acq.acquisition_medium,
        acq.acquisition_campaign
    from orders as ord
    inner join {{ ref('int_crm__order_acquisition') }} as acq on acq.order_nr = ord.order_nr
    qualify row_number() over (partition by ord.user_nr order by ord.order_date) = 1
),


-- MONTHLY COHORTS

customer_cohort_month as (
    select
        ord.user_nr,
        date_trunc(min(ord.order_date), month) as cohort_period,
        acq.acquisition_sm,
        acq.acquisition_source,
        acq.acquisition_medium,
        acq.acquisition_campaign
    from orders as ord
    left join first_order_acquisition as acq on acq.user_nr = ord.user_nr
    group by
        ord.user_nr,
        acq.acquisition_sm,
        acq.acquisition_source,
        acq.acquisition_medium,
        acq.acquisition_campaign
),

cohort_size_month as (
    select
        cohort_period,
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign,
        count(distinct user_nr) as cohort_size
    from customer_cohort_month
    group by
        cohort_period,
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign
),

orders_offset_month as (
    select
        ord.user_nr,
        coh.cohort_period,
        coh.acquisition_sm,
        coh.acquisition_source,
        coh.acquisition_medium,
        coh.acquisition_campaign,
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
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign,
        period_offset,
        count(distinct user_nr) as retained_customers,
        count(*)                as orders,
        round(sum(amount), 2)   as revenue
    from orders_offset_month
    group by
        cohort_period,
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign,
        period_offset
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
        agg.revenue,
        agg.acquisition_sm,
        agg.acquisition_source,
        agg.acquisition_medium,
        agg.acquisition_campaign
    from agg_month as agg
    inner join cohort_size_month as siz
        on siz.cohort_period = agg.cohort_period
        and siz.acquisition_sm = agg.acquisition_sm
        and siz.acquisition_source = agg.acquisition_source
        and siz.acquisition_medium = agg.acquisition_medium
        and siz.acquisition_campaign = agg.acquisition_campaign
),


-- WEEKLY COHORTS

customer_cohort_week as (
    select
        ord.user_nr,
        date_trunc(min(ord.order_date), week(monday)) as cohort_period,
        acq.acquisition_sm,
        acq.acquisition_source,
        acq.acquisition_medium,
        acq.acquisition_campaign
    from orders as ord
    left join first_order_acquisition as acq on acq.user_nr = ord.user_nr
    group by
        ord.user_nr,
        acq.acquisition_sm,
        acq.acquisition_source,
        acq.acquisition_medium,
        acq.acquisition_campaign
),

cohort_size_week as (
    select
        cohort_period,
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign,
        count(distinct user_nr) as cohort_size
    from customer_cohort_week
    group by
        cohort_period,
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign
),

orders_offset_week as (
    select
        ord.user_nr,
        coh.cohort_period,
        coh.acquisition_sm,
        coh.acquisition_source,
        coh.acquisition_medium,
        coh.acquisition_campaign,
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
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign,
        period_offset,
        count(distinct user_nr) as retained_customers,
        count(*)                as orders,
        round(sum(amount), 2)   as revenue
    from orders_offset_week
    group by
        cohort_period,
        acquisition_sm,
        acquisition_source,
        acquisition_medium,
        acquisition_campaign,
        period_offset
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
        agg.revenue,
        agg.acquisition_sm,
        agg.acquisition_source,
        agg.acquisition_medium,
        agg.acquisition_campaign
    from agg_week as agg
    inner join cohort_size_week as siz
        on siz.cohort_period = agg.cohort_period
        and siz.acquisition_sm = agg.acquisition_sm
        and siz.acquisition_source = agg.acquisition_source
        and siz.acquisition_medium = agg.acquisition_medium
        and siz.acquisition_campaign = agg.acquisition_campaign
)

select * from monthly
union all
select * from weekly
order by period_type, cohort_period, period_offset
