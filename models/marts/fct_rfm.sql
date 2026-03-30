with

executed_orders as (
    select
        user_nr,
        order_date,
        amount
    from {{ ref('fct_crm_sales') }}
    where is_executed
),

customer_metrics as (
    select
        user_nr,
        max(order_date)                                         as last_order_date,
        date_diff(current_date(), max(order_date), day)         as recency_days,
        count(*)                                                as frequency,
        round(sum(amount), 2)                                   as monetary
    from executed_orders
    group by user_nr
),

rfm_scores as (
    select
        user_nr,
        last_order_date,
        recency_days,
        frequency,
        monetary,

        -- r: recent buyers score high (invert ntile)
        6 - ntile(5) over (order by recency_days asc)   as r_score,

        -- f: explicit thresholds (80% of customers have 1 order, ntile is meaningless)
        case
            when frequency = 1  then 1
            when frequency = 2  then 2
            when frequency = 3  then 3
            when frequency <= 5 then 4
            else                     5
        end                                             as f_score,

        -- m: quintile
        ntile(5) over (order by monetary asc)           as m_score

    from customer_metrics
),

final as (
    select
        user_nr,
        last_order_date,
        recency_days,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        cast(r_score as string)
            || cast(f_score as string)
            || cast(m_score as string)                  as rfm_score,
        case
            when r_score >= 4 and f_score >= 4 and m_score >= 4    then 'Champions'
            when r_score >= 3 and f_score >= 4                      then 'Loyal'
            when r_score >= 4 and f_score between 2 and 3          then 'Potential Loyalists'
            when r_score >= 4 and f_score = 1                       then 'New Customers'
            when r_score = 3  and f_score = 1                       then 'Promising'
            when r_score = 3  and f_score >= 3                      then 'Need Attention'
            when r_score = 1  and f_score >= 4                      then 'Cannot Lose'
            when r_score <= 2 and f_score >= 3                      then 'At Risk'
            when r_score <= 2 and f_score <= 2                      then 'About to Sleep'
            else                                                         'Hibernating'
        end                                             as rfm_segment
    from rfm_scores
)

select * from final
