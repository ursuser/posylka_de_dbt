with

    orders as (
        select
            order_nr,
            source_id
        from {{ ref('stg_crm__orders') }}
    ),

    crm_sources as (
        select
            source_id,
            source_name
        from {{ ref('stg_crm__dict_source') }}
    ),

    ga4_purchases as (
        select
            transaction_id,
            last_click_sm,
            last_click_campaign
        from {{ ref('int_ga4_purchases') }}
    ),

    final as (
        select
            ord.order_nr,

            case
                when ga4.transaction_id is not null then 'ga4'
                else 'crm'
            end as source_type,

            case
                when ga4.transaction_id is not null
                then split(ga4.last_click_sm, ' / ')[safe_offset(0)]
                else src.source_name
            end as acquisition_source,

            case
                when ga4.transaction_id is not null
                then split(ga4.last_click_sm, ' / ')[safe_offset(1)]
                else '(crm)'
            end as acquisition_medium,

            case
                when ga4.transaction_id is not null
                then ga4.last_click_sm
                else concat(coalesce(src.source_name, '(unknown)'), ' / (crm)')
            end as acquisition_sm,

            case
                when ga4.transaction_id is not null
                then ga4.last_click_campaign
                else '(none)'
            end as acquisition_campaign

        from orders as ord
        left join crm_sources as src on ord.source_id = src.source_id
        left join ga4_purchases as ga4 on ord.order_nr = ga4.transaction_id
    )

select * from final
