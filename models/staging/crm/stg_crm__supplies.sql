{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["procurement_nr", "product_sku"],
        partition_by={"field": "procurement_date", "data_type": "date"},
    )
}}

with raw as (
    select src.*
    from {{ source('mv3_data', 'mv3_data_supplies') }} src
    {% if is_incremental() %}
    where src.procurement_nr in (
        select distinct procurement_nr
        from {{ source('mv3_data', 'mv3_data_supplies') }}
        where date(loaded_at) >= date_sub(current_date, interval 5 day)
    )
    {% endif %}
),

deduped as (
    select
        procurement_nr,
        any_value(date(procurement_at) having min loaded_at)                                    as procurement_date,
        any_value(supplier_nr having max loaded_at)                                             as supplier_nr,
        trim(split(any_value(supplier_name having max loaded_at), '\r')[offset(0)])             as supplier_name,
        product_sku,
        any_value(product_name having max loaded_at)                                            as product_name,
        any_value(quantity having max loaded_at)                                                as quantity,
        any_value(unit_price having max loaded_at)                                              as unit_price,
        not (
            product_sku in ('text', 'text1', 'deleted', '7777')
            or regexp_contains(product_sku, r'^9[0-9]{2,3}$')
        )                                                                                       as is_product,
        min(loaded_at)                                                                          as first_loaded_at,
        max(loaded_at)                                                                          as last_loaded_at
    from raw
    group by procurement_nr, product_sku
)

select * from deduped
