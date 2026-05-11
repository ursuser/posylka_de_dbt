{% set partitions_to_replace = [
    "date_sub(current_date, interval 1 day)",
    "date_sub(current_date, interval 2 day)",
    "date_sub(current_date, interval 3 day)",
    "date_sub(current_date, interval 4 day)",
    "date_sub(current_date, interval 5 day)",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "procurement_date", "data_type": "date"},
        partitions=partitions_to_replace,
    )
}}

with raw as (
    select *
    from {{ source('mv3_data', 'mv3_data_supplies') }}

    {% if is_incremental() %}
        where date(loaded_at) in ({{ partitions_to_replace | join(",") }})
    {% endif %}
),

deduped as (
    select
        procurement_nr,
        any_value(date(procurement_at) having max loaded_at)                                    as procurement_date,
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
        max(loaded_at)                                                                          as loaded_at
    from raw
    group by procurement_nr, product_sku
)

select * from deduped
