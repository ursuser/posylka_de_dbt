with raw as (
    select * from {{ source('mv3_data', 'mv3_data_status') }}
),

final as (
    select
        order_nr,
        order_status as status_id,
        changed_at,
        loaded_at
    from raw
)

select * from final
