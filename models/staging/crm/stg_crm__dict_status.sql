with raw as (
    select * from {{ source('mv3_data', 'dict_status') }}
),

final as (
    select
        status_id,
        status_name,
        note,
        case
            when status_id in (37, 38, 113, 118) then 'executed'
            when status_id in (35, 88)           then 'cancelled'
            when status_id = 73                  then 'return_partial'
            when status_id = 74                  then 'return_full'
            when status_id in (33, 119)          then 'return_full'
            when status_id = 92                  then 'catalog_request'
            when status_id in (32, 85, 91, 101, 112, 130) then 'new'
            when status_id in (30, 59, 62, 111, 127, 128, 129, 143, 151, 161, 162, 163, 164, 166, 167) then 'in_progress'
            else 'unknown'
        end as status_category
    from raw
)

select * from final
