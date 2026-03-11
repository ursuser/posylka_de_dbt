{{
    config(
        materialized="incremental",
        unique_key="user_pseudo_id",
        incremental_strategy="merge",
    )
}}

with
    ga4_source as (
        select
            user_pseudo_id,
            user_id
        from {{ source("analytics_298705553", "events") }}
        {% if is_incremental() %}
            where _table_suffix >= format_date('%Y%m%d', date_sub(current_date(), interval 3 day))
        {% else %}
            where _table_suffix >= format_date('%Y%m%d', date_sub(current_date(), interval 12 month))
        {% endif %}
    ),

    final as (
        select
            user_pseudo_id,
            max(user_id) as user_id
        from ga4_source
        group by user_pseudo_id
        having user_id is not null
    )

select *
from final
