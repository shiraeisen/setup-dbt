--insert `views.appointments_materialized` 

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with final as (
    select * from {{ ref('appointments_temp_dbt_incremental')}}
)

select  *
from    final --fivetran_esteem_provide_staging.appointments_temp

