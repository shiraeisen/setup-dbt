--insert `views.fulfillment_orders_materialized` 

{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

with final as (
    select * from {{ ref('fulfillment_orders_temp_dbt_incremental')}}
)

select  *
from    final --fivetran_esteem_provide_staging.fulfillment_orders_temp