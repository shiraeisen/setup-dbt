--insert `views.fulfillment_orders_materialized` 


with final as (
    select * from {{ ref('fulfillment_orders_temp_dbt')}}
)

select  *
from    final --fivetran_esteem_provide_staging.fulfillment_orders_temp