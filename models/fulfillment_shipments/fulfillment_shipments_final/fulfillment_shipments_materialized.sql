--insert `views.fulfillment_shipments_materialized` 


with final as (
    select * from {{ ref('fulfillment_shipments_temp')}}
)

select  *
from    final --fivetran_esteem_provide_staging.fulfillment_shipments_temp