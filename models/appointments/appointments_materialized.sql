--insert `views.appointments_materialized` 

with final as (
    select * from {{ ref('appointments_temp')}}
)

select  
from    final --fivetran_esteem_provide_staging.appointments_temp

{{ config(
    posthook="drop table fivetran_esteem_provide_staging.appointments_temp"
) }}