with temp as 
(
    select * from {{ ref('ticket_order_mapping_temp_dbt')}}
)

select * from temp
