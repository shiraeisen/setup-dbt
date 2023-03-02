with temp as 
(
    select * from {{ ref('ticket_order_mapping_temp')}}
)

select * from temp
