select  
        cast(t.id as string) ticket_id, 
        fo.id fulfillment_orders_id
from    `zendesk_support.tickets_for_order_mapping_temp` t
        inner join `core_prod_public.fulfillment_orders` fo on split(split(description,'\nOrder ID: ')[safe_offset(1)],'\n')[safe_offset(0)]=order_id
where   split(split(description,'\nOrder ID: ')[safe_offset(1)],'\n')[safe_offset(0)] is not null
        and (custom_order_appointment_number is null or lower(trim(t.custom_order_appointment_number)) in ('','na'))