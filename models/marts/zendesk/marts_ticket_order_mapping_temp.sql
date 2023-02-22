select  distinct cast(t.id as string) ticket_id, first_value(fo.id) over (partition by t.id order by fo.created_at) fulfillment_orders_id 
from    `zendesk_support.tickets_for_order_mapping_temp` t
        inner join `core_prod_public.fulfillment_orders` fo on (
          trim(t.custom_order_appointment_number) = fo.order_id
          or trim(t.custom_order_appointment_number) = trim(external_order_id)
          or trim(t.custom_order_appointment_number) = trim(external_order_number)
          or trim(t.custom_order_appointment_number) = split(trim(external_order_id),'___')[safe_offset(0)]
          or trim(t.custom_order_appointment_number) = split(trim(external_order_id),'___')[safe_offset(1)]
        ) and timestamp(fo.created_at) < t.created_at
where   custom_order_appointment_number is not null
        and trim(t.custom_order_appointment_number) != ''
        and lower(trim(t.custom_order_appointment_number)) != 'na'