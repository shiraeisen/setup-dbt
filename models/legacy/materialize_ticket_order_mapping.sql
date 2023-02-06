-- materialize ticket_order_mapping

create or replace table `iron-zodiac-336013.zendesk_support.tickets_for_order_mapping_temp`
as
select  id, created_at, custom_order_appointment_number, description
from    `iron-zodiac-336013.zendesk_support.ticket`
where   date(_fivetran_synced) >= current_date()-1 ;

create or replace table `zendesk_support.ticket_order_mapping_temp`
as
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
union all
select  cast(t.id as string) ticket_id, fo.id fulfillment_orders_id
from    `zendesk_support.tickets_for_order_mapping_temp` t
        inner join `core_prod_public.fulfillment_orders` fo on split(split(description,'\nOrder ID: ')[safe_offset(1)],'\n')[safe_offset(0)]=order_id
where   split(split(description,'\nOrder ID: ')[safe_offset(1)],'\n')[safe_offset(0)] is not null
        and (custom_order_appointment_number is null or lower(trim(t.custom_order_appointment_number)) in ('','na')) ;

delete from `iron-zodiac-336013.zendesk_support.ticket_order_mapping`
where  ticket_id in (select ticket_id from `zendesk_support.ticket_order_mapping_temp`) ;

insert `iron-zodiac-336013.zendesk_support.ticket_order_mapping`
select  *
from    `iron-zodiac-336013.zendesk_support.ticket_order_mapping_temp` ;

drop table `iron-zodiac-336013.zendesk_support.tickets_for_order_mapping_temp`;

drop table `iron-zodiac-336013.zendesk_support.ticket_order_mapping_temp`;