select  id, created_at, custom_order_appointment_number, description
from    `iron-zodiac-336013.zendesk_support.ticket`
where   date(_fivetran_synced) >= current_date()-1 
--where created_at >= '2023-03-01'