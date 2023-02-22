create or replace table `iron-zodiac-336013.zendesk_support.tickets_for_order_mapping_temp`
as (
    select * from {{ ref('stg_tickets_for_order_mapping_temp')}}
);

create or replace table `zendesk_support.ticket_order_mapping_temp`
as (
    select * from {{ ref('marts_ticket_order_mapping_temp')}}
    union all
    select * from {{ ref('marts_ticket_for_order_mapping_temp')}}
); 

delete from `iron-zodiac-336013.zendesk_support.ticket_order_mapping`
where  ticket_id in (select ticket_id from `zendesk_support.ticket_order_mapping_temp`) ;

insert `iron-zodiac-336013.zendesk_support.ticket_order_mapping`
select  *
from    `iron-zodiac-336013.zendesk_support.ticket_order_mapping_temp` ;

drop table `iron-zodiac-336013.zendesk_support.tickets_for_order_mapping_temp`;

drop table `iron-zodiac-336013.zendesk_support.ticket_order_mapping_temp`;