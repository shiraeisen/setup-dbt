-- create or replace table fivetran_esteem_provide_staging.appointments_temp as
-- (
{{ config(materialized='table') }}

with items as 
(
    select * from {{ ref('items')}}
),

addresses as
(
    select * from {{ ref('addresses')}}
),

final as
(
    select  
        a.id, 
        a.number name, 
        cast(job_type as string) type_id, t.name type_name, 
        cast(status as string) status_id, s.name status_name, 
        tb.service_area_id service_area_id, upper(r.name) service_area_name,
        tb.base_date delivery_date, 
        cast(a.delivery_service_level as string) delivery_service_level_id,
        dsl.name delivery_service_level_name,
        case 
            when lower(dsl.name) like '%1-person%' then true 
        end is_one_person_appointment,

        cast(null as int64) timeslot_id,-- replaced by logistics_time_box_id
        tb.time_box_name timeslot_name, -- from time_box_view
        timestamp(tb.starts_at) timeslot_starts_at, -- from time_box_view
        timestamp(tb.ends_at) timeslot_ends_at, -- from time_box_view

        timestamp(arrived_time) arrived_at, -- at destination
        timestamp(start_time) started_at, -- on doorstep
        timestamp(end_time) ended_at, -- appointmented ended; use for On Time Rate
        timestamp(a.cancelled_at) canceled_at, -- often faked for ikea
        timestamp(incomplete_at) incomplete_at, -- for failed jobs
        cast(a.failed_reason as string) failed_reason_id,
        fr.name failed_reason_name,
        trim(failed_notes) failed_notes,
        a.user_id,
        fca.external_id external_location_name,
        case 
            when source_type = 'Fulfillment::Order' then source_id 
        end fulfillment_orders_id,
        i.items,
        aa.address,
        case 
            when delivery_type=1 then true 
        end is_parcel,
        estimated_duration_seconds,
        ls.organization_id,
        a.air_skip is_air_skip,
        ls.id logistics_shipment_id,
        ls.shipment_number shipment_name,
        timestamp(a.completed_at) completed_at,
        a.logistics_time_box_id,-- new timeslot/timebox_id 
        timestamp(a.created_at) created_at
from    `core_prod_public.appointments` a
        left outer join `core_prod_public.fulfillment_customer_addresses` fca on a.location_id=fca.id and fca._fivetran_deleted is false
        left outer join `reference.appointment_type_view` t on cast(a.job_type as string)=t.id
        left outer join `reference.appointment_status_view` s on cast(a.status as string)=s.id
        left outer join `reference.time_boxes_view` tb on cast(a.logistics_time_box_id as string) = tb.id
        left outer join `reference.region_view` r on cast(tb.service_area_id as string)=r.id
        left outer join `reference.delivery_service_level_view` dsl on cast(a.delivery_service_level as string)=dsl.id
        left outer join `reference.appointment_failed_reason_view` fr on cast(a.failed_reason as string)=fr.id
        left outer join items i on a.id=i.appointment_id 
        left outer join addresses aa on a.id=aa.appointment_id
        left outer join `iron-zodiac-336013.core_prod_public.logistics_shipments` ls on a.id=ls.shipping_method_id and ls._fivetran_deleted is false
where   a._fivetran_deleted is false
        --and date(a._fivetran_synced) >= current_date()-1
        and a.created_at >= '2023-03-01'
        
)

select * from final