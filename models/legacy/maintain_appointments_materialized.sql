-- maintain appointments_materialized
create or replace table fivetran_esteem_provide_staging.appointments_temp
as
with items as (
  select  shipping_method_id appointment_id,
          array_agg(
            struct(
              i.id,
              struct(
                case when length_value != 0 and lower(length_unit) = 'in' then length_value end as length_in,
                case when width_value != 0 and lower(width_unit) = 'in' then width_value end as width_in,
                case when height_value != 0 and lower(height_unit) = 'in' then height_value end as height_in,
                case when weight_value = 0 then null
                  when lower(weight_unit) = 'lb' then weight_value 
                  when lower(weight_unit) = 'kg' then weight_value * 2.20462 
                  when lower(weight_unit) = 'g' then weight_value * 1000 * 2.20462 end as weight_lb,
                case when weight_value = 0 then null
                  when lower(weight_unit) = 'kg' then weight_value
                  when lower(weight_unit) = 'g' then weight_value * 1000 
                  when lower(weight_unit) = 'lb' then safe_divide(weight_value, 2.20462) end as weight_kg
              ) as dimensions,
              1 as quantity,
              oi.fulfillment_product_id,
              logistics_shipment_item_group_id as item_group_id -- these join to virtual kits; count distinct would give a count of vks on shipment
            ) 
          ) items
  from    `core_prod_public.logistics_shipments` s
          left outer join `core_prod_public.logistics_shipment_items` i on s.id=i.logistics_shipment_id and ifnull(i._fivetran_deleted,false) is false
          left outer join `core_prod_public.fulfillment_order_items` oi on i.external_id=oi.id and ifnull(oi._fivetran_deleted,false) is false
  where   ifnull(s._fivetran_deleted,false) is false
          and shipping_method_type = 'Appointment'
  group by appointment_id
),
addresses as (
  select  a.id appointment_id,
          struct(
            coalesce(fca.address,aa.address) as street,
            initcap(coalesce(fca.city,aa.city)) as city,
            f.iso_all(
              coalesce(fca.country,aa.country),
              coalesce(fca.postal_code,aa.postal_code),
              coalesce(fca.province,aa.province)
            ) as province_iso,
            coalesce(aic.province_name,initcap(fca.province),initcap(aa.province)) as province,
            upper(coalesce(fca.postal_code,aa.postal_code)) as postal_code,
            coalesce(aic.country_name,initcap(fca.country),initcap(aa.country)) as country,
            aic.country_iso,
            coalesce(fca.latitude, aa.latitude, fca.longitude_latitude_y, aa.longitude_latitude_y) as latitude, 
            coalesce(fca.longitude, fca.longitude_latitude_x, aa.longitude, fca.longitude_latitude_x) as longitude
          ) address,
  from    `core_prod_public.appointments` a
          left outer join `iron-zodiac-336013.core_prod_public.fulfillment_customer_addresses` fca on case when a.location_type='Fulfillment::CustomerAddress' then a.location_id end=fca.id and ifnull(fca._fivetran_deleted,false) is false
          left outer join `core_prod_public.addresses` aa on a.address_id=aa.id and ifnull(aa._fivetran_deleted, false) is false
          left outer join `reference.address_iso_codes` aic on f.iso_all(coalesce(fca.country,aa.country),coalesce(fca.postal_code,aa.postal_code),coalesce(fca.province,aa.province))=aic.province_iso
  where   ifnull(a._fivetran_deleted,false) is false
)
select  a.id, a.number name, 
        cast(job_type as string) type_id, t.name type_name, 
        cast(status as string) status_id, s.name status_name, 
        tb.service_area_id service_area_id, upper(r.name) service_area_name,
        tb.base_date delivery_date, 
        cast(a.delivery_service_level as string) delivery_service_level_id,
        dsl.name delivery_service_level_name,
        case when lower(dsl.name) like '%1-person%' then true end is_one_person_appointment,

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
        case when source_type = 'Fulfillment::Order' then source_id end fulfillment_orders_id,
        i.items,
        aa.address,
        case when delivery_type=1 then true end is_parcel,
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
        and date(a._fivetran_synced) >= current_date()-1
        ;
        
delete from `views.appointments_materialized` 
where   id in (select id from fivetran_esteem_provide_staging.appointments_temp);

delete from `views.appointments_materialized` 
where   id in (select id from `core_prod_public.appointments` where _fivetran_deleted);

insert `views.appointments_materialized` 
select  *
from    fivetran_esteem_provide_staging.appointments_temp;

drop table fivetran_esteem_provide_staging.appointments_temp;