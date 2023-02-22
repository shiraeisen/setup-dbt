-- fivetran sql transformation to maintain fulfillment_orders_materialized

create or replace table fivetran_esteem_provide_staging.fulfillment_orders_temp
as
with order_items as (
  select  fulfillment_order_id,
          array_agg(struct(
            oi.id,
            fulfillment_product_id,
            quantity,
            position,
            sku,
            fulfillment_stock_id
          )) items
  from    (
            select  *, row_number() over(partition by fulfillment_order_id) position
            from    `core_prod_public.fulfillment_order_items`
          ) oi
          left outer join `core_prod_public.fulfillment_products` fp on oi.fulfillment_product_id=fp.id and fp.deleted_at is null and ifnull(fp._fivetran_deleted,false) is false
  where   ifnull(oi._fivetran_deleted, false) is false 
          and oi.deleted_at is null 
          and removed is false
  group by fulfillment_order_id
),
shipment_tags as (
  select  o.id fulfillment_order_id, 
          array_agg(struct(
            st as id,
            name
          )) shipment_tags,
          replace(replace(replace(replace(to_json_string(array_agg(stv.name order by stv.id)),'[',''),']',''),',',' | '),'"','') all_shipment_tags,
          count(*) count_shipment_tags
  from    `core_prod_public.fulfillment_orders` o
          left outer join unnest(json_extract_array(shipment_tags)) st
          left outer join `reference.shipment_tags_view` stv on st=stv.id
  where   ifnull(o._fivetran_deleted,false) is false and o.deleted_at is null 
  group by fulfillment_order_id
),
ikea_location_types as 
(
  select  distinct 
          fulfillment_orders_id,
          first_value(external_location_name) over(partition by fulfillment_orders_id order by a.delivery_date) ikea_location_name
  from    `views.appointments` a
          left outer join `core_prod_public.fulfillment_orders` fo on a.fulfillment_orders_id=fo.id and fo.deleted_at is null and ifnull(fo._fivetran_deleted,false) is false
          left outer join `reference.organizations_view` o on fo.organization_id=o.id
          left outer join `reference.facilities_view` f on cast(fo.facility as string)=f.id 
  where   o.code = 'IKEA'
          and a.type_name in ('Inventory Pickup','Return to Sender')
)
select  o.id, 
        timestamp(o.created_at) created_at, 
        cancelled is_cancelled,
        timestamp(o.cancelled_at) cancelled_at,
        order_id name, 
        external_order_number external_order_name, 
        case when trim(notes) = '' then null else notes end notes,
        case when trim(internal_notes) = '' then null else internal_notes end internal_notes,
        items,
        st.shipment_tags,
        st.all_shipment_tags,
        st.count_shipment_tags,
        struct(
          fca.address as street,
          initcap(city) as city, 
          contact_name as name,
          f.iso_all(country, postal_code, province) as province_iso,
          coalesce(aic.province_name,initcap(province)) as province,
          upper(postal_code) as postal_code,
          coalesce(aic.country_name,initcap(country)) as country,
          country_iso,
          coalesce(latitude, longitude_latitude_y) as latitude, 
          coalesce(longitude, longitude_latitude_x) as longitude
        ) shipping_address,
        insurance_value,
        on_hold is_on_hold,
        o.status status_id, os.name status_name,
        o.stock_status stock_status_id, oss.name stock_status_name,
        cast(o.facility as string) facility_id,
        fc.name customer_name,
        case when trim(fc.email_address) != '' then lower(trim(fc.email_address)) end customer_email,
        o.organization_id,
        ilt.ikea_location_name,
        case when ends_with(ilt.ikea_location_name,'STO') then 'Store'
          when ends_with(ilt.ikea_location_name,'CDC') then 'CDC' end ikea_location_type,
        cast(platform as string) platform_id,
        op.name platform_name,
        timestamp(latest_booking_email_sent_at) latest_booking_email_sent_at,
        o.shipping_service
from    `core_prod_public.fulfillment_orders` o
        left outer join order_items oi on o.id=oi.fulfillment_order_id
        left outer join shipment_tags st on o.id=st.fulfillment_order_id
        left outer join ikea_location_types ilt on o.id=ilt.fulfillment_orders_id
        left outer join `core_prod_public.fulfillment_customers` fc on o.fulfillment_customer_id=fc.id and fc.deleted_at is null and ifnull(fc._fivetran_deleted,false) is false
        left outer join `core_prod_public.fulfillment_customer_addresses` fca on o.fulfillment_customer_address_id=fca.id and fca.deleted_at is null and ifnull(fca._fivetran_deleted,false) is false
        left outer join `reference.facilities_view` f on cast(o.facility as string)=f.id
        left outer join `reference.order_status_view` os on cast(o.status as string)=os.id
        left outer join `reference.order_stock_status_view` oss on cast(o.stock_status as string) = oss.id
        left outer join `reference.address_iso_codes` aic on f.iso_all(country, postal_code, province)=aic.province_iso
        left outer join `reference.order_platforms` op on cast(o.platform as string)=op.id
where   ifnull(o._fivetran_deleted,false) is false
        and o.deleted_at is null
        and date(o._fivetran_synced) >= current_date()-1
        ;

-- to confirm; does _fivetran_deleted date match _fivetran synced?
delete from `views.fulfillment_orders_materialized`
where   id in (select id from `fivetran_esteem_provide_staging.fulfillment_orders_temp`);

delete from `views.fulfillment_orders_materialized` 
where   id in (select id from `core_prod_public.fulfillment_orders` where _fivetran_deleted or deleted_at is not null);

insert `views.fulfillment_orders_materialized`
select  *
from    fivetran_esteem_provide_staging.fulfillment_orders_temp;

drop table fivetran_esteem_provide_staging.fulfillment_orders_temp;