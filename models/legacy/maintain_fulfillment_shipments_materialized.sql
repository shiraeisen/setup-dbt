-- fivetran sql transformation to maintain fulfillment_shipments_materialized

create or replace table fivetran_esteem_provide_staging.fulfillment_shipments_temp
as
with packages as (
  select  psi.logistics_shipment_item_id,
          array_agg(struct(
            p.id, package_id as name, 
            case when lower(trim(height_unit)) = 'in' then height_value end as height_in,
            case when lower(trim(length_unit)) = 'in' then length_value end as length_in,
            case when lower(trim(width_unit)) = 'in' then width_value end as width_in,
            case when lower(trim(weight_unit)) = 'lb' then weight_value end as weight_lb,
            replace(replace(replace(replace(initcap(replace(trim(external_platform_service_code),'_',' ')),'Ups','UPS'),'Fedex','FedEx'),'Usa','USA'),'dhl','DHL') as carrier_service_code,
            ecsl.tracking_number,
            label_id,
            tracking_url
          )) packages
  from    `core_prod_public.logistics_shipment_packages_shipment_items` psi
          left outer join `core_prod_public.logistics_shipment_packages` p on psi.logistics_shipment_package_id=p.id and p._fivetran_deleted is false and p.failed_reason is null
          left outer join `core_prod_public.logistics_external_carrier_shipping_labels` ecsl on p.logistics_external_carrier_shipping_label_id=ecsl.id and ecsl._fivetran_deleted is false and ecsl.voided_at is null
  where   psi._fivetran_deleted is false
  group by psi.logistics_shipment_item_id
),
shipment_items as (
  select  logistics_shipment_id,
          array_agg(struct(
            si.id,
            case when item_identifier_type = 0 then item_identifier_value end as sku,
            oi.fulfillment_product_id,
            logistics_shipment_item_group_id as item_group_id, -- these join to virtual kits; count distinct would give a count of vks on shipment
            1 as quantity,
            packages
          )) items,
          max(case when action = 8 then true else false end) is_return
  from    `core_prod_public.logistics_shipment_items` si
          left outer join `core_prod_public.logistics_shipment_item_actions` sia on si.id=sia.logistics_shipment_item_id  and sia._fivetran_deleted is false
          left outer join `reference.shipment_item_action_view` a on cast(sia.action as string)=a.id
          left outer join packages p on si.id=p.logistics_shipment_item_id 
          left outer join `core_prod_public.fulfillment_order_items` oi on si.external_id=oi.id and ifnull(oi._fivetran_deleted,false) is false
  where   si._fivetran_deleted is false -- only consider rows that are not deleted
          -- and ifnull(a.name, 'null') != 'Reverse Logistics (External Carrier)' -- these are actually returns; included as of 2022-9-28
  group by logistics_shipment_id
)
select  s.id, shipment_number, external_shipment_id,
        facility_id,
        timestamp(s.created_at) created_at, -- reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp
        timestamp(shipped_at) shipped_at,
        items,
        is_return,
        billed is_billed,
        freight is_freight,
        case when external_order_type = 'Fulfillment::Order' then external_order_id end fulfillment_orders_id,
        logistics_carrier_id logistics_carriers_id,
        organization_id,
        ecs.carrier_id external_carrier_shipment_carrier_id,
        cast(ecs.status  as string) status_id, ecss.name status_name 
from    `core_prod_public.logistics_shipments` s
        inner join shipment_items si on s.id=si.logistics_shipment_id
        inner join `core_prod_public.logistics_external_carrier_shipments` ecs on s.shipping_method_id=ecs.id and ecs._fivetran_deleted is false
        left outer join `iron-zodiac-336013.reference.external_carrier_shipment_status_view` ecss on ecs.status=ecss.id
where   s._fivetran_deleted is false
        and shipping_method_type = 'Logistics::ExternalCarrierShipment' -- consider only external carrier shipment       
        and (date(s._fivetran_synced) >= current_date()-1 or date(ecs._fivetran_synced) >= current_date()-1) ;

-- delete from materialized view to prep for insert
delete from `views.fulfillment_shipments_materialized` 
where   id in (select id from `core_prod_public.logistics_shipments` where _fivetran_deleted or shipping_method_type != 'Logistics::ExternalCarrierShipment');

delete from `views.fulfillment_shipments_materialized`
where   id in (select id from `fivetran_esteem_provide_staging.fulfillment_shipments_temp`);

insert `views.fulfillment_shipments_materialized` 
select  *
from    `fivetran_esteem_provide_staging.fulfillment_shipments_temp`;

drop table `fivetran_esteem_provide_staging.fulfillment_shipments_temp`;