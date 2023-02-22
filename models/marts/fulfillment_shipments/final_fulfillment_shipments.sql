create or replace table fivetran_esteem_provide_staging.fulfillment_shipments_temp as
(
    with packages as
    (
        select * from {{ ref('packages')}}
    ),

    shipment_items as 
    (
        select * from {{ ref('shipment_items')}}
    ),

    --- sources ---
    s as (
        select * from {{ source('core_prod_public', 'logistic_shipments')}}
    ),
    ecs as (
        select * from {{ source('core_prod_public', 'logistics_external_carrier_shipments')}}
    ),
    ecss as (
        select * from {{ source('reference', 'external_carrier_shipment_status_view')}}
    )

    ---------------

    final as
    (
        select  s.id, shipment_number, external_shipment_id,
            facility_id,
            timestamp(s.created_at) created_at, -- reference: https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp
            timestamp(shipped_at) shipped_at,
            items,
            is_return,
            billed is_billed,
            freight is_freight,
            case 
                when external_order_type = 'Fulfillment::Order' then external_order_id 
            end fulfillment_orders_id,
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
    )

    select * from final

);

-- delete from materialized view to prep for insert
delete from `views.fulfillment_shipments_materialized` 
where   id in (select id from `core_prod_public.logistics_shipments` where _fivetran_deleted or shipping_method_type != 'Logistics::ExternalCarrierShipment');

delete from `views.fulfillment_shipments_materialized`
where   id in (select id from `fivetran_esteem_provide_staging.fulfillment_shipments_temp`);

insert `views.fulfillment_shipments_materialized` 
select  *
from    `fivetran_esteem_provide_staging.fulfillment_shipments_temp`;

drop table `fivetran_esteem_provide_staging.fulfillment_shipments_temp`;