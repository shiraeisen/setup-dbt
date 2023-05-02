{{ config(materialized='table') }}

select  distinct 
          fulfillment_orders_id,
          first_value(external_location_name) over(partition by fulfillment_orders_id order by a.delivery_date) ikea_location_name
from    `views.appointments` a
        left outer join `core_prod_public.fulfillment_orders` fo on a.fulfillment_orders_id=fo.id and fo.deleted_at is null and ifnull(fo._fivetran_deleted,false) is false
        left outer join `reference.organizations_view` o on fo.organization_id=o.id
        left outer join `reference.facilities_view` f on cast(fo.facility as string)=f.id 
where   o.code = 'IKEA'
        and a.type_name in ('Inventory Pickup','Return to Sender')