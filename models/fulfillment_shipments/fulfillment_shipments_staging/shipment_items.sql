with packages as (
  select * from {{ ref('packages')}}
)

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