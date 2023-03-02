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