{{ config(materialized='table') }}

with formatted_shipment_tags as 
(
    select * from {{ ref('formatted_tags_incremental')}}
)

SELECT 
o.id fulfillment_order_id, 
          array_agg(struct(
            json_extract_scalar(st) as id,
            stv.name
          )) shipment_tags,
          replace(replace(replace(replace(to_json_string(array_agg(stv.name order by stv.id)),'[',''),']',''),',',' | '),'"','') all_shipment_tags,
          count(*) count_shipment_tags
FROM formatted_shipment_tags o
left outer join unnest(json_extract_array(shipment_tags)) st
left outer join `reference.shipment_tags_view` stv on JSON_EXTRACT_SCALAR(st)=stv.id
group by fulfillment_order_id