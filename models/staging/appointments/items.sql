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
                  when lower(weight_unit) = 'g' then weight_value * 1000 * 2.20462 
                end as weight_lb,
                case when weight_value = 0 then null
                  when lower(weight_unit) = 'kg' then weight_value
                  when lower(weight_unit) = 'g' then weight_value * 1000 
                  when lower(weight_unit) = 'lb' then safe_divide(weight_value, 2.20462) 
                end as weight_kg
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