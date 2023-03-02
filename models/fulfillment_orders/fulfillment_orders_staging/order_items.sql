with oi as 
(
    select * from {{ ref('order_position')}}
)

select  fulfillment_order_id,
          array_agg(struct(
            oi.id,
            fulfillment_product_id,
            quantity,
            position,
            sku,
            fulfillment_stock_id
          )) items
from    oi
        left outer join `core_prod_public.fulfillment_products` fp on oi.fulfillment_product_id=fp.id and fp.deleted_at is null and ifnull(fp._fivetran_deleted,false) is false
where   ifnull(oi._fivetran_deleted, false) is false 
        and oi.deleted_at is null 
        and removed is false
group by fulfillment_order_id