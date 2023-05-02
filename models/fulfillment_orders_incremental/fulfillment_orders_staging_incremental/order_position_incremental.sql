{{ config(materialized='table') }}

select  *, row_number() over(partition by fulfillment_order_id) position
from    `core_prod_public.fulfillment_order_items`