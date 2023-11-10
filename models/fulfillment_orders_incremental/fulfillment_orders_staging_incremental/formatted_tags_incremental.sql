{{ config(materialized='table') }}

SELECT
    REGEXP_REPLACE(TO_JSON_STRING(shipment_tags), '"', '') AS shipment_tags,
    id,
    external_order_number
FROM
    `core_prod_public.fulfillment_orders`
WHERE ifnull(_fivetran_deleted,false) is false and deleted_at is null