select  psi.logistics_shipment_item_id,
        array_agg(struct(
            p.id, 
            package_id as name, 
            case 
                when lower(trim(height_unit)) = 'in' then height_value 
            end as height_in,
            case 
                when lower(trim(length_unit)) = 'in' then length_value 
            end as length_in,
            case 
                when lower(trim(width_unit)) = 'in' then width_value 
            end as width_in,
            case 
                when lower(trim(weight_unit)) = 'lb' then weight_value 
            end as weight_lb,
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