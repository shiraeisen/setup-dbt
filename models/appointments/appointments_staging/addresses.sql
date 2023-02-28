select  a.id appointment_id,
        struct(
          coalesce(fca.address,aa.address) as street,
          initcap(coalesce(fca.city,aa.city)) as city,
          f.iso_all(
            coalesce(fca.country,aa.country),
            coalesce(fca.postal_code,aa.postal_code),
            coalesce(fca.province,aa.province)
          ) as province_iso,
          coalesce(aic.province_name,initcap(fca.province),initcap(aa.province)) as province,
          upper(coalesce(fca.postal_code,aa.postal_code)) as postal_code,
          coalesce(aic.country_name,initcap(fca.country),initcap(aa.country)) as country,
          aic.country_iso,
          coalesce(fca.latitude, aa.latitude, fca.longitude_latitude_y, aa.longitude_latitude_y) as latitude, 
          coalesce(fca.longitude, fca.longitude_latitude_x, aa.longitude, fca.longitude_latitude_x) as longitude
        ) address,
from    `core_prod_public.appointments` a
        left outer join `iron-zodiac-336013.core_prod_public.fulfillment_customer_addresses` fca on case when a.location_type='Fulfillment::CustomerAddress' then a.location_id end=fca.id and ifnull(fca._fivetran_deleted,false) is false
        left outer join `core_prod_public.addresses` aa on a.address_id=aa.id and ifnull(aa._fivetran_deleted, false) is false
        left outer join `reference.address_iso_codes` aic on f.iso_all(coalesce(fca.country,aa.country),coalesce(fca.postal_code,aa.postal_code),coalesce(fca.province,aa.province))=aic.province_iso
where   ifnull(a._fivetran_deleted,false) is false