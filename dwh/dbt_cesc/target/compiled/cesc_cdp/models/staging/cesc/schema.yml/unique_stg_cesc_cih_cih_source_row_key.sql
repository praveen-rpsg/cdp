
    
    

select
    cih_source_row_key as unique_field,
    count(*) as n_records

from "cdp_meta"."staging"."stg_cesc_cih"
where cih_source_row_key is not null
group by cih_source_row_key
having count(*) > 1


