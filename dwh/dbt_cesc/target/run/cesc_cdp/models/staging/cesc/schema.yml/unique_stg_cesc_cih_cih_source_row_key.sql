select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    cih_source_row_key as unique_field,
    count(*) as n_records

from "cdp_meta"."staging"."stg_cesc_cih"
where cih_source_row_key is not null
group by cih_source_row_key
having count(*) > 1



      
    ) dbt_internal_test