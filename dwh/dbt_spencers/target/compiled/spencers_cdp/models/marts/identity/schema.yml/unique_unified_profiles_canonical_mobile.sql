
    
    

select
    canonical_mobile as unique_field,
    count(*) as n_records

from "cdp_meta"."silver_identity"."unified_profiles"
where canonical_mobile is not null
group by canonical_mobile
having count(*) > 1


