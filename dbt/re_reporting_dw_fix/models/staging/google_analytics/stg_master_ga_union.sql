select *
from
{{ ref('stg_master_ga_ua') }}
union
select * 
from
{{ ref('stg_master_ga_ga4') }}