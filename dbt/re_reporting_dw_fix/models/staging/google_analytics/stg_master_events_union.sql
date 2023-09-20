select *
from
{{ ref('stg_master_events_ua') }}
union
select * 
from
{{ ref('stg_master_events_ga4') }}