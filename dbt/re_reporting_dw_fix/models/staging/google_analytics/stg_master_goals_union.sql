select *
from
{{ ref('stg_master_goals_ua') }}
union
select * 
from
{{ ref('stg_master_goals_ga4') }}