select 
	* 
from {{ ref('stg_master_events_union') }}
where partner = 'sofi'