{{
  config(
	materialized = 'table',
	tags=['citizens','ga']
	)
}}
select * 
from {{ ref('stg_master_events_union') }}
where partner = 'citizens'