{{
  config(
	materialized = 'incremental',
	unique_key=['ga_date','ga_eventlabel','ga_eventaction','ga_eventcategory','ga_source','ga_adcontent'],
	tags=['fidelity','ga','events']
	)
}}

select 
	* 
from {{ ref('stg_master_events_union') }}
where partner = 'fidelity'