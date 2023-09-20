{{
  config(
	materialized = 'table',
	tags=['lakeview','ga','goals']
	)
}}
select 
ga_date,
sourcemedium as ga_sourcemedium,
homestory_lead_submitted,
homestory_lead_submitted_pdp,
viewed_search_results,
viewed_details_page,
logged_in,
account_created,
property_saved,
search_saved
from {{ ref('stg_master_goals_union') }}
where partner = 'lakeview'