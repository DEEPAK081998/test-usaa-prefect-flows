{{
  config(
    materialized = 'table',
    tags=['citizens','ga','goals']
	)
}}
select ga_date,
  homestory_lead_submitted,
  account_created,
  homestory_lead_submitted_pdp,
  property_saved,
  viewed_details_page,
  viewed_search_results
  from {{ ref('stg_master_goals_union') }}
  where partner = 'citizens'