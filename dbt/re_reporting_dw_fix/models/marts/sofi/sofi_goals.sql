{{
  config(
    materialized = 'table',
    tags=['sofi','ga','goals']
	)
}}
select ga_date,
  homestory_lead_submitted,
  homestory_lead_submitted_search_landing_page,
  account_created,
  search_saved,
  lead_submitted_pdp,
  property_saved,
  viewed_pdp,
  viewed_search_results,
  teambuilder_completion,
  am_i_purchase_ready_profile_builder
  from {{ ref('stg_master_goals_union') }}
  where partner = 'sofi'