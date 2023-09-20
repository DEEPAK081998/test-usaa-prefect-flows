{{
  config(
	materialized = 'table',
	tags=['pmc','ga','goals']
	)
}}
select ga_date,
  homestory_lead_submitted,
  homestory_lead_submitted_search_landing_page,
  homestory_lead_submitted_about_landing_page,
  account_created,
  search_saved,
  homestory_lead_submitted_pdp,
  homestory_lead_submitted_lo,
  property_saved,
  viewed_detail_page,
  viewed_search_results
  from {{ ref('stg_master_goals_union') }}
  where partner = 'pennymac'