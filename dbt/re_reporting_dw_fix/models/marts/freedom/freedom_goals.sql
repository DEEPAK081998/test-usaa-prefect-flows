{{
  config(
	materialized = 'table',
	tags=['freedom','ga','goals']
	)
}}
SELECT
    ga_date,
    sessioncampaignname,
    sourcemedium,
    homestory_lead_submitted,
    homestory_lead_submitted_pdp,
    viewed_search_results,
    viewed_details_page,
    account_created,
    property_saved
FROM {{ ref('stg_master_goals_union') }}
where partner = 'freedom'
ORDER BY ga_date DESC