{{
  config(
	materialized = 'table',
	tags=['pmc','ga','goals']
	)
}}
WITH goals AS(
SELECT
ga_date,
CAST(ga_goal1completions AS INT) as ga_goal1completions,
CAST(ga_goal3completions AS INT) as ga_goal3completions,
CAST(ga_goal4completions AS INT) as ga_goal4completions,
CAST(ga_goal5completions AS INT) as ga_goal5completions,
CAST(ga_goal6completions AS INT) as ga_goal6completions,
CAST(ga_goal7completions AS INT) as ga_goal7completions,
CAST(ga_goal8completions AS INT) as ga_goal8completions,
CAST(ga_goal9completions AS INT) as ga_goal9completions,
CAST(ga_goal10completions AS INT) as ga_goal10completions,
CAST(ga_goal11completions AS INT) as ga_goal11completions
FROM {{ source('public', 'pennymac_ga_goals') }}
)
SELECT 
ga_date,
MAX(ga_goal1completions) homestory_lead_submitted,
MAX(ga_goal3completions) homestory_lead_submitted_search_landing_page,
MAX(ga_goal4completions) account_created,
MAX(ga_goal5completions) search_saved,
MAX(ga_goal6completions) homestory_lead_submitted_pdp,
MAX(ga_goal7completions) property_saved,
MAX(ga_goal8completions) viewed_detail_page,
MAX(ga_goal9completions) viewed_search_results,
MAX(ga_goal10completions) homestory_lead_submitted_about_landing_page,
MAX(ga_goal11completions) homestory_lead_submitted_lo

FROM goals
GROUP BY ga_date
ORDER BY ga_date DESC