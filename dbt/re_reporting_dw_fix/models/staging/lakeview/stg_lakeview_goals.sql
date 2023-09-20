{{
  config(
	materialized = 'table',
	tags=['lakeview','ga','goals']
	)
}}
WITH
 goals AS(
    SELECT
    ga_date,
    ga_sourceMedium,
    CAST(ga_goal1completions AS INT) as ga_goal1completions,
    CAST(ga_goal2completions AS INT) as ga_goal2completions,
    CAST(ga_goal4completions AS INT) as ga_goal4completions,
    CAST(ga_goal5completions AS INT) as ga_goal5completions,
    CAST(ga_goal7completions AS INT) as ga_goal7completions,
    CAST(ga_goal8completions AS INT) as ga_goal8completions,
    CAST(ga_goal9completions AS INT) as ga_goal9completions,
    CAST(ga_goal10completions AS INT) as ga_goal10completions
    FROM {{ source('public', 'lakeview_ga_goals_final') }}
)
SELECT
    ga_date,
    ga_sourceMedium,
    MAX(ga_goal1completions) homestory_lead_submitted,
    MAX(ga_goal2completions) homestory_lead_submitted_pdp,
    MAX(ga_goal4completions) viewed_search_results,
    MAX(ga_goal5completions) viewed_details_page,
    MAX(ga_goal7completions) logged_in,
    MAX(ga_goal8completions) account_created,
    MAX(ga_goal9completions) property_saved,
    MAX(ga_goal10completions) search_saved
FROM goals
GROUP BY ga_date, ga_sourceMedium
ORDER BY ga_date DESC