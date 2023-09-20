{{
  config(
    materialized = 'table',
    tags=['sofi','ga','goals']
	)
}}
WITH
 goals AS(
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
    CAST(ga_goal13completions AS INT) as ga_goal13completions,
    CAST(ga_goal14completions AS INT) as ga_goal14completions
  FROM {{ source('public', 'sofi_ga_goals') }}
)
SELECT
  ga_date,
  MAX(ga_goal1completions) as homestory_lead_submitted,
  MAX(ga_goal3completions) as lead_submitted_search_landing_page,
  MAX(ga_goal4completions) as account_created,
  MAX(ga_goal5completions) as search_saved,
  MAX(ga_goal6completions) as lead_submitted_pdp,
  MAX(ga_goal7completions) as property_saved,
  MAX(ga_goal8completions) as viewed_pdp,
  MAX(ga_goal9completions) as viewed_search_results,
  MAX(ga_goal13completions) as teambuilder_completion,
  MAX(ga_goal14completions) as am_i_purchase_ready_profile_builder
FROM goals
GROUP BY ga_date
ORDER BY ga_date DESC