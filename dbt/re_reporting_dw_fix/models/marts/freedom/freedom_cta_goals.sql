{{
  config(
	materialized = 'table',
    tags=['freedom','ga','cta','goals']
	)
}}
SELECT 
	ga_date,
	ga_medium,
	ga_source,
	MAX(ga_goal13completions) AS teambuilder_completions,
	MAX(ga_goal14completions) AS get_started_cta
FROM {{ source('public', 'freedom_ga_tb_cta_goals') }}
GROUP BY 
	ga_date,
	ga_medium,
	ga_source
ORDER BY ga_date DESC