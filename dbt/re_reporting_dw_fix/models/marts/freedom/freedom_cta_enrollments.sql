{{
  config(
	materialized = 'table',
    tags=['freedom','ga','cta','adcontent']
	)
}}
WITH normalize AS(
SELECT
	ga_date,
	ga_medium,
	ga_source,
	CAST(ga_goal13completions AS INT) as ga_goal13completions,
	CAST(ga_goal14completions AS INT) as ga_goal14completions
FROM {{ source('public', 'freedom_ga_tb_cta_goals') }}
),
--deduplication
dedup AS(
SELECT
	ga_date,
	ga_medium,
	ga_source,
	MAX(ga_goal13completions) AS teambuilder_completions,
	MAX(ga_goal14completions) AS enrollments_cta
FROM normalize
GROUP BY
	ga_date,
	ga_medium,
	ga_source
ORDER BY ga_date DESC
)
SELECT * FROM dedup