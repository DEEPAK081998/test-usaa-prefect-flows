WITH

final_cte AS(
	SELECT * FROM {{ ref('stg_sofi_sendgrid') }}
)

SELECT
	*,
	CASE WHEN opens_count > 0 THEN True ELSE False END AS unique_open,
	CASE WHEN clicks_count > 0 THEN True ELSE False END AS unique_click
FROM final_cte