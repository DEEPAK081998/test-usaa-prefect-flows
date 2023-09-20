WITH
totals_cte AS(
	SELECT
		to_email,
		MIN(first_event_time) AS first_email_event,
		SUM(opens_count) AS total_opens,
		SUM(clicks_count) AS total_clicks,
		COUNT(email_no) AS email_sent
	FROM {{ ref('stg_pmc_sendgrid') }}
	GROUP BY
		to_email
)
SELECT
	*,
	CASE WHEN total_opens > 0 THEN True ELSE False END AS unique_campaign_open,
	CASE WHEN total_clicks > 0 THEN True ELSE False END AS unique_campaign_click
FROM totals_cte