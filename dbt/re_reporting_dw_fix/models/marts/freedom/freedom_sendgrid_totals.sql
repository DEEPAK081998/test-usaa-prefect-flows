WITH
 agg_freedom AS(
	SELECT
		to_email,
		MIN(first_event_time) AS first_email_event,
		SUM(opens_count) AS total_opens,
		SUM(clicks_count) AS total_clicks,
		MAX(inviting_la) AS inviting_la,
		COUNT(email_no) AS email_sent
	FROM {{ ref('fact_freedom_sendgrid') }}
	GROUP BY to_email
)
SELECT *,
	CASE WHEN total_opens > 0 THEN True ELSE False END AS unique_campaign_open,
	CASE WHEN total_clicks > 0 THEN True ELSE False END AS unique_campaign_click
FROM agg_freedom