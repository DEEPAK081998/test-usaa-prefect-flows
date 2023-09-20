WITH twilio_clean_ratings_cte AS (
SELECT
	"from",
	"to",
	{{  special_column_name_formatter('status') }},
	{{  special_column_name_formatter('direction') }},
	length(body) AS body_len,
	CASE
		WHEN length(body) = 1
		AND body IN ('0', '1', '2', '3', '4', '5')
	THEN body::int
		ELSE NULL
	END AS rating,
	{{  special_column_name_formatter('date_sent') }},
	{{  special_column_name_formatter('body') }},
	{{  special_column_name_formatter('num_media') }},
	{{  special_column_name_formatter('sid') }}
FROM
	{{ ref('base_twilio_messages') }}
WHERE
	status IN ('delivered', 'received')
),
twilio_phone_identified_cte AS (
SELECT
	CASE
		WHEN direction = 'inbound' THEN "from"
		ELSE "to"
	END AS client_phone,
	CASE
		WHEN direction = 'outbound-api' THEN "from"
		ELSE "to"
	END AS automated_phone,
	*
FROM
	twilio_clean_ratings_cte)
SELECT
	*
FROM
	twilio_phone_identified_cte
ORDER BY date_sent DESC