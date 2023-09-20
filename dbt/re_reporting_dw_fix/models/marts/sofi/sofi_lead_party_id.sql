WITH user_id_data AS(
	SELECT
		ga_date,
		ga_medium,
		ga_campaign,
		ga_pagepath,
		ga_adcontent,
		CAST(ga_dimension5 AS INT) AS user_id,
		MAX(ga_uniquepageviews)
	FROM {{ source('public', 'source_sofi_user_id') }}
	WHERE ga_dimension5 != 'Not Set'
	GROUP BY
		ga_date,
		ga_medium,
		ga_campaign,
		ga_pagepath,
		ga_adcontent,
		ga_dimension5
	ORDER BY ga_date DESC
	)
SELECT *
FROM user_id_data
FULL JOIN {{ ref('stg_sofi_partner_unpack') }} partner_user_profiles
ON user_id_data.user_id = partner_user_profiles.id
{% if target.type == 'snowflake' %}
WHERE not IS_NULL_VALUE(sofi_data)
{% else %}
WHERE sofi_data IS NOT NULL
{% endif %}