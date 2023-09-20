WITH combined_phones_cte AS (
SELECT DISTINCT "to" AS phone FROM {{ ref('base_twilio_messages') }}
UNION
SELECT DISTINCT "from" AS phone FROM {{ ref('base_twilio_messages') }}
),
distinct_cte AS (SELECT DISTINCT phone as twilio_phone_id FROM combined_phones_cte)
SELECT
	twilio_phone_id,
	pup.pup_id,
	leads.lead_id
FROM distinct_cte
LEFT JOIN {{ ref('stg_pup_phones') }} AS pup ON twilio_phone_id = pup.pup_phone_id
LEFT JOIN {{ ref('stg_lead_phones_normalized') }} AS leads ON twilio_phone_id = leads.lead_phone_id