WITH to_cte AS (
    SELECT
        "to",
        ROUND(AVG(rating), 2) AS to_avg_rating,
        COUNT(*) AS total_received
    FROM
        {{ ref('stg_twilio_messages') }}
    GROUP BY
        "to"
),
from_cte AS (
    SELECT
        "from",
        ROUND(AVG(rating), 2) AS from_avg_rating,
        COUNT(*) AS total_sent
    FROM
        {{ ref('stg_twilio_messages') }}
    GROUP BY
        "from"
)
SELECT
    "to" AS twilio_phone_id,
    to_avg_rating,
    from_avg_rating,
    total_received,
    total_sent
FROM
    to_cte
    JOIN from_cte
    ON to_cte."to" = from_cte."from"
