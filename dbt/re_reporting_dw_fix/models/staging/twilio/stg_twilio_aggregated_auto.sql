WITH aggregation_cte AS (
    --average_rating
    SELECT
        {{  special_column_name_formatter('automated_phone') }},
        ROUND(AVG(rating), 2) AS avg_rating
    FROM
        {{ ref('stg_twilio_messages') }}
    GROUP BY
        {{  special_column_name_formatter('automated_phone') }}
    ORDER BY
        avg_rating ASC
),
total_received_cte AS (
    --total_received
    SELECT
        {{  special_column_name_formatter('automated_phone') }},
        COUNT(*) AS total_received
    FROM
        {{ ref('stg_twilio_messages') }}
    GROUP BY
        {{  special_column_name_formatter('automated_phone') }},
        status
    HAVING
        status = 'received'
    ORDER BY
        COUNT(*) DESC
),
total_sent_cte AS (
    --total_sent
    SELECT
        {{  special_column_name_formatter('automated_phone') }},
        COUNT(*) AS total_sent
    FROM
        {{ ref('stg_twilio_messages') }}
    GROUP BY
        {{  special_column_name_formatter('automated_phone') }},
        status
    HAVING
        status = 'sent'
    ORDER BY
        COUNT(*) DESC
)
SELECT
    *
FROM
    aggregation_cte
    LEFT JOIN total_received_cte USING({{  special_column_name_formatter('automated_phone') }})
    LEFT JOIN total_sent_cte USING({{  special_column_name_formatter('automated_phone') }})
WHERE
    total_received {{ not_null_operator() }}
    OR total_sent {{ not_null_operator() }}
