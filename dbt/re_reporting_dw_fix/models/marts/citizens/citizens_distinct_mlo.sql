WITH mlo_cte AS (
    SELECT
        concat(pup.first_name, ' ', pup.last_name) AS mlo_name,
        pup.email AS mlo_email,
        ROW_NUMBER() over (
        {% if target.type == 'snowflake' %}
            PARTITION BY pup.data:nmlsid::VARCHAR
            ORDER BY
                pup.updated DESC
        ) AS row_id,
        data:jobTitle::VARCHAR AS job_title
        {% else %}
            PARTITION BY pup.data ->> 'nmlsid'
            ORDER BY
                pup.updated DESC
        ) AS row_id,
        data::json->>'jobTitle' AS job_title
        {% endif %}
    FROM
        {{ ref('partner_user_profiles') }} pup
        LEFT JOIN {{ ref('partner_user_roles') }} pur ON pur.user_profile_id = pup.id
    WHERE
        pur.role = 'MLO'
        AND pup.email LIKE '%citizens%'
)
SELECT
    mlo_name,
    mlo_email
FROM
    mlo_cte
WHERE
    row_id = 1
    AND (job_title != 'HM Loan Officer Assistant' OR job_title {{ is_null_operator() }})
ORDER BY
    mlo_name