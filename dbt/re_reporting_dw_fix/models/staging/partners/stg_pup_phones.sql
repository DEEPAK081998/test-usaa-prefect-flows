WITH pup_raw AS (
    SELECT
        id AS pup_id,
        phone AS original_phone,
        {% if target.type == 'snowflake' %}
        ARRAY_SIZE(phones) AS num_phones,
        CASE
            WHEN phones[0].phoneType::VARCHAR = 'mobilePhone' THEN phones[0].phoneNumber::VARCHAR
            ELSE phones[1].phoneNumber::VARCHAR
        END AS mobile_phone,
        CASE
            WHEN phones[0].phoneType::VARCHAR = 'officePhone' THEN phones[0].phoneNumber::VARCHAR
            ELSE phones[1].phoneNumber::VARCHAR
        END AS office_phone,*,
        {% else %}
        json_array_length("phones") AS num_phones,
        CASE
            WHEN phones -> 0 ->> 'phoneType' = 'mobilePhone' THEN phones -> 0 ->> 'phoneNumber'
            ELSE phones -> 1 ->> 'phoneNumber'
        END AS mobile_phone,
        CASE
            WHEN phones -> 0 ->> 'phoneType' = 'officePhone' THEN phones -> 0 ->> 'phoneNumber'
            ELSE phones -> 1 ->> 'phoneNumber'
        END AS office_phone,*,
        {% endif %}
        {{ current_date_time() }} AS _updated_at
    FROM
        {{ ref('partner_user_profiles') }}
),
pup_norm AS(
    SELECT
        pup_id,
        {{ normalize_phone_number('original_phone') }} AS pup_original_phone_id,
        {{ normalize_phone_number('mobile_phone') }} AS pup_mobile_phone_id,
        {{ normalize_phone_number('office_phone') }} AS pup_office_phone_id,
        {{ current_date_time() }} AS _updated_at
    FROM
        pup_raw
)
SELECT
    pup_id,
    CASE
        WHEN pup_original_phone_id {{ is_null_operator() }}
        AND pup_mobile_phone_id {{ not_null_operator() }} THEN pup_mobile_phone_id
        WHEN pup_original_phone_id {{ is_null_operator() }}
        AND pup_office_phone_id {{ not_null_operator() }} THEN pup_office_phone_id
        ELSE pup_original_phone_id
    END AS pup_phone_id
FROM
    pup_norm
