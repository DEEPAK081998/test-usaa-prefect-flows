WITH

brokerage_coverage AS (
    SELECT
        brokerage_code,
        count(*) as coverage
    FROM {{ ref('brokerage_coverage_zips') }}
    WHERE length(zip)>=5
    GROUP BY brokerage_code
)

, profile_current_leadcount AS (
    SELECT profile_aggregate_id, count(lead_id) as currentLeadCount 
    FROM {{ ref('current_assignments') }}
    GROUP BY profile_aggregate_id
)

, final_cte AS (
    SELECT
        pup.id,
        pup.first_name,
        pup.last_name,
        pup.email,
        pup.phones,
        pup.eligibility_status,
        {% if target.type == 'snowflake' %}
        ARRAY_SIZE(pup.data:coveredAreas) zipcount,
        pup.data:brokerage.brokerageCode::VARCHAR AS brokerageCode,
        {% else %}
        json_array_length(pup.data->'coveredAreas') zipcount,
        pup.data->'brokerage'->>'brokerageCode' AS brokerageCode,
        {% endif %}
        bu.email as RCemail,
        rc.first_name as RCFirstName,
        rc.last_name as RCLastName,
        concat(rc.first_name,' ',rc.last_name) as RCFullName,
        b.full_name as BrokerageName,
        clc.currentLeadCount,
        z.coverage as brokerageCoverage
    FROM {{ ref('partner_user_profiles') }} pup
        JOIN {{ ref('partner_user_roles') }} pur ON pup.id = pur.user_profile_id
        {% if target.type == 'snowflake' %}
        LEFT JOIN {{ ref('brokerages') }} b on pup.data:brokerage.brokerageCode::VARCHAR = b.brokerage_code
        {% else %}
        LEFT JOIN {{ ref('brokerages') }} b on pup.data->'brokerage'->>'brokerageCode' = b.brokerage_code
        {% endif %}
        LEFT JOIN {{ ref('brokerage_user') }} bu on b.brokerage_code = bu.brokerage_code
        LEFT JOIN {{ ref('partner_user_profiles') }} rc on bu.email = rc.email
        LEFT JOIN profile_current_leadcount clc on clc.profile_aggregate_id = pup.aggregate_id
        LEFT OUTER JOIN brokerage_coverage z on b.brokerage_code = z.brokerage_code
    WHERE pur.role = 'AGENT' and z.coverage >0
)
SELECT * FROM final_cte

