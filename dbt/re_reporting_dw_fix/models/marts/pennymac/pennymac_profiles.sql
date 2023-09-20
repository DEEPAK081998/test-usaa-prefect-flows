{{
  config(
    materialized = 'table'
    )
}}
with
{# ADD THIS CTE AS STG TABLE #}
 pop_numbers AS (
    SELECT 
    {% if target.type =='snowflake'%}
        p.aggregate_id, 
        f.value:phoneType::STRING as PhoneType,
        f.value:phoneNumber::STRING as phoneNumber
    FROM partner_user_profiles p,
    LATERAL FLATTEN(input => parse_json(p.phones)) f
    {% else %}
        aggregate_id, 
        json_array_elements(phones)->>'phoneType' as PhoneType,
        json_array_elements(phones)->>'phoneNumber' as phoneNumber
    FROM {{ ref('partner_user_profiles') }}
    {% endif %}
)
, amp_cte AS (
    SELECT 
        aggregate_id,
        min(PhoneNumber) as agentMobilePhone
    FROM pop_numbers
    WHERE lower(Phonetype) = 'mobilephone'
    GROUP BY aggregate_id
)
, aop_cte AS (
    SELECT 
        aggregate_id,
        min(PhoneNumber) as agentOfficePhone
    FROM pop_numbers
    WHERE lower(Phonetype) = 'office'
    GROUP BY aggregate_id
)

, agent_cte AS (
    SELECT 
        DISTINCT
        ca.profile_aggregate_id,
        pup.aggregate_id,
        pup.first_name,
        pup.last_name,
        pup.email,
        coalesce(amp.agentMobilePhone,aop.agentOfficePhone) as phone,
        pup.brokerage_code,
        {% if target.type == 'snowflake' %}
        pup.data:brokerage:fullName::TEXT as brokerage_name,
        pup.data:profileInfo:stateLicences[0] as license_nmlsid,
        {% else %}
        pup.data->'brokerage'->>'fullName' as brokerage_name,
        pup.data->'profileInfo'->'stateLicences'->>0 as license_nmlsid,
        {% endif %}
        pup.created,
        pup.updated,
        ca.role
    FROM {{ ref('current_assignments') }} ca 
    LEFT JOIN {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = ca.profile_aggregate_id
    LEFT JOIN {{ ref('leads_data_v3') }} t1 on t1.id = ca.lead_id 
    LEFT OUTER JOIN amp_cte  amp on amp.aggregate_id = pup.aggregate_id
    LEFT OUTER JOIN aop_cte  aop on aop.aggregate_id = pup.aggregate_id
    WHERE ca.role = 'AGENT' and t1.bank_name = 'PennyMac'
)
, customer_cte AS (
    SELECT
    {% if target.type == 'snowflake'%}
        pup.aggregate_id,
        f.value:phoneNumber::STRING as phone
    FROM {{ ref('partner_user_profiles') }}  pup
    LEFT JOIN {{ ref('partner_user_roles') }} pur
    ON pur.user_profile_id = pup.id
    -- Use LATERAL FLATTEN to extract data from the JSON array
    , LATERAL FLATTEN(input => parse_json(pup.phones)) f
    {% else %}
        pup.aggregate_id,
        json_array_elements(pup.phones)->>'phoneNumber' as phone
    FROM {{ ref('partner_user_profiles') }} pup
    LEFT JOIN {{ ref('partner_user_roles') }} pur
    on pur.user_profile_id = pup.id 
    {% endif %}
)
, final_cte AS (
    SELECT 
        pup.aggregate_id,
        pup.first_name,
        pup.last_name,
        pup.email,
        pup.created,
        pup.updated,
        coalesce(cc.phone,t1.client_phone) as phone,
        pur.role,
        '' as brokerage_code,
        '' as brokerage_name,
        '' as license_nmlsid
    FROM {{ ref('partner_user_profiles') }} pup 
    LEFT JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id 
    LEFT JOIN {{ ref('leads_data_v3') }} t1 on t1.client_email = pup.email 
    LEFT JOIN customer_cte cc on cc.aggregate_id = pup.aggregate_id
    WHERE pur.role = 'CUSTOMER' and pup.partner_id = 'E2A46D0A-6544-4116-8631-F08D749045AC'

    UNION 
    {% if target.type == 'snowflake' %}
    SELECT 
        pup.aggregate_id,
        pup.first_name,
        pup.last_name,
        pup.email,
        pup.created,
        pup.updated,
        f.value:phoneNumber::STRING as phone,
        pur.role,
        '' as brokerage_code,
        '' as brokerage_name,
        pup.data:nmlsid::TEXT as license_nmlsid
    FROM {{ ref('partner_user_profiles') }} pup
    LEFT JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
    -- Use LATERAL FLATTEN to extract data from the JSON array
    , LATERAL FLATTEN(input => parse_json(pup.phones)) f
    WHERE pur.role = 'MLO' and pup.partner_id ILIKE 'E2A46D0A-6544-4116-8631-F08D749045AC'
    {% else %}
    SELECT 
        pup.aggregate_id,
        pup.first_name,
        pup.last_name,
        pup.email,
        pup.created,
        pup.updated,
        json_array_elements(pup.phones)->>'phoneNumber' as phone,
        pur.role,
        '' as brokerage_code,
        '' as brokerage_name,
        pup.data->>'nmlsid' as license_nmlsid
    FROM {{ ref('partner_user_profiles') }} pup
    LEFT JOIN {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
    WHERE pur.role = 'MLO' and pup.partner_id = 'E2A46D0A-6544-4116-8631-F08D749045AC'
    {% endif %}
    UNION

    SELECT
        ac.aggregate_id,
        ac.first_name,
        ac.last_name,
        ac.email,
        ac.created,
        ac.updated,
        ac.phone,
        ac.role,
        ac.brokerage_code,
        ac.brokerage_name,
        ac.license_nmlsid
    FROM agent_cte ac
)
SELECT * FROM final_cte