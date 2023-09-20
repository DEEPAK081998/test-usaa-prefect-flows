{{
  config(
    materialized = 'incremental',
    unique_key = ['brokerage_code'],
    indexes=[
	  {'columns': ['brokerage_code'], 'type': 'btree'}
	]
    )
}}
WITH

pop_data AS (
{% if target.type == 'snowflake' %}
    SELECT aggregate_id, fp.value:phoneType::VARCHAR as PhoneType, fp.value:phoneNumber::VARCHAR as phoneNumber
    FROM {{ ref('partner_user_profiles') }} pup,
    lateral flatten(input => pup.phones) fp
{% else %}
    SELECT aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType, json_array_elements(phones)->>'phoneNumber' as phoneNumber
    FROM {{ ref('partner_user_profiles') }}
{% endif %}
)

,aop_cte AS (
    SELECT aggregate_id, min(PhoneNumber) as OfficePhone
    FROM pop_data pop
    WHERE lower(pop.Phonetype) = 'office'
    GROUP BY aggregate_id
)

,amp_cte AS (
    SELECT aggregate_id, min(PhoneNumber) as MobilePhone
    FROM pop_data pop
    WHERE lower(pop.Phonetype) = 'mobilephone'
    GROUP BY aggregate_id
)

SELECT
{% if target.type != 'snowflake' %}
distinct on (b.brokerage_code)
{% endif %}
    b.brokerage_code,
    pup.first_name,
    pup.last_name,
    b.full_name,
    {% if target.type == 'snowflake' %}
    b.data:address::VARCHAR as address,
    {% else %}
    b.data->>'address' as address,
    {% endif %}
    pup.email,
    aop_cte.officePhone,
    amp_cte.mobilephone,
    b.updated,
    b.enabled,
    pur.created as purcreated,
    b.updated as updated_at
FROM {{ ref('brokerages') }} b
JOIN {{ ref('partner_user_relationships') }} pur on b.aggregate_id = pur.parent_profile_uuid
JOIN {{ ref('partner_user_profiles') }} pup on pur.child_profile_uuid = pup.aggregate_id
LEFT OUTER JOIN aop_cte on aop_cte.aggregate_id = pup.aggregate_id
LEFT OUTER JOIN amp_cte on amp_cte.aggregate_id = pup.aggregate_id
{% if is_incremental() %}
WHERE b.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}
{% if target.type == 'snowflake' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY b.brokerage_code ORDER BY b.brokerage_code) = 1
{% endif %}
ORDER BY b.brokerage_code, pur.created DESC