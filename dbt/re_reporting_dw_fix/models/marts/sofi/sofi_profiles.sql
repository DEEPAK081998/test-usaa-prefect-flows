WITH
{% if target.type == 'snowflake' %}
partner_user_profiles_cte AS (
  SELECT
    id,
    fp.value:phoneType::VARCHAR as PhoneType,
    fp.value:phoneNumber::VARCHAR as phoneNumber
  FROM {{ ref('partner_user_profiles') }} pup,
  lateral flatten(input => pup.phones) fp
)
{% else %}
partner_user_profiles_cte AS (
  SELECT 
    id,
    json_array_elements(phones)->>'phoneType' as PhoneType,
    json_array_elements(phones)->>'phoneNumber' as phoneNumber
  FROM {{ ref('partner_user_profiles') }}
)
{% endif %}
, mobile_phone_cte AS (
  SELECT
    pop.id, min(pop.PhoneNumber) as clientMobilePhone
  FROM partner_user_profiles_cte pop
  WHERE lower(pop.Phonetype) = 'mobilephone'
  GROUP BY pop.id
)
, office_phone_cte AS (
  SELECT pop.id, min(pop.PhoneNumber) as clientOfficePhone
  FROM partner_user_profiles_cte pop
  WHERE lower(pop.Phonetype) = 'office'
  GROUP BY pop.id
)

, final_cte AS (
  SELECT
  pup.id,
  pup.first_name,
  pup.last_name,
  case when amp.clientMobilePhone is null then aop.clientOfficePhone else amp.clientMobilePhone end as phone,
  pup.email,
  pup.data,
  pup.created,
  pup.updated,
  pup.lead_id as leadID,
  {% if target.type == 'snowflake' %}
  pup.data:agent::VARCHAR as has_agent
  {% else %}
  pup.data->>'agent' as has_agent
  {% endif %}
  FROM {{ ref('stg_sofi_traffic_sources_and_registrations') }} pup
  LEFT JOIN mobile_phone_cte amp on amp.id = pup.id
  LEFT JOIN office_phone_cte aop on aop.id = pup.id
)

SELECT * FROM final_cte
