{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    enabled=false
    )
}}
WITH

partner_user_profiles_cte AS (
  SELECT 
    id,
    json_array_elements(phones::json)->>'phoneType' as PhoneType,
    json_array_elements(phones::json)->>'phoneNumber' as phoneNumber
  FROM {{ ref('partner_user_profiles') }} 
)
, mobile_phone_cte AS (
  SELECT
    pop.id, min(pop.PhoneNumber) as MobilePhone
  FROM partner_user_profiles_cte pop
  WHERE lower(pop.Phonetype) = 'mobilephone'
  GROUP BY pop.id
)
, office_phone_cte AS (
  SELECT pop.id, min(pop.PhoneNumber) as OfficePhone
  FROM partner_user_profiles_cte pop
  WHERE lower(pop.Phonetype) = 'office'
  GROUP BY pop.id
)
, current_assigments_cte AS (
  SELECT ca.email, count(ca.email) as current_assignment_count
  FROM {{ ref('current_assignments') }}  ca
  WHERE lower(ca.role)='agent'
  GROUP BY ca.email
)
, user_assigments_cte AS (
  SELECT ua.email, max(created) as last_referral_date
  FROM {{ ref('user_assignments') }}  ua
  WHERE lower(ua.role) ='agent'
  GROUP BY ua.email
)

SELECT
pup.id,
pup.first_name as agent_first_name,
pup.last_name as agent_last_name,
pup.eligibility_status as eligibility_status,
pup.brokerage_code as brokerage_code,
aop.OfficePhone as office_phone,
amp.MobilePhone as Agent_phone,
pup.email as Agent_email,
pup.created as Joindate,
rc.RC_Name as RC_name,
rc.RC_Phone as RC_phone,
rc.RC_Email as RC_Email,
lr.last_referral_date,
case
 when aca.current_assignment_count is null then 0
 else aca.current_assignment_count
end as current_assignment_count,
case
 when pup.partner_id = '2DCA0E1B-DAD1-4164-B440-7BC716BDF56D' then 'BHHS'
 when pup.partner_id = 'E6FF474F-ECDF-4B6E-B45C-97B86914468A' then 'HS'
 when pup.partner_id ='8E44B34E-2092-45F5-915C-737C967387F0' then 'RiseNetwork'
 when pup.partner_id ='D5A91FF8-8C2D-4BB9-9D39-571A6EA58DA1' then 'EXP'
 when pup.partner_id ='CE78476B-4538-4BAA-930A-6194F513A536' then 'PlaceNetwork'
 else null
end as HS_Network_Source,
NOW() AS updated_at

FROM {{ ref('partner_user_profiles') }}  pup
JOIN {{ ref('partner_user_roles') }}  pur on pur.id = pup.id
LEFT OUTER JOIN office_phone_cte aop on aop.id = pup.id
LEFT OUTER JOIN mobile_phone_cte amp on amp.id = pup.id
LEFT OUTER JOIN current_assigments_cte aca on aca.email = pup.email
LEFT OUTER JOIN user_assigments_cte lr on lr.email = pup.email
LEFT OUTER JOIN {{ ref('stg_brokerage') }} rc on rc.brokerage_code = pup.brokerage_code
WHERE lower(pur.role) = 'agent'
{% if is_incremental() %}
  and pup.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}