{{
  config(
    materialized = 'table',
    enabled=false
    )
}}
SELECT
  pz.profile_id,
  pz.zip,
  pz.city,
  pz.state,
  pz.county,
  pup.first_name,
  pup.last_name,
  pup.brokerage_code,
  pup.eligibility_status
FROM {{ ref('profile_coverage_zips') }} pz
  JOIN {{ ref('partner_user_profiles') }}  pup on pz.profile_id = pup.id
