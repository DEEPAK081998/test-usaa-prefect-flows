{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
	enabled=false
    )
}}
SELECT
	{{ dbt_utils.surrogate_key(
        ['user_profile_id',
        'address',
        'city']
    )}} AS id,
	listings.user_profile_id,
	listings.address,
	listings.city,
	listings.state,
	listings.zip,
	listings.latitude,
	listings.longitude,
	listings.type,
	partner_user_profiles.first_name,
	partner_user_profiles.last_name,
	partner_user_profiles.email,
	partner_user_profiles.created,
	partner_user_profiles.updated as updated_at,
    partner_user_profiles.partner_id,
	CAST(partner_user_profiles.phones->0->>'phoneType' AS TEXT) AS phonetype,
	CAST(partner_user_profiles.phones->0->>'phoneNumber' AS TEXT) AS phone_number
FROM {{ source('public', 'listings') }} listings
JOIN {{ ref('partner_user_profiles') }} 
ON listings.user_profile_id = partner_user_profiles.id
{% if is_incremental() %}
WHERE partner_user_profiles.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}