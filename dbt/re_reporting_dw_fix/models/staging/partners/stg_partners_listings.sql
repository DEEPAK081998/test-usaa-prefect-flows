{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}}

WITH source_cte AS(
	SELECT {{ dbt_utils.generate_surrogate_key(
        ['user_profile_id',
        'address',
        'city'] 
    )}} AS source_id,
	ROW_NUMBER() OVER (PARTITION BY user_profile_id, address, city ORDER BY updated DESC) as row_number,
	* FROM {{ ref('listings') }} 
),
dedup_cte AS(
	SELECT 
		*
	FROM
		source_cte
	WHERE
		row_number = 1
)
SELECT
	listings.source_id as id,
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
    {% if target.type == 'snowflake' %}
	CAST(partner_user_profiles.phones[0].phoneType::VARCHAR AS TEXT) AS phonetype,
	CAST(partner_user_profiles.phones[0].phoneNumber::VARCHAR AS TEXT) AS phone_number
    {% else %}
	CAST(partner_user_profiles.phones->0->>'phoneType' AS TEXT) AS phonetype,
	CAST(partner_user_profiles.phones->0->>'phoneNumber' AS TEXT) AS phone_number
	{% endif %}
FROM dedup_cte  listings
JOIN {{ ref('partner_user_profiles') }}
ON listings.user_profile_id = partner_user_profiles.id
{% if is_incremental() %}
WHERE partner_user_profiles.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}