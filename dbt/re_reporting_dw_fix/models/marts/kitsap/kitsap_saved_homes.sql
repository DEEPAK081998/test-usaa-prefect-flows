{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
    )
}}
-- listings table contains homes saved mapped to pup
WITH listing_join AS(
SELECT 
	listings.user_profile_id AS id,
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
	partner_user_profiles.updated,
	{% if target.type == 'snowflake' %}
	CAST(partner_user_profiles.phones[0].phoneType AS TEXT) AS phonetype,
	CAST(partner_user_profiles.phones[0].phoneNumber AS TEXT) AS phone_number
FROM {{ source('public', 'raw_import_listings') }} listings
	{% else %}
	CAST(partner_user_profiles.phones->0->'phoneType' AS TEXT) AS phonetype,
	CAST(partner_user_profiles.phones->0->'phoneNumber' AS TEXT) AS phone_number
FROM {{ source('public', 'listings') }} listings
    {% endif %}
JOIN {{ ref('partner_user_profiles') }} 
ON listings.user_profile_id = partner_user_profiles.id
WHERE partner_id = 'C124C142-A3BA-45BC-9A6F-493BCF386C0F' 
)
-- remove quotes from strings
SELECT
	id,
	address,
	city,
	state,
	zip,
	latitude,
	longitude,
	type,
	first_name,
	last_name,
	email,
	created,
	updated,
	REPLACE(phonetype, '"','') AS phonetype,
	REPLACE(phone_number, '"','') AS phone_number
FROM listing_join
  {% if is_incremental() %}
    where updated >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
  {% endif %}
