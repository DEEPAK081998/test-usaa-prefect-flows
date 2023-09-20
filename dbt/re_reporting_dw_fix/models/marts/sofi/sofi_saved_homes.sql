{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
    )
}}
WITH listing_join AS(
	SELECT * FROM {{ ref('stg_partners_listings') }}
	WHERE partner_id = '77daccc1-1178-4fd2-b95e-4f291476cbd9'
)
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
	updated_at,
	REPLACE(phonetype, '"','') AS phonetype,
	REPLACE(phone_number, '"','') AS phone_number
FROM listing_join
{% if is_incremental() %}
WHERE updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}

