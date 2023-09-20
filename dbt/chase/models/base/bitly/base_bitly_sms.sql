{{
  config(
	materialized = 'incremental',
	unique_key = ['bitly_id'],
	tags=['bitly']
	)
}}
with bitly_data AS(
	select distinct 
		custom_link,
		bitlink,
		alltime_clicks,
		date_created,
		title,
		long_url,
		bitly_id
	from 
		chase_bitly_sms 
    {% if is_incremental() %}
    WHERE {{ convert_timezone('date_created','CETDST') }} >= coalesce((select max({{ convert_timezone('date_created','CETDST') }} ) from {{ this }}), '1900-01-01')
    {% endif %}
)
SELECT 
    *
FROM 
    bitly_data
