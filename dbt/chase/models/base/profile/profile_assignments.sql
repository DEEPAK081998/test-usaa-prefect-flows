{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}}
SELECT
	 cast(id as bigint) as id,
	 role as role,
	 cast(lead_id as bigint) as lead_id,
	 cast(profile_aggregate_id as uuid) as profile_aggregate_id,
	 {{ convert_timezone('created','CETDST') }} as created
FROM {{source('public', 'raw_profile_assignments')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
