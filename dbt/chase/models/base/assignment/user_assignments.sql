{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['partner_id'], 'type': 'hash'},
	  {'columns': ['profile_aggregate_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','user_assignments_pkey') }}")
	]
	, enabled=false
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ convert_timezone('created','CETDST') }} as created,
	 role,
	 email,
	 cast(partner_id as uuid) as partner_id,
	 cast(profile as json) as profile,
	 cast(profile_aggregate_id as uuid) as profile_aggregate_id
FROM {{source('public', 'raw_user_assignments')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
