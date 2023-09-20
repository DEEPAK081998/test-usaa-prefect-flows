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
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 role,
	 NULL AS email,
	 NULL AS partner_id,
	 NULL AS profile,
	 cast(profile_aggregate_id as {{ uuid_formatter() }}) as profile_aggregate_id
FROM {{source('public', 'raw_import_user_assignments')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
