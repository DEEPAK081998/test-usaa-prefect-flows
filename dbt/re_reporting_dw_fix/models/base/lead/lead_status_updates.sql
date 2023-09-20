{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['created'], 'type': 'btree'}
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','lead_status_updates_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(update_id as {{ uuid_formatter() }}) as update_id,
	 cast(lead_id as bigint) as lead_id,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 role,
	 category,
	 status,
	 comment,
	 {{ parse_json('data') }} as data,
	 cast(profile_aggregate_id as {{ uuid_formatter() }}) as profile_aggregate_id,
	 {{ current_date_time() }} as updated_at
FROM {{source('public', 'raw_import_lead_status_updates')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}