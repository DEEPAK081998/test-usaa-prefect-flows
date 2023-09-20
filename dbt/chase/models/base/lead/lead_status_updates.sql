{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['created'], 'type': 'btree'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','lead_status_updates_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(update_id as uuid) as update_id,
	 cast(lead_id as bigint) as lead_id,
	 {{ convert_timezone('created','CETDST') }} as created,
	 role,
	 cast(profile_aggregate_id as uuid) as profile_aggregate_id,
	 --cast(partner_id as uuid) as partner_id,
	 --email,
	 --cast(profile as json) as profile,
	 category,
	 status,
	 comment,
	 cast(data as json) as data,
	 NOW() as updated_at
FROM {{source('public', 'raw_lead_status_updates')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}