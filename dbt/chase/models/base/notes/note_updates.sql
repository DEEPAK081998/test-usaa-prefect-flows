{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
	  {'columns': ['created'], 'type': 'btree'},

	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','note_updates_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ convert_timezone('created','CETDST') }} as created,
	 role,
	 --cast(partner_id as uuid) as partner_id,
	 --email,
	 --cast(profile as json) as profile,
	 cast(data as json) as data,
	 cast(profile_aggregate_id as UUID) AS profile_aggregate_id
FROM {{source('public', 'raw_note_updates')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
