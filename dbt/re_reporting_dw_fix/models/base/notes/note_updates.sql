{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['partner_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','note_updates_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 role,
	 cast(partner_id as {{ uuid_formatter() }}) as partner_id,
	 email,
	 {{ parse_json('profile') }} as profile,
	 {{ parse_json('data') }} as data
FROM {{source('public', 'raw_import_note_updates')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
