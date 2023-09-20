{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['partner_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','current_notes_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated,
	 role,
	 cast(partner_id as {{ uuid_formatter() }}) as partner_id,
	 email,
	 {{ parse_json('profile') }} as profile,
	 {{ parse_json('data') }} as data,
	 private
FROM {{source('public', 'raw_import_current_notes')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}

