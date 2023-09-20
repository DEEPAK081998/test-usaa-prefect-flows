{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
	  {'columns': ['category'],'type': 'btree'},
      {'columns': ['role'],'type': 'btree'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','current_lead_statuses_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated,
	 role,
	 category,
	 status,
	 comment,
	 {{ parse_json('data') }} as data
FROM {{source('public', 'raw_import_current_lead_statuses')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
