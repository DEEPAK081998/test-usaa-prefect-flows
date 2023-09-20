{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','leads_data_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ parse_json('data') }} as data,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated
FROM {{source('public', 'raw_import_leads_data')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
