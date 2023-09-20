{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['aggregate_id'], 'type': 'hash'},
	  {'columns': ['created'], 'type': 'btree'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','lead_inserts_decrypted_pkey') }}")
	]
) }}
SELECT
	DISTINCT
	 cast(id as bigint) as id,
	 cast(aggregate_id as uuid) as aggregate_id,
	 {{ convert_timezone('created','CETDST') }} as created,
	 body
FROM {{source('public', 'raw_lead_inserts')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
