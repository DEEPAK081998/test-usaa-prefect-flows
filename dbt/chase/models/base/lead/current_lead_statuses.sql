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
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated,
	 role,
	 --cast(partner_id as uuid) as partner_id,
	 --email,
	 --cast(profile as json) as profile,
	 category,
	 status,
	 comment,
	 cast(data as json) as data,
	 cast(profile_aggregate_id AS UUID) as profile_aggregate_id
FROM {{source('public', 'raw_current_lead_statuses')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
