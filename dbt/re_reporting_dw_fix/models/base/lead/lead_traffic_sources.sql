{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['lead_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','lead_traffic_sources_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as  lead_id,
	 source,
	 medium,
	 campaign,
	 term,
	 content
FROM {{source('public', 'raw_import_lead_traffic_sources')}}
