{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['lead_id'], 'type': 'btree'}
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','normalized_lead_zips_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
     cast(zip AS text) as zip,
     transaction_type
FROM 
    {{ source('public', 'raw_normalized_lead_zips') }}