{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','normalized_lead_locations_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 cast(normalized_purchase_location as json) as normalized_purchase_location,
	 cast(normalized_sell_location as json) as normalized_sell_location,
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated
	 --purchase_state,
	 --sell_state
FROM {{source('public', 'raw_normalized_lead_locations')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }}  >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
