{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['brokerage_code'], 'type': 'btree'},
	  {'columns': ['broker_network'], 'type': 'hash'},
	  {'columns': ['aggregate_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','brokerages_incremental_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(broker_network as uuid) as broker_network,
	 brokerage_code,
	 name,
	 full_name,
	 enabled,
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated,
	 cast(data as json) as data,
	 opening_hours_minutes_of_day,
	 closing_hours_minutes_of_day,
	 local_timezone,
	 click_to_call_enabled,
	 cast(aggregate_id as uuid) as aggregate_id,
	 referral_aggreement_email,
	 verification_status
	 --,use_agent_zips
FROM {{source('public', 'raw_brokerages')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
