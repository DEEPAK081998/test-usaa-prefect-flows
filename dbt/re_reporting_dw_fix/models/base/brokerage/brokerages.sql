{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['brokerage_code'], 'type': 'btree'},
	  {'columns': ['broker_network'], 'type': 'hash'},
	  {'columns': ['aggregate_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','brokerages_pkey') }}")
	]
) }}
WITH dedup AS (
SELECT
	 cast(id as bigint) as id,
	 cast(broker_network as {{ uuid_formatter() }}) as broker_network,
	 brokerage_code,
	 name,
	 full_name,
	 enabled,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated,
	 {{ parse_json('data') }} as data,
	 opening_hours_minutes_of_day,
	 closing_hours_minutes_of_day,
	 local_timezone,
	 click_to_call_enabled,
	 cast(aggregate_id as {{ uuid_formatter() }}) as aggregate_id,
	 use_agent_zips,
	 row_number() over(partition by id order by updated desc) as row_id
FROM {{source('public', 'raw_import_brokerages')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
)
SELECT
	id,
	broker_network,
	brokerage_code,
	name,
	full_name,
	enabled,
	created,
	updated,
	data,
	opening_hours_minutes_of_day,
	closing_hours_minutes_of_day,
	local_timezone,
	click_to_call_enabled,
	aggregate_id,
	use_agent_zips
FROM dedup
WHERE row_id =1
order by id 
