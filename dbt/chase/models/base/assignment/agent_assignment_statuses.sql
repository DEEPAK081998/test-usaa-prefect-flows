{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['agent_external_id'], 'type': 'btree'},
	  {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['status'], 'type': 'btree'},
 	  {'columns': ['broker_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','pk_agent_assignment_statuses') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 {{ convert_timezone('created','CETDST') }} as created,
	 status,
	 cast(lead_id as bigint) as lead_id,
	 agent_external_id,
	 cancellation_reason,
	 agent_name,
	 agent_phone,
	 agent_email,
	 cast(broker_id as uuid) as broker_id
FROM {{source('public', 'raw_agent_assignment_statuses')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}