{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_id'], 'type': 'btree'},
	  {'columns': ['partner_id'], 'type': 'hash'},
	  {'columns': ['profile_aggregate_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','current_assignments_pkey') }}")
	]
) }}
with cte as (
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 role,
	 null as email,
	 null AS partner_id,
	 null as profile,
	 cast(profile_aggregate_id as {{ uuid_formatter() }}) as profile_aggregate_id,
	 -- we need a case statement to parse the dates based on DST
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 ROW_NUMBER() over (PARTITION BY id order by created DESC) as row_id
FROM {{source('public', 'raw_import_current_assignments')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
)
select id,lead_id,	 role,
	 email,partner_id,profile, profile_aggregate_id , created from cte where row_id = 1