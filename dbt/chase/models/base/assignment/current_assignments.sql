{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_id'], 'type': 'btree'},
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
	 ---email,
	 --cast(partner_id as uuid) AS partner_id,
	 --cast(profile as json) as profile,
	 cast(profile_aggregate_id as uuid) as profile_aggregate_id,
	 {{ convert_timezone('created','CETDST') }} as created,
	 ROW_NUMBER() over (PARTITION BY id order by created DESC) as row_id
FROM {{source('public', 'raw_current_assignments')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('created','CETDST') }} >= coalesce((select max(created) from {{ this }}), '1900-01-01')
{% endif %}
)
select id,lead_id,	 role,
	 --email,
	 --partner_id,
	 --profile, 
	 profile_aggregate_id ,
     created from cte where row_id = 1