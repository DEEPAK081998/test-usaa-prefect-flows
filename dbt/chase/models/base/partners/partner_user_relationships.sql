{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','partner_user_relationships_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(parent_profile_id as uuid) as parent_profile_id,
	 cast(child_profile_id as uuid) as child_profile_id,
	 enabled,
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated
	 --cast(parent_profile_uuid as uuid) AS parent_profile_uuid,
	 --cast(child_profile_uuid as uuid) AS child_profile_uuid
FROM {{source('public', 'raw_partner_user_relationships')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
