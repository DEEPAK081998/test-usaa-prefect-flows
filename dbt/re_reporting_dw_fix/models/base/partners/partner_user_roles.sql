{{ config(
	materialized='incremental',
	unique_key=['user_profile_id','role'],
	incremental_strategy='delete+insert',
	indexes=[
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','partner_user_roles_pkey') }}")
	]
) }}

{% if is_incremental() %}

with pur_row_id_cte as (
	select
		user_profile_id,
		role,
		row_number() over
			(partition by user_profile_id, role order by updated desc) as row_id
	from
		{{ this }}
),
pur_duplicated as (
	select distinct
		user_profile_id
	from
		pur_row_id_cte
	where
		row_id > 1
)

{% endif %}

SELECT
	 max(cast(id as bigint)) as id,
	 cast(user_profile_id as bigint) as user_profile_id,
	 role,
	 max({{ local_convert_timezone('created','CETDST') }}) as created,
	 max({{ local_convert_timezone('updated','CETDST') }}) as updated
FROM 
	{{source('public', 'raw_import_partner_user_roles')}}

{% if is_incremental() %}

WHERE 
	{{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
	OR cast(user_profile_id as bigint) IN (select user_profile_id from pur_duplicated)

{% endif %}

GROUP BY
	cast(user_profile_id as bigint),
	role