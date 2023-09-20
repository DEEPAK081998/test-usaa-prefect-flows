{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['lead_id'], 'type': 'btree'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','current_notes_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 cast(lead_id as bigint) as lead_id,
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated,
	 role,
	 --cast(partner_id as uuid) as partner_id,
	 --email,
	 --cast(profile as json) as profile,
	 cast(data as json) as data,
	 private,
	 cast(profile_aggregate_id as UUID) AS profile_aggregate_id
FROM {{source('public', 'raw_current_notes')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}

