{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','saved_searches_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 name,
	 {{ parse_json('params') }} AS params,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated,
	 user_profile_id
FROM {{source('public', 'raw_import_saved_searches')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}

