{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','saved_listings_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 vast_id,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated,
	 cast(user_profile_id as bigint) as user_profile_id,
	 address,
	 city,
	 state,
	 zip,
	 latitude,
	 longitude,
	 {{ parse_json('data') }} as data,
	 type
FROM {{source('public', 'raw_import_listings')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
