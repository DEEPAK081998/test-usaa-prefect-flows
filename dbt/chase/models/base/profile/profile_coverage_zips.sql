{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['zip'], 'type': 'btree'},
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','profile_coverage_zips_pkey') }}")
	]
) }}
WITH dedup AS (
SELECT
	DISTINCT
	 cast(id as bigint) as id,
	 cast(profile_id as bigint) as profile_id,
	 zip,
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated,
	 city,
	 county,
	 state,
	 row_number() over(partition by id order by updated desc) as row_id
FROM {{source('public', 'raw_profile_coverage_zips')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
)
SELECT
id,
profile_id,
zip,
created,
updated,
city,
county,
state FROM dedup
WHERE row_id = 1
order by id 