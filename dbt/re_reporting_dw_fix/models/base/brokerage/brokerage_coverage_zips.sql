{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['brokerage_code'], 'type': 'btree'},
	  {'columns': ['zip'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','brokerage_coverage_zips_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 brokerage_code,
	 zip,
	 city,
	 county,
	 state,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated
FROM {{source('public', 'raw_import_brokerage_coverage_zips')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}