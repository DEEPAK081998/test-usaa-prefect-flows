{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['qualification'], 'type': 'btree'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','brokerage_qualifications_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 brokerage_code,
	 qualification,
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated
FROM {{source('public', 'raw_brokerage_qualifications')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
