{{ config(
	materialized='incremental',
	unique_key='id',
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','brokerage_user_pkey') }}")
	]
) }}
WITH dedup AS (
SELECT
	DISTINCT
	 cast(id as bigint) as id,
	 brokerage_code,
	 email,
	 notify_on_assignment,
	 {{ convert_timezone('created','CETDST') }} as created,
	 {{ convert_timezone('updated','CETDST') }} as updated,
	 row_number() over(partition by id order by updated desc) as row_id
FROM {{source('public', 'raw_brokerage_user')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
)
SELECT
id,
brokerage_code,
email,
notify_on_assignment,
created,
updated FROM dedup
WHERE row_id = 1
order by id 