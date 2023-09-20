{{ config(
	materialized='incremental',
	unique_key =['id'],
	indexes=[
      {'columns': ['zip'], 'type': 'hash'},
	  {'columns': ['id'], 'type': 'btree'},
    ]
) }}
WITH
 cbsa_locations AS	(
	SELECT
		zip,
	 	city,
	 	county,
	 	{% if target.name | upper =='PROD' or target.name == 'warehouse_db' or target.name == 'snowflake_sandbox' or target.name == 'snowflake_prod' or target.type == 'snowflake' %}
	 	cbmsa,
	 	st
	 	{% else %}
	 	cbsa_name as cbmsa,
	 	state as st
	 	{% endif %}
		,MAX({{  special_column_name_formatter('_airbyte_normalized_at') }}) as updated_at
	FROM {{source('public', 'raw_import_cbsa_locations')}}
	{% if is_incremental() %}
	  WHERE _airbyte_normalized_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
	{% endif %}
	GROUP BY 1,2,3,4,5
)
SELECT 
	{{ dbt_utils.generate_surrogate_key(
        ['st',
        'county',
        'city',
        'zip']
    )}} AS id, * FROM cbsa_locations
order by zip,city,county,st