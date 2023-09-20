{{
  config(
	materialized = 'incremental',
	unique_key = 'id',
	tags=['sofi']
	)
}}
WITH 
cte AS
(
	SELECT * FROM {{ ref('stg_partners_user_profile_mortgagelinkclicks') }}
	WHERE partner_id = '77daccc1-1178-4fd2-b95e-4f291476cbd9'
	{% if is_incremental() %}
	  and updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
	{% endif %}
)
SELECT
	*,
	CASE
		WHEN url1_ LIKE '%utm_content=prequalcta' THEN 'prequalcta'
		WHEN url1_ LIKE '%utm_content=mortgageratescta' THEN 'mortgageratescta'
		ELSE url1_
	END AS url1_cat,
	CASE
		WHEN url2_ LIKE '%utm_content=prequalcta' THEN 'prequalcta'
		WHEN url2_ LIKE '%utm_content=mortgageratescta' THEN 'mortgageratescta'
		ELSE url2_
	END AS url2_cat,
	CASE
		WHEN url3_ LIKE '%utm_content=prequalcta' THEN 'prequalcta'
		WHEN url3_ LIKE '%utm_content=mortgageratescta' THEN 'mortgageratescta'
		ELSE url3_
	END AS url3_cat,
	CASE
		WHEN url4_ LIKE '%utm_content=prequalcta' THEN 'prequalcta'
		WHEN url4_ LIKE '%utm_content=mortgageratescta' THEN 'mortgageratescta'
		ELSE url4_
	END AS url4_cat,
	CASE
		WHEN url5_ LIKE '%utm_content=prequalcta' THEN 'prequalcta'
		WHEN url5_ LIKE '%utm_content=mortgageratescta' THEN 'mortgageratescta'
		ELSE url5_
	END AS url5_cat
FROM cte