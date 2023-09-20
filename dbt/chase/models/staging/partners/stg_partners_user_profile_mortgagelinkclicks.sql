{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    )
}}

WITH
mort_clicks AS(
	SELECT
		*,
		data::json->'mortgageLinkClicks' AS mortgage_clicks,
		COALESCE(data::json->'mortgageLinkCLicks'->0->'link'::TEXT,data::json->'mortgageLinkClicks'->0->'url'::TEXT) AS url1,
		COALESCE(data::json->'mortgageLinkCLicks'->0->'link'::TEXT,data::json->'mortgageLinkClicks'->1->'url'::TEXT) AS url2,
		COALESCE(data::json->'mortgageLinkCLicks'->0->'link'::TEXT,data::json->'mortgageLinkClicks'->2->'url'::TEXT) AS url3,
		COALESCE(data::json->'mortgageLinkCLicks'->0->'link'::TEXT,data::json->'mortgageLinkClicks'->3->'url'::TEXT) AS url4,
		COALESCE(data::json->'mortgageLinkCLicks'->0->'link'::TEXT,data::json->'mortgageLinkClicks'->4->'url'::TEXT) AS url5,
		updated as updated_at
	FROM 
		{{ ref('partner_user_profiles') }} 
	WHERE
		data::json->'mortgageLinkClicks' IS NOT NULL
		{% if is_incremental() %}
		  and updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
		{% endif %}
),
pull_timestamps AS(
	SELECT
		partner_id,
		id,
		first_name,
		last_name,
		email,
		created,
		updated_at,
		data::json->'mortgageLinkClicks',
		REPLACE(COALESCE(url1->'link',url1)::text,'"','') AS url1_,
		CAST(data::json->'mortgageLinkClicks'->0->'timestamp' AS TEXT) AS url1_timestamp_,
		REPLACE(COALESCE(url2->'link',url2)::text,'"','') AS url2_,
		CAST(data::json->'mortgageLinkClicks'->1->'timestamp' AS TEXT) AS url2_timestamp_,
		REPLACE(COALESCE(url3->'link',url3)::text,'"','') AS url3_,
		CAST(data::json->'mortgageLinkClicks'->2->'timestamp' AS TEXT) AS url3_timestamp_,
		REPLACE(COALESCE(url4->'link',url4)::text,'"','') AS url4_,
		CAST(data::json->'mortgageLinkClicks'->3->'timestamp' AS TEXT) AS url4_timestamp_,
		REPLACE(COALESCE(url5->'link',url5)::text,'"','') AS url5_,
		CAST(data::json->'mortgageLinkClicks'->4->'timestamp' AS TEXT) AS url5_timestamp_
	FROM 
		mort_clicks
	),
-- recast into bigint
recast AS(
	SELECT
		partner_id,
		id,
		first_name,
		last_name,
		email,
		created,
		updated_at,
		url1_,
		(CAST (url1_timestamp_ AS bigint)) AS url1_timestamp,
		url2_,
		(CAST (url2_timestamp_ AS bigint)) AS url2_timestamp,
		url3_,
		(CAST (url3_timestamp_ AS bigint)) AS url3_timestamp,
		url4_,
		(CAST (url4_timestamp_ AS bigint)) AS url4_timestamp,
		url5_,
		(CAST (url5_timestamp_ AS bigint)) AS url5_timestamp
	FROM
		pull_timestamps
),
-- cast datetime
final_cte AS
(
	SELECT
		partner_id,
		id,
		first_name,
		last_name,
		email,
		created,
		updated_at,
		url1_,
		TO_TIMESTAMP(url1_timestamp::bigint/1000) AS url1_date,
		url2_,
		TO_TIMESTAMP(url2_timestamp::bigint/1000) AS url2_date,
		url3_,
		TO_TIMESTAMP(url3_timestamp::bigint/1000) AS url3_date,
		url4_,
		TO_TIMESTAMP(url4_timestamp::bigint/1000) AS url4_date,
		url5_,
		TO_TIMESTAMP(url5_timestamp::bigint/1000) AS url5_date
	FROM 
		recast
)
SELECT * FROM final_cte