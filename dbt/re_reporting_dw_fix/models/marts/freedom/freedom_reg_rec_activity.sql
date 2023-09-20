{{
  config(
    materialized = 'incremental',
    unique_key = 'id'
    )
}}

WITH partner_profiles AS
(
    SELECT
		CAST(id AS VARCHAR) AS id,
		CAST(partner_id AS VARCHAR) AS partner_id,
		first_name,
		last_name,
    created,
    email,
    phone,
    updated
	FROM
		{{ ref('partner_user_profiles') }}
    WHERE partner_id = '3405dc7c-e972-4bc4-a3da-cb07e822b7c6'
),
registered_rec_data AS(
    SELECT
        a.id,
        b.id AS registered_id, 
        a.ga_date,
        a.ga_pagepath,
        a.updated_at,
        a.ga_users,
        a.ga_newusers,
        a.ga_sessions,
        a.ga_entrances,
        a.ga_pageviews,
        a.ga_bouncerate,
        a.ga_avgtimeonpage,
        a.ga_uniquepageviews,
        a.ga_avgsessionduration,
        a.ga_pageviewspersession,
        b.first_name,
        b.last_name,
        b.email,
        b.phone,
        b.updated 
    FROM {{ ref('freedom_user_id_paths') }} a
    JOIN partner_profiles b
    ON a.ga_dimension5 = b.id
    WHERE ga_dimension5 !='Not Set' AND
    ga_pagepath = '/' 
  
)
select * from registered_rec_data
{% if is_incremental() %}
  where ga_date >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}
