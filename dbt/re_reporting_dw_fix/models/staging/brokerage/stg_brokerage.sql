{{
  config(
    materialized = 'incremental',
    unique_key = ['brokerage_code','id'],
	indexes=[
      {'columns': ['id'], 'type': 'btree'},
	  {'columns': ['brokerage_code'], 'type': 'btree'},
	]
    )
}}
WITH

brokerage_cte AS (
    SELECT * FROM {{ ref('brokerages') }}
    {% if is_incremental() %}
    WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)

, partner_user_profiles_cte AS (
    SELECT * FROM {{ ref('partner_user_profiles') }}
    {% if is_incremental() %}
    WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)

, final_cte AS (
    SELECT
        b.brokerage_code,
        b.full_name as brokerage,
        pup.id,
        concat(pup.first_name,' ',pup.last_name) as RC_Name,
        pup.first_name, pup.last_name,
        pup.email as RC_Email,
        pup.phones as RC_Phone,
        {{ current_date_time() }} as updated_at
    FROM brokerage_cte b
    JOIN {{ ref('brokerage_user') }} bu on b.brokerage_code = bu.brokerage_code
    JOIN partner_user_profiles_cte pup on bu.email = pup.email
)
SELECT * FROM final_cte