{{
  config(
    materialized = 'incremental',
    unique_key = ['brokerage_code'],
	indexes=[
	  {'columns': ['brokerage_code'], 'type': 'btree'}
	]
    )
}}
WITH
brokerage_cte  AS (
    SELECT * FROM {{ ref('brokerages') }}
    {% if is_incremental() %}
    WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)
, partner_user_cte AS (
    SELECT * FROM {{ ref('partner_user_profiles') }}
    {% if is_incremental() %}
    WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)

, final_cte AS (
    SELECT
        b.brokerage_code,
        b.full_name,
        concat(pup.first_name,' ',pup.last_name) as RC_Name,
        pup.first_name, pup.last_name,
        pup.email as RC_Email,
        pup.phones as RC_Phone,
        {% if target.type == 'snowflake' %}
        pup.data:address.city::VARCHAR as rc_city,
        pup.data:address:state::VARCHAR as rc_state,
        {% else %}
        pup.data->'address'->>'city' as rc_city,
        pup.data->'address'->>'state' as rc_state,
        {% endif %}
        case
            when lower(b.brokerage_code) = 'cs001' then 'CAE'
            when lower (b.brokerage_code) = 'hs001' then 'HSAdmin'
            when substring(b.brokerage_code,3,1) = '2' then 'RLRE'
            when substring(b.brokerage_code,3,1) = '3' then 'BHHS'
            when substring(b.brokerage_code,3,1) = '4' then 'HS'
            when substring(b.brokerage_code,3,1) = '5' then 'RISE'
            when substring(b.brokerage_code,3,1) = '7' then 'HSoA'
            when substring(b.brokerage_code,3,1) = '8' then 'HS'
            when substring(b.brokerage_code,3,1) = '9' then 'HS'
            else 'Other'
        end as RCbrokerageGroup,
        {{ current_date_time() }} as updated_at
    FROM brokerage_cte b
    JOIN {{ ref('partner_user_relationships') }} pur on b.aggregate_id = pur.parent_profile_uuid
    JOIN partner_user_cte pup on pur.child_profile_uuid = pup.aggregate_id
)
SELECT * FROM final_cte