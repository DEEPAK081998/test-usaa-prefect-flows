{{
  config(
    materialized = 'incremental',
    unique_key = ['brokerage_code','zip'],
  	tags=['brokerage']
    )
}}
WITH
brokerage_zip_counts_cte AS (
    SELECT brokerage_code, state, count(*) as ZipCount
    FROM {{ ref('brokerage_coverage_zips') }}
    GROUP BY brokerage_code,state
)

,brokerage_assignments_cte as (
    SELECT brokerage_code, count(*) as leadCount
    FROM {{ ref('brokerage_assignments') }} ba
    GROUP BY brokerage_code
)
, brokerage_cte AS (
    SELECT * FROM {{ ref('brokerages') }}
    {% if is_incremental() %}
    WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)

, final_cte AS (
    SELECT
        b.brokerage_code,
        b.full_name as brokerageName,
        bcz.zip,
        cbsa.cbmsa,
        cbsa.st,
        ZipCount.ZipCount,
        bac.rc_name as RC_Name,
        bac.RCbrokerageGroup as brokerageGroup,
        bac.rc_email,
        bac.rc_phone,
        bba.leadCount,
        {{ current_date_time() }} as updated_at
    FROM brokerage_cte b
    join {{ ref('brokerage_coverage_zips') }} bcz on bcz.brokerage_code = b.brokerage_code
    left join {{ ref('cbsa_locations') }} cbsa on cbsa.zip = bcz.zip
    left outer join {{ ref('stg_brokerage_group') }} bac on bac.brokerage_code=b.brokerage_code
    left outer join brokerage_assignments_cte bba on bba.brokerage_code = b.brokerage_code
    left outer join brokerage_zip_counts_cte ZipCount on ZipCount.brokerage_code = b.brokerage_code
)
SELECT * FROM final_cte
