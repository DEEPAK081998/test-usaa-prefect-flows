{{
  config(
    materialized = 'incremental',
    unique_key = ['lead_id','brokerage_code'],
    indexes=[
	  {'columns': ['lead_id'], 'type': 'btree'},
      {'columns': ['brokerage_code'], 'type': 'btree'}
	],
    enabled=false 
    )
}}
WITH
brokerage_assignments_cte AS (
    SELECT lead_id, max(created) as most_recent
    FROM {{ ref('user_assignments') }} 
    GROUP BY lead_id
)
, brokerage_qualifications_cte AS (
    SELECT
        brokerage_code,
        created as agentdirectDate,
        qualification as brokeragequalification
    FROM {{ ref('brokerage_qualifications') }} 
    WHERE qualification = 'directAgentQualified'
)
, partner_user_profiles_cte AS (
    SELECT brokerage_code, count(*) as agentCount
    FROM {{ ref('partner_user_profiles') }} 
    GROUP BY brokerage_code
)
, brokerageDirectAssignments_cte AS (
    SELECT brokerage_code, count(*) as eligibleAgentCount
    FROM {{ ref('partner_user_profiles') }} 
    WHERE eligibility_status = 'brokerageDirectAssignments'
    GROUP BY brokerage_code
)
, brokerageCoverage_cte AS (
    SELECT
        brokerage_code,
        count(*) as coverage
    FROM {{ ref('brokerage_coverage_zips') }} 
    GROUP BY brokerage_code
)
, final_cte AS (
    SELECT
        ba.lead_id,
        b.brokerage_code,
        b.full_name,
        concat(pup.first_name, ' ', pup.last_name) as RC_Name,
        pup.first_name,
        pup.last_name,
        pup.email as RC_Email,
        pup.phone as RC_Phone,
        pup.data -> 'address' ->> 'city' as rc_city,
        pup.data -> 'address' ->> 'state' as rc_state,
        pup.data -> 'address' ->> 'zip' as rc_zip,
        z.coverage,
        bq.brokeragequalification,
        bq.agentdirectDate,
        ac.agentCount,
        ea.eligibleAgentCount,
        NOW() as updated_at
    FROM 
    {{ ref('user_assignments') }}  ba
    JOIN brokerage_assignments_cte mt on mt.lead_id = ba.lead_id and mt.most_recent = ba.created
    left join {{ ref('brokerages') }} b on b.aggregate_id = ba.profile_aggregate_id
    left join {{ ref('brokerage_user') }} bu on bu.brokerage_code = b.brokerage_code
    left outer join brokerage_qualifications_cte bq on b.brokerage_code = bq.brokerage_code
    left outer join partner_user_profiles_cte ac on b.brokerage_code = ac.brokerage_code
    left outer join brokerageDirectAssignments_cte ea on b.brokerage_code = ea.brokerage_code
    left outer join brokerageCoverage_cte z on b.brokerage_code = z.brokerage_code
    left join {{ ref('partner_user_profiles') }}  pup on pup.email = bu.email
    {% if is_incremental() %}
    WHERE ba.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)
SELECT * FROM final_cte