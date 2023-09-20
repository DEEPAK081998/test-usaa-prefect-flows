{{
  config(
    materialized = 'incremental',
    unique_key = ['lead_id','brokerage_code','rc_name'],
    indexes=[
	  {'columns': ['lead_id'], 'type': 'btree'},
      {'columns': ['brokerage_code'], 'type': 'btree'}
	]
    )
}}
WITH
brokerage_assignments_cte AS (
    SELECT lead_id, max(created) as most_recent
    FROM {{ ref('brokerage_assignments') }}
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
        ba.brokerage_code,
        b.full_name,
        concat(pup.first_name, ' ', pup.last_name) as RC_Name,
        pup.first_name,
        pup.last_name,
        pup.email as RC_Email,
        pup.phone as RC_Phone,
        {% if target.type == 'snowflake' %}
        pup.data:address.city::VARCHAR as rc_city,
        pup.data:address.state::VARCHAR as rc_state,
        pup.data:address.zip::VARCHAR as rc_zip,
        {% else %}
        pup.data -> 'address' ->> 'city' as rc_city,
        pup.data -> 'address' ->> 'state' as rc_state,
        pup.data -> 'address' ->> 'zip' as rc_zip,
        {% endif %}
        z.coverage,
        bq.brokeragequalification,
        bq.agentdirectDate,
        ac.agentCount,
        ea.eligibleAgentCount,
        {{ current_date_time() }} as updated_at
    FROM 
    {{ ref('brokerage_assignments') }} ba
    JOIN brokerage_assignments_cte mt on mt.lead_id = ba.lead_id and mt.most_recent = ba.created
    left join {{ ref('brokerages') }} b on b.brokerage_code = ba.brokerage_code
    left join {{ ref('brokerage_user') }} bu on bu.brokerage_code = ba.brokerage_code
    left outer join brokerage_qualifications_cte bq on ba.brokerage_code = bq.brokerage_code
    left outer join partner_user_profiles_cte ac on ba.brokerage_code = ac.brokerage_code
    left outer join brokerageDirectAssignments_cte ea on ba.brokerage_code = ea.brokerage_code
    left outer join brokerageCoverage_cte z on ba.brokerage_code = z.brokerage_code
    left join {{ ref('partner_user_profiles') }} pup on pup.email = bu.email
    {% if is_incremental() %}
    WHERE ba.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)
SELECT DISTINCT * FROM final_cte