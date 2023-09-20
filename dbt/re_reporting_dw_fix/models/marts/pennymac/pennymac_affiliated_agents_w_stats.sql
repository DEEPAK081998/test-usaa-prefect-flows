{{
  config(
    materialized = 'table',
    )
}}
WITH 

lo_lead_cte AS (
    SELECT 
        date,
        count(distinct id) as affiliated_lead_count,
        count(distinct id) as total_lead_count,
        lo_aggregate_id,
        agent_aggregate_id
    FROM {{ ref('leads_data_v3') }}
    WHERE bank_name = 'PennyMac'
    group by date, agent_aggregate_id, lo_aggregate_id
)
, agent_lead_cte AS (
    SELECT 
        date,
        count(distinct id) as agent_total_lead_count,
        agent_aggregate_id
    FROM {{ ref('leads_data_v3') }}
    WHERE bank_name = 'PennyMac'
    GROUP BY date, agent_aggregate_id
)
, agent_count AS (
    SELECT
        pa.lo_aggregate_id,
        count( distinct agent_aggregate_id) as affiliated_agent_counts
    FROM {{ ref('pennymac_affiliated_agents') }} pa
    GROUP BY lo_aggregate_id
)
, leads_to_pennymac AS (
    SELECT
        date,
        count(distinct id) as leads_to_pennymac,
        agent_aggregate_id
    FROM {{ ref('leads_data_v3_agent_submitted') }}
    WHERE bank_name = 'PennyMac' and agent_submitted = 'true'
    GROUP BY date, agent_aggregate_id
)
, final_cte AS (
    SELECT
        paa.*,
        ac.affiliated_agent_counts,
        llc.date as affiliated_enroll_date,
        coalesce(ll.date,al.date) as enroll_date,
        llc.affiliated_lead_count,
        ll.total_lead_count,
        al.agent_total_lead_count,
        lp.leads_to_pennymac
    FROM {{ ref('pennymac_affiliated_agents') }} paa
    LEFT JOIN agent_count ac on ac.lo_aggregate_id = paa.lo_aggregate_id
    LEFT JOIN lo_lead_cte llc on llc.agent_aggregate_id = paa.agent_aggregate_id and llc.lo_aggregate_id = paa.lo_aggregate_id
    LEFT JOIN lo_lead_cte ll on ll.agent_aggregate_id = paa.agent_aggregate_id
    LEFT JOIN agent_lead_cte al on al.agent_aggregate_id = paa.agent_aggregate_id
    LEFT JOIN leads_to_pennymac lp on lp.agent_aggregate_id = paa.agent_aggregate_id

    UNION

    SELECT
        p.*,
        ac.affiliated_agent_counts,
        '1/1/1990' as affiliated_enroll_date,
        '1/1/1990' as enroll_date,
        0 as affiliated_lead_count,
        0 as total_lead_count,
        0 as agent_total_lead_count,
        0 as leads_to_penymac
    FROM pennymac_affiliated_agents p
    LEFT JOIN agent_count ac on ac.lo_aggregate_id = p.lo_aggregate_id

)
SELECT * FROM final_cte