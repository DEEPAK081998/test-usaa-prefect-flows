WITH 

brokerage_cte as (
    SELECT brokerage_code, created as agentDirectStartDate, qualification as brokeragequalification FROM {{ ref('brokerage_qualifications') }} 
    WHERE qualification = 'directAgentQualified'
)

,brokerage_coverage_zips_cte as (
    SELECT brokerage_code, count(*) as coverage FROM {{ ref('brokerage_coverage_zips') }} 
    GROUP BY  brokerage_code
)

, most_recent_cte AS (
    SELECT lead_id, max(created) as most_recent FROM {{ source('public', 'profile_assignments') }}
    GROUP BY lead_id
)

, brokerage_assignments_cte AS (
  SELECT
    ba.lead_id,
    b.brokerage_code,
    b.full_name,
    concat(pup.first_name,' ',pup.last_name) as RC_Name,
    pup.first_name, pup.last_name,
    pup.email as RC_Email,
    pup.phone as RC_Phone,
    pup.data::json->'address'->'city' as rc_city,
    pup.data::json->'address'->'state' as rc_state,
    z.coverage,
    bq.brokeragequalification,
    bq.agentDirectStartDate
  FROM {{ source('public', 'profile_assignments') }} ba
  JOIN most_recent_cte mt on mt.lead_id = ba.lead_id and mt.most_recent = ba.created
  LEFT JOIN {{ ref('brokerages') }} b on b.aggregate_id = ba.profile_aggregate_id
  LEFT JOIN {{ ref('brokerage_user') }} bu on bu.brokerage_code = b.brokerage_code
  LEFT OUTER JOIN brokerage_cte bq on b.brokerage_code = bq.brokerage_code
  LEFT OUTER JOIN brokerage_coverage_zips_cte z on b.brokerage_code = z.brokerage_code
  LEFT JOIN {{ ref('partner_user_profiles') }}  pup on pup.email = bu.email
)

, user_assignments_cte AS (
  SELECT
    pa.lead_id,
    (min(case when pa.role = 'REFERRAL_COORDINATOR' then pa.created end) - interval '7 hour') as RC_assign_time,
    (min(case when pa.role = 'AGENT' then pa.created end) - interval '7 hour') as agent_assign_time,
    (min(case when pa.role = 'AGENT' then pa.created end)) as agent_assign_time_unadjusted,
    (min(case when pa.role = 'MLO' then pa.created end) - interval '7 hour') as MLO_assign_time,
    (COUNT(case when pa.role = 'REFERRAL_COORDINATOR'  and pup.email = ('routing+prod@homestory.co') then 1 end )) as EscalatedtoConcierge
  FROM {{ source('public', 'profile_assignments') }} pa
  left join {{ ref('partner_user_profiles') }}  pup
  on pa.profile_aggregate_id = pup.aggregate_id
  GROUP BY pa.lead_id
)

, leads_brokerage_cte AS (
    SELECT
        t1.id,
        date(t1.created - interval '7 Hours') as enrollDate,
        bac.brokerage_code,
        bac.full_name,
        bac.RC_name,
        bac.RC_Email,
        bac.RC_phone,
        bac.coverage,
        bac.brokeragequalification,
        bac.agentDirectStartDate,
        case when lower(cls.HB_Status) like '%unassigned%' then 1 else 0 end as unassignedEnrollments,
        case when lower(cls.HB_Status) like '%new%' then 1 else 0 end as unacceptedEnrollments,
        fat.Accept_Time_Delay_Hrs,
        EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted - fat.RC_accept_time_unadjusted)) /60/60 as AgentAssign_Time_Delay_Hrs,
        CONCAT(date_part('month', t1.created),'/1/',date_part('year', t1.created)) as mnth_yr,
        case
            when date_part('hour',(t1.created - interval '7' hour)) between 10 and 17 and date_part('dow', t1.created) between 1 and 5 then 'Business Hours' 
            else 'Outside Business Hours' 
        end as BusinessHours,
        fat.first_contact_delay_from_enrollment,
        fat.accept_to_contact_delay
    FROM
        {{ ref('leads') }}  t1
        left outer join brokerage_assignments_cte bac on bac.lead_id = t1.id
        left outer join user_assignments_cte ua1 on ua1.lead_id = t1.id
        left outer join {{ ref('stg_lead_status_agg') }} fat on fat.lead_id = t1.id
        left outer join {{ ref('stg_lead_current_lead_statuses_agg') }} cls on cls.lead_id = t1.id
    WHERE
        t1.id not in {{ TestLeads() }} --Excludes TestLeads/KnownFakes
        and t1.bank_id not in ('25A363D4-0BE0-4001-B9FB-A2F8BA91170C') --Excludes TD
        and date(t1.created) > '2017-09-01'
        and cls.HB_Status <> 'Inactive Test Referral'
)

, final_cte AS (
    SELECT
        brokerage_code,
        full_name,
        rc_name,
        rc_email,
        rc_phone,
        case when coverage is null then 0 else coverage end as coverage ,
        enrollDate,
        BusinessHours,
        brokeragequalification,
        agentDirectStartDate,
        count(id),
        sum(unassignedEnrollments) as unassignedEnrollments,
        sum(unacceptedEnrollments) as unacceptedEnrollments,
        avg(Accept_Time_Delay_Hrs) as acceptDelay,
        avg(AgentAssign_Time_Delay_Hrs) as assignDelay,
        avg(first_contact_delay_from_enrollment) as contactDelay,
        avg(accept_to_contact_delay) as acceptContactDelay
    FROM leads_brokerage_cte a
    GROUP BY
        brokerage_code,
        full_name,
        rc_name,
        rc_email,
        rc_phone,
        coverage,
        enrollDate,
        BusinessHours,
        brokeragequalification,
        agentDirectStartDate
)
SELECT * FROM final_cte