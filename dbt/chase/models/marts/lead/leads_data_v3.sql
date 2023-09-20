{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    indexes=[
	  {'columns': ['id'], 'type': 'hash'},
	  {'columns': ['updated_at'], 'type': 'btree'},
	  ],
    )
}}

WITH 

brokerage_cte as (
    SELECT brokerage_code, qualification as brokerageRoutingMethod FROM {{ ref('brokerage_qualifications') }} 
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

, current_assigment_cte AS (
    SELECT lead_id, id FROM {{ ref('current_assignments') }} 
    WHERE lower(role) = 'agent'
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
    bq.brokerageRoutingMethod
  FROM {{ source('public', 'profile_assignments') }} ba
  JOIN most_recent_cte mt on mt.lead_id = ba.lead_id and mt.most_recent = ba.created
  LEFT JOIN {{ source('public', 'brokerages') }} b on b.aggregate_id = ba.profile_aggregate_id
  LEFT JOIN {{ source('public', 'partner_user_relationships') }} pur on pur.parent_profile_id = b.aggregate_id and pur.enabled
  LEFT OUTER JOIN brokerage_cte bq on b.brokerage_code = bq.brokerage_code
  LEFT OUTER JOIN brokerage_coverage_zips_cte z on b.brokerage_code = z.brokerage_code
  LEFT JOIN {{ ref('partner_user_profiles') }}  pup on pup.aggregate_id = pur.child_profile_id
)
, agent_notes_cte AS (
    SELECT pp.aggregate_id,pur.role,pp.email, pp.data::json->'additionalNotes' as AgentNotes
    , pp.first_name || ' ' || pp.last_name as fullName,pp.phone
    , pp.first_name 
    , pp.last_name
    FROM {{ ref('partner_user_profiles') }}  pp
        JOIN {{ source('public', 'partner_user_relationships') }} pr 
        ON pp.aggregate_id = pr.child_profile_id
        LEFT JOIN {{ ref('partner_user_roles') }}  pur
        on pp.id = pur.user_profile_id
    --WHERE pr.parent_profile_id= 1011
)

, current_assigment__agent_notes_cte  AS (
    SELECT ca.lead_id, p.email, p.phone,
           p.fullName as fullName,
           p.first_name as AgentFirstName,
           p.last_name as AgentLastName,
           p.AgentNotes as OutofNetworkBrokerage
    FROM {{ ref('current_assignments') }}  ca
    LEFT OUTER JOIN agent_notes_cte p on ca.profile_aggregate_id = p.aggregate_id
    WHERE p.role = 'AGENT'
)

, current_assigment_mlo_cte AS (
    SELECT
     lead_id,
     pp.email,
     pp.phone as phone,
     pp,first_name as lo_first_name,
     pp.last_name as lo_last_name,
     pp.first_name || ' ' || pp.last_name  as fullName,
     pp.data::json->>'nmlsid' as NMLSid
    FROM {{ ref('current_assignments') }}  ca
    join {{ ref('partner_user_profiles') }}  pp
    on ca.profile_aggregate_id = pp.aggregate_id
    JOIN {{ source('public', 'partner_user_relationships') }} pr 
        ON pp.aggregate_id = pr.child_profile_id
    WHERE role = 'MLO'
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

, leads_data_v2 AS (

  SELECT
    date(t1.created - interval '7 Hours'),
    t1.id,
    concat(t1.first_name,' ',t1.last_name) as client_name, --<--Use this when decrypted
    t1.email as client_email,
    replace(t1.phone,'+','') as client_phone,
    t2.fullName as agent_name,
    case when t2.email is null then null else t2.email end as agent_email,
    t2.phone as agent_phone,

    t3.fullName as lo_name,
    case when t3.email is null then null else t3.email end as lo_email,
    t3.phone as lo_phone,

    t1.purchase_location,
    case when t1.purchase_time_frame = 1 then 90
    when t1.purchase_time_frame = 2 then 180 else 365 end as purchase_time_frame,
    t1.prequal,
    (
      case 
          when t1.price_range_lower_bound is null or t1.price_range_lower_bound = 0
              then t1.price_range_upper_bound::decimal 
          when t1.price_range_upper_bound is null or t1.price_range_lower_bound = 0
              then t1.price_range_lower_bound::decimal 
          else ((t1.price_range_lower_bound::decimal +t1.price_range_upper_bound::decimal)/2) 
          end
    ) as avg_price,
    t1.comments,
    t1.bank_name,

    case when t1.transaction_type = 'PURCHASE' then 'BUY'
    when t1.transaction_type = 'BOTH' then 'BUY' 
    else t1.transaction_type end as transaction_type,

    cls.MLO_Status,
    cls.MLO_Status_Time,
    cls.RC_Status,
    cls.RC_Status_Time,
    cls.Agent_Status,
    cls.Agent_Status_Time,
    cls.CloseDate,
    cls.PropertyAddress,
    cls.PropertyPrice,
    cls.LenderClosedWith,

    cls.AgtCommPct,

    t1.created,
    slsag.RC_accept_time_unadjusted,
    ua1.agent_assign_time_unadjusted,
    ua1.agent_assign_time,
    slsag.Accept_Time_Delay_Hrs,
    EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted - slsag.RC_accept_time_unadjusted))/60/60 as AgentAssign_Time_Delay_Hrs,
    CONCAT(date_part('month',t1.created),'/1/',date_part('year',t1.created)) as mnth_yr,
    bac.brokerage_code,	
    bac.full_name,	
    bac.rc_name,
    bac.rc_email,
    bac.rc_phone,	
    date_part('month',t1.created),
    '' as attachment,
    cls.HB_Status,
    date_part('dow',t1.created) as DOW,
    t1.created - interval '7' hour as CST ,
    date(t1.created - interval '7' hour) as CSTDate,
    date_part('hour',(t1.created - interval '7' hour)) as CSTHour,
    date_part('year',(t1.created - interval '7' hour)) as Year,
    date_part('week',(t1.created - interval '7' hour)) as WeekNum,	
    case 
            when date_part('hour',(t1.created - interval '7' hour)) between 10 and 17 and date_part('dow',t1.created) between 1 and 5 then 'Business Hours'
            else 'Outside Business Hours'
            end as BusinessHours,
    case when cls.HB_Status like 'Pending%' then '1'
        --when lower(cls.HB_Status) like '%inspection%' then '1'
          end as Pending,
    case when lower (ld.CitizensSalesForceEnrollment) = 'true' then 'true'
        when lower (ld.MLOSubmission) = 'true' then 'true'
        else ld.MLOSubmission end as MLOSubmission,
    t3.nmlsid,
    t2.OutofNetworkBrokerage,
    t1.first_name as Client_First_Name,
    t1.last_name as Client_Last_Name,
    bac.first_name as RC_Contact_First,
    bac.last_name as RC_Contact_Last,
    t1.price_range_lower_bound,
    t1.price_range_upper_bound,
    t2.AgentFirstName,
    t2.AgentLastName,
    bac.rc_city,
    bac.rc_state,
    t1.selling_address,
    ld.BankCustomer,
    lts.source, 
    slsag.first_contact_time,
    slsag.first_contact_delay_from_enrollment,
    slsag.accept_to_contact_delay,
    ld.velocifyID,
    case 
        when lower(cls.HB_Status) like '%closed%' then 'Closed'
        when lower(cls.HB_Status) like 'active%' then 'Active'
        when lower(cls.HB_Status) like '%pending%' then 'Pending'
        when lower(cls.HB_Status) like 'inactive%' then 'Inactive'
        when lower(cls.HB_Status) like 'routing%' then 'Routing'
        when lower(cls.HB_Status) like 'invited%' then 'Invited'
        when lower(cls.HB_Status) like 'new%' then 'Concierge'
        when lower(cls.HB_Status) like 'hold%' then 'On Hold'
    else 'Active' end as major_status,
    ld.branchName,
    ld.branchEmployee,
    bac.coverage,
    case when lower(LenderClosedWith) = lower(t1.bank_name) then 1 else 0 end as attached,
    slsag.lastPendingDate,
    inds.inactiveDate,
    inds.timetoInactive,
    inds.inactiveRole,
    slsag.last_agent_update,
    slsag.last_rc_update,
    ld.CitizensSalesForceEnrollment,
    cls.PendingCloseDate as pendingcloseDate,
    cls.PendingPropertyAddress as pendingpropertyaddress,
    cls.PropertyAddress as closeAddress,
    cls.FinanceCloseDate as Financeclosedate,
    t1.id as Networkid,
    ld.customerECI,
    bac.brokerageRoutingMethod,
    case when ca.id is null then 0
    else 1
    end as agent_assigned,
    case when ua1.EscalatedtoConcierge is null then 0
    else ua1.EscalatedtoConcierge
    end as EscalatedtoConcierge,
    case when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'zip' else nl.normalized_purchase_location::json->>'zip' end as zip,
          case when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'city' else nl.normalized_purchase_location::json->>'city' end as city,
          case when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'state' else nl.normalized_purchase_location::json->>'state' end as state,
    t3.lo_first_name,
    t3.lo_last_name,
    cls.HB_Status_Time,
    t1.contact_methods as contactmethod,
    note_content.OutofNetwork,
    note_content.FailC,
    note_content.FailD,
    slsag.first_agent_contact_time,
    case
      when t1.contact_methods is null then 'Do Not Contact'
      when t1.contact_methods ='' then 'Do Not Contact'
      when t1.contact_methods not like '%PHONE%' then 'Do Not Call'
      else 'Contact' 
    end as client_contact_approval,
    /*cast(nullif(replace(cp.HomesalePrice,',',''),'')as decimal) as HomePrice*/
    cp.HomesalePrice,
    t1.updated_at
    --CONCAT(date_part('month',cls.CloseDate),'/1/',date_part('year',cls.CloseDate)) as closeMonthYear
    --case when cls.closedate is not null then (t1.created - cls.closedate) end as DaysToClose
  FROM  {{ ref('stg_leads_filtered') }} t1
  left outer join {{ ref('normalized_lead_locations') }}  nl on t1.id = nl.lead_id 
  left outer join  current_assigment_cte ca on t1.id = ca.lead_id
  left outer join {{ source('public','lead_traffic_sources') }} lts on  t1.id = lts.lead_id 
  left outer join brokerage_assignments_cte bac on t1.id = bac.lead_id
  left outer join current_assigment__agent_notes_cte t2 on t1.id = t2.lead_id
  left outer join current_assigment_mlo_cte t3 on t1.id = t3.lead_id
  left outer join {{ ref('stg_lead_status_agg') }} slsag on  t1.id = slsag.lead_id
  left outer join user_assignments_cte ua1 on t1.id = ua1.lead_id
  left outer join {{ ref('stg_lead_data') }} ld on t1.id = ld.lead_id
  left outer join {{ ref('stg_lead_current_status_inactive') }} inds on t1.id = inds.id
  left outer join {{ ref('stg_notes_content') }} note_content on t1.id = note_content.lead_id
  left outer join {{ ref('stg_lead_statuses_details') }} cls on  t1.id = cls.lead_id 
  left outer join {{ ref('stg_lead_current_status_sales_closed') }} cp on t1.id = cp.lead_id
  WHERE cls.HB_Status <> 'Inactive Test Referral'
  {% if is_incremental() %}
		{%- if not var('incremental_catch_up') %}
    and t1.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% else %}
    and t1.updated >= CURRENT_DATE + interval '-60 day'
    {% endif %}
  {% endif %}

  {#- 
  WHERE cls.HB_Status <> 'Inactive Test Referral'
  --and bac.brokerage_code <> 'AL999'
  --and t1.bank_id = '3EEAEF5F-6F12-4B42-BB7B-3B0EB74E4B1F'
  --and t1.bank_id = 'a1443203-486e-42a1-8c2f-01af57d0295c'
  --and t2.email = 'collettewright@comcast.net'
  --and t1.id in (10284) --Use this to investigate specific lead_ids
  -#}
)

, final_cte AS (

  SELECT ld.*,cbsa.cbmsa FROM leads_data_v2 ld
  left join {{ source('public','cbsa_locations') }} cbsa
  on ld.zip = cbsa.zip

)
SELECT * FROM leads_data_v2
ORDER BY id desc
