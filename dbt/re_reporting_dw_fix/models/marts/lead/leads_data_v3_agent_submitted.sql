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
 -- depends_on: {{ ref('leads') }}
 -- depends_on: {{ ref('current_assignments') }}
 -- depends_on: {{ ref('normalized_lead_locations') }}
 -- depends_on: {{ ref('lead_status_updates') }}
 -- depends_on: {{ ref('stg_lead_details') }}
WITH 

{% if is_incremental() %}
updated_cte as (

  SELECT DISTINCT t1.id as new_lead_id
  FROM {{ ref('leads') }} t1
  LEFT JOIN {{ ref('current_assignments') }} ca on t1.id = ca.lead_id
  LEFT JOIN {{ ref('normalized_lead_locations') }} nl on t1.id = nl.lead_id
  LEFT JOIN {{ ref('lead_status_updates') }} slsag on  t1.id = slsag.lead_id
  LEFT JOIN {{ ref('stg_lead_details') }} cls on  t1.id = cls.lead_id
  WHERE
    t1.updated >= {{ calculate_time_interval('coalesce((select max(updated_at) from ' ~ this ~ '), \'1900-01-01\')', '-', '2', 'day') }}
  AND
  (
    ca.created >= {{ calculate_time_interval('coalesce((select max(updated_at) from ' ~ this ~ '), \'1900-01-01\')', '-', '2', 'day') }}
    OR nl.updated >= {{ calculate_time_interval('coalesce((select max(updated_at) from ' ~ this ~ '), \'1900-01-01\')', '-', '2', 'day') }}
    OR slsag.updated_at >= {{ calculate_time_interval('coalesce((select max(updated_at) from ' ~ this ~ '), \'1900-01-01\')', '-', '2', 'day') }}
    OR cls.updated_at >= {{ calculate_time_interval('coalesce((select max(updated_at) from ' ~ this ~ '), \'1900-01-01\')', '-', '2', 'day') }}
  )

),
{% endif %}

brokerage_cte as (
    SELECT brokerage_code, qualification as brokerageRoutingMethod FROM {{ ref('brokerage_qualifications') }}
    WHERE qualification = 'directAgentQualified'
)

,brokerage_coverage_zips_cte as (
    SELECT brokerage_code, count(*) as coverage FROM {{ ref('brokerage_coverage_zips') }}
    GROUP BY  brokerage_code
)


, current_assigment_cte AS (
    SELECT lead_id, id FROM {{ ref('current_assignments') }}
    WHERE lower(role) = 'agent'
)


,pop_data AS (
{% if target.type == 'snowflake' %}
    SELECT aggregate_id, fp.value:phoneType::VARCHAR as PhoneType, fp.value:phoneNumber::VARCHAR as phoneNumber
    FROM {{ ref('partner_user_profiles') }} pup,
    lateral flatten(input => pup.phones) fp
{% else %}
    SELECT aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType, json_array_elements(phones)->>'phoneNumber' as phoneNumber
    FROM {{ ref('partner_user_profiles') }}
{% endif %}
)

,aop_cte AS (
    SELECT aggregate_id, min(PhoneNumber) as OfficePhone
    FROM pop_data pop
    WHERE lower(pop.Phonetype) = 'office'
    GROUP BY aggregate_id
)

,amp_cte AS (
    SELECT aggregate_id, min(PhoneNumber) as MobilePhone
    FROM pop_data pop
    WHERE lower(pop.Phonetype) = 'mobilephone'
    GROUP BY aggregate_id
)
, rc_cte AS (
    SELECT
        ca.lead_id,
        ca.profile_aggregate_id,
        rop.OfficePhone as rc_office_phone,
        rmp.MobilePhone as rc_mobile_phone,
        concat(pup.first_name,' ',pup.last_name) as RC_Name,
        pup.first_name,
        pup.last_name,
        pup.email as RC_Email,
        pup.phone as RC_Phone,
        {% if target.type == 'snowflake' %}
        pup.data:address.city::VARCHAR as rc_city,
        pup.data:address.state::VARCHAR as rc_state
        {% else %}
        pup.data->'address'->>'city' as rc_city,
        pup.data->'address'->>'state' as rc_state
        {% endif %}
    FROM {{ ref('current_assignments') }} ca 
        left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = ca.profile_aggregate_id
        left outer join aop_cte rop on rop.aggregate_id = ca.profile_aggregate_id
        left outer join amp_cte rmp on rmp.aggregate_id = ca.profile_aggregate_id
    WHERE ca.role = 'REFERRAL_COORDINATOR'
)

, agent_notes_cte AS (
{% if target.type == 'snowflake' %}
    SELECT pp.aggregate_id, pp.data:additionalNotes as AgentNotes
{% else %}
    SELECT pp.aggregate_id, pp.data->'additionalNotes' as AgentNotes
{% endif %}
    FROM {{ ref('partner_user_profiles') }} pp
        JOIN {{ ref('partner_user_relationships') }} pr 
        ON pp.id = pr.child_profile_id
    WHERE pr.parent_profile_id= 1011
)

, current_assigment__agent_notes_cte  AS (
    SELECT 
          ca.lead_id,
          ca.profile_aggregate_id,
          pup.email,
          pup.brokerage_code,
          coalesce(amp.MobilePhone,aop.OfficePhone) as phone,
          concat(pup.first_name,' ',pup.last_name) as fullName,
          pup.first_name as AgentFirstName,
          pup.last_name as AgentLastName,
          p.AgentNotes as OutofNetworkBrokerage
    FROM {{ ref('current_assignments') }} ca
    LEFT JOIN {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = ca.profile_aggregate_id
    LEFT OUTER JOIN agent_notes_cte p on p.aggregate_id = ca.profile_aggregate_id
    LEFT OUTER JOIN amp_cte amp on amp.aggregate_id = pup.aggregate_id
    LEFT OUTER JOIN aop_cte aop on aop.aggregate_id = pup.aggregate_id
    WHERE ca.role = 'AGENT'
)
, brokerage_assignments_cte AS (
  SELECT
    DISTINCT
    ca.lead_id, 
    ca.brokerage_code,
    b.full_name,
    z.coverage,
    bq.brokerageRoutingMethod
  FROM current_assigment__agent_notes_cte ca
  LEFT JOIN {{ ref('brokerages') }} b on b.brokerage_code = ca.brokerage_code
  LEFT OUTER JOIN brokerage_cte bq on ca.brokerage_code = bq.brokerage_code
  LEFT OUTER JOIN brokerage_coverage_zips_cte z on ca.brokerage_code = z.brokerage_code
)
, current_assigment_mlo_cte AS (
   SELECT
      DISTINCT
      ca.lead_id,
      pup.email,
      coalesce(mop.OfficePhone,mmp.MobilePhone) as phone,
      concat(pup.first_name,' ',pup.last_name) as fullName,
      pup.first_name as lo_first_name,
      pup.last_name as lo_last_name,
      {% if target.type == 'snowflake' %}
      pup.data:nmlsid::VARCHAR as NMLSid,
      {% else %}
      pup.data->>'nmlsid' as NMLSid,
      {% endif %}
      pup.aggregate_id as lo_aggregate_id
    FROM {{ ref('current_assignments') }} ca
    LEFT JOIN {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = ca.profile_aggregate_id
    LEFT OUTER JOIN amp_cte mmp on mmp.aggregate_id = pup.aggregate_id
    LEFT OUTER JOIN aop_cte mop on mop.aggregate_id = pup.aggregate_id
    WHERE ca.role = 'MLO'

)


, user_assignments_cte AS (
  SELECT
    lead_id,
    ({{ calculate_time_interval('min(case when role = \'REFERRAL_COORDINATOR\' then created end)', '-', '7', 'hour') }}) as RC_assign_time,
    ({{ calculate_time_interval('min(case when role = \'AGENT\' then created end)', '-', '7', 'hour') }}) as agent_assign_time,
    (min(case when role = 'AGENT' then created end)) as agent_assign_time_unadjusted,
    ({{ calculate_time_interval('min(case when role = \'MLO\' then created end)', '-', '7', 'hour') }}) as MLO_assign_time,
    (COUNT(case when role = 'REFERRAL_COORDINATOR'  and email = ('routing+prod@homestory.co') then 1 end )) as EscalatedtoConcierge
  FROM {{ ref('user_assignments') }}
  GROUP BY lead_id
)
, source_inactive_cte AS (
    SELECT
    {% if target.type != 'snowflake' %}
    distinct on (lead_id)
    {% endif %}
    lead_id, min(created) as inactiveDate, role as inactiveRole,max(updated_at) as updated_at
    FROM {{ ref('lead_status_updates') }}
    WHERE category in ('PropertySell','PropertySearch','ConciergeStatus') and status like 'Inactive%'
    GROUP BY lead_id, role
    {% if target.type == 'snowflake' %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY lead_id) = 1
    {% endif %}
)
, stg_lead_current_status_inactive as (
  SELECT
      l.id,
      ind.inactiveDate,
      ind.inactiveRole,
      {% if target.type == 'snowflake' %}
      DATEDIFF(DAY, l.created, ind.inactiveDate) as timetoInactive,
      {% else %}
      DATE_PART('day', ind.inactiveDate - l.created) as timetoInactive,
      {% endif %}
      COALESCE(ind.updated_at,l.updated) as updated_at
  FROM {{ ref('leads') }} l
  left outer join source_inactive_cte ind 
  on l.id = ind.lead_id
)
, stg_lead_current_status_sales_closed AS (
{% if target.type == 'snowflake' %}
  SELECT
    lsu.lead_id,
    lsu.data:salePrice::VARCHAR as HomesalePrice
  FROM {{ ref('lead_status_updates') }} lsu
  JOIN {{ ref('current_lead_statuses') }} cls on cls.lead_id = lsu.lead_id
  WHERE lsu.data:salePrice is not null  and cls.status like '%Closed%'
  GROUP BY lsu.data:salePrice::VARCHAR, lsu.lead_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lsu.lead_id ORDER BY lsu.lead_id) = 1
{% else %}
  SELECT
    distinct on (lsu.lead_id) lsu.lead_id,
    lsu.data->>'salePrice' as HomesalePrice
  FROM {{ ref('lead_status_updates') }} lsu
  JOIN {{ ref('current_lead_statuses') }} cls on cls.lead_id = lsu.lead_id
  WHERE lsu.data->'salePrice' is not null  and cls.status like '%Closed%'
  GROUP BY lsu.data->>'salePrice', lsu.lead_id
{% endif %}
)

, stg_lead_data AS (
{% if target.type == 'snowflake' %}
  SELECT
    lead_id,
    data:mloSubmission::VARCHAR as MLOSubmission,
    data:salesforceEnroll::VARCHAR as CitizensSalesForceEnrollment,
    data:currentBankCustomer as BankCustomer ,
    data:velocifyId as velocifyId,
    data:branchName as branchName,
    data:employeeName as branchEmployee,
    data:ECI as customerECI,
    data:closingProcess.rebate as rebate_obj,
    data:closingProcess.documentation as documentation_obj,
    data:freedom.VRLIdValue::VARCHAR as freedom_internal_id,
    data:SFOpportunityID::VARCHAR as pennymac_internal_id,
    updated as updated_at
  FROM {{ ref('leads_data') }}
{% else %}
  SELECT
    lead_id,
    data->>'mloSubmission' as MLOSubmission,
    data->>'salesforceEnroll' as CitizensSalesForceEnrollment,
    data->'currentBankCustomer' as BankCustomer ,
    data->'velocifyId' as velocifyId,
    data->'branchName' as branchName,
    data->'employeeName' as branchEmployee,
    data->'ECI' as customerECI,
    data->'closingProcess'->'rebate' as rebate_obj,
    data->'closingProcess'->'documentation' as documentation_obj,
    data->'freedom'->>'VRLIdValue' as freedom_internal_id,
    data->>'SFOpportunityID' as pennymac_internal_id,
    updated as updated_at
  FROM {{ ref('leads_data') }}
{% endif %}
)
{% if is_incremental() %}
, inner_reference AS (

  SELECT id as lead_id, MIN(first_time_wh) as first_time_wh
  FROM {{ this }}
  GROUP BY 1

)
{% endif %}
, leads_data_v2 AS (

  SELECT
    date({{ calculate_time_interval('t1.created', '-', '7', 'hour') }}) as date,
    t1.id,
    concat(t1.first_name,' ',t1.last_name) as client_name, --<--Use this when decrypted
    t1.email as client_email,
    replace(t1.phone,'+','') as client_phone,
    t2.fullName as agent_name,
    case when t2.email is null then null else t2.email end as agent_email,
    t2.phone as agent_phone,

    t3.fullName as lo_name,
    case
      when t1.bank_name = 'PennyMac' then replace(replace(t3.email,'.com',''),'@pennymac','@pnmac')
      else t3.email
    end as lo_email,
    t3.email as lo_email_final,
    t3.phone as lo_phone,

    t1.purchase_location,
    case when t1.purchase_time_frame = 1 then 90
    when t1.purchase_time_frame = 2 then 180 else 365 end as purchase_time_frame,
    t1.prequal,
    (
      case 
        when t1.price_range_lower_bound is null or t1.price_range_lower_bound = 0
          then t1.price_range_upper_bound 
        when t1.price_range_upper_bound is null or t1.price_range_upper_bound = 0
          then t1.price_range_lower_bound 
        else ((t1.price_range_lower_bound+t1.price_range_upper_bound)/2) 
        end
    ) as avg_price,
    t1.comments,
    t1.bank_name,
    t1.bank_id,

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
    cls.LsuCloseDate,
    cls.PropertyAddress,
    cls.PropertyPrice,
    cls.LenderClosedWith,

    cls.AgtCommPct,

    t1.created,
    slsag.RC_accept_time_unadjusted,
    ua1.agent_assign_time_unadjusted,
    ua1.agent_assign_time,
    slsag.Accept_Time_Delay_Hrs,
    {% if target.type == 'snowflake' %}
    (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.RC_accept_time_unadjusted))/60/60 as AgentAssign_Time_Delay_Hrs,
    {% else %}
    EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted - slsag.RC_accept_time_unadjusted))/60/60 as AgentAssign_Time_Delay_Hrs,
    {% endif %}
    CONCAT(date_part('month',t1.created),'/1/',date_part('year',t1.created)) as mnth_yr,
    t2.brokerage_code,	
    bac.full_name,	
    rc_info.rc_name,
    rc_info.rc_email,
    rc_info.rc_phone,	
    date_part('month',t1.created) as date_part,
    '' as attachment,
    cls.HB_Status,
    date_part('dow',t1.created) as DOW,
    {{ calculate_time_interval('t1.created', '-', '7', 'hour') }} as CST ,
    date({{ calculate_time_interval('t1.created', '-', '7', 'hour') }}) as CSTDate,
    date_part('hour',({{ calculate_time_interval('t1.created', '-', '7', 'hour') }})) as CSTHour,
    date_part('year',({{ calculate_time_interval('t1.created', '-', '7', 'hour') }})) as Year,
    date_part('week',({{ calculate_time_interval('t1.created', '-', '7', 'hour') }})) as WeekNum,
    case 
            when date_part('hour',({{ calculate_time_interval('t1.created', '-', '7', 'hour') }})) between 10 and 17 and date_part('dow',t1.created) between 1 and 5 then 'Business Hours'
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
    rc_info.first_name as RC_Contact_First,
    rc_info.last_name as RC_Contact_Last,
    t1.price_range_lower_bound,
    t1.price_range_upper_bound,
    t2.AgentFirstName,
    t2.AgentLastName,
    rc_info.rc_city,
    rc_info.rc_state,
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
    {% if target.type == 'snowflake' %}
    case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:zip::VARCHAR else nl.normalized_purchase_location:zip::VARCHAR end as zip,
          case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:city::VARCHAR else nl.normalized_purchase_location:city::VARCHAR end as city,
          case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:state::VARCHAR else nl.normalized_purchase_location:state::VARCHAR end as state,
    {% else %}
    case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'zip' else nl.normalized_purchase_location->>'zip' end as zip,
          case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'city' else nl.normalized_purchase_location->>'city' end as city,
          case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'state' else nl.normalized_purchase_location->>'state' end as state,
    {% endif %}
    t3.lo_first_name,
    t3.lo_last_name,
    cls.HB_Status_Time,
    t1.contact_methods as contactmethod,
    note_content.OutofNetwork,
    note_content.FailC,
    note_content.FailD,
    slsag.first_agent_contact_time,
    slsag.actively_searching_status_time,
    slsag.pending_status_time,
    case
      when t1.contact_methods is null then 'Do Not Contact'
      when t1.contact_methods ='' then 'Do Not Contact'
      when t1.contact_methods not like '%PHONE%' then 'Do Not Call'
      else 'Contact' 
    end as client_contact_approval,
    cls.date_marked_closed_final,
    t1.current_location,
    t1.sell_location,
    cls.category,
    /*cast(nullif(replace(cp.HomesalePrice,',',''),'')as decimal) as HomePrice*/
    cp.HomesalePrice,
    t1.updated_at,
    rc_info.rc_office_phone,
    rc_info.rc_mobile_phone,
    t3.lo_aggregate_id,
    t1.consumer_confirmed,
    case
    {% if target.type == 'snowflake' %}
      when RLIKE(cls.CloseDate::text, '^[0-9]{13}$', 'i') then to_timestamp(cls.CloseDate::text::bigint/1000)
      when cls.CloseDate::text = '0' then null
      when cls.CloseDate::text = '' then null
      else to_timestamp(REGEXP_SUBSTR(cls.CloseDate::text,'\\d+[-_]\\d+[-_]\\d+$'), 'yyyy-MM-dd') END AS normalizedclosedate,
    {% else %}
      when cls.CloseDate::text ~* '^[0-9]{13}$' then to_timestamp(cls.CloseDate::text::bigint/1000)
      when cls.CloseDate::text = '0' then null
      when cls.CloseDate::text = '' then null 
      else to_timestamp(substring(cls.CloseDate::text,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') END AS normalizedclosedate,
    {% endif %}
      t2.profile_aggregate_id as agent_aggregate_id,
    lts.medium as traffic_medium,
    lts.campaign as traffic_campaign,
    ua1.MLO_assign_time,
    t1.agent_submitted,
    case t1.bank_name
      when 'Freedom' then ld.freedom_internal_id
      when 'PennyMac' then ld.pennymac_internal_id
      else null
    end as partner_internal_id,
    {% if is_incremental() %}
    COALESCE(ir.first_time_wh,{{ current_date_time() }}) as first_time_wh,
    {{ current_date_time() }} as updated_in_wh,
    'Incremental' as type_of_load_wh
    {% else %}
    {{ current_date_time() }} as first_time_wh,
    {{ current_date_time() }} as updated_in_wh,
    'Full_Refresh' as type_of_load_wh
    {% endif %}
   
    --CONCAT(date_part('month',cls.CloseDate),'/1/',date_part('year',cls.CloseDate)) as closeMonthYear
    --case when cls.closedate is not null then (t1.created - cls.closedate) end as DaysToClose
  FROM  {{ ref('stg_leads_filtered') }} t1
  left outer join {{ ref('normalized_lead_locations') }} nl on t1.id = nl.lead_id 
  left outer join  current_assigment_cte ca on t1.id = ca.lead_id
  left outer join {{ ref('lead_traffic_sources') }} lts on  t1.id = lts.lead_id 
  left outer join brokerage_assignments_cte bac on t1.id = bac.lead_id
  left outer join rc_cte rc_info on t1.id = rc_info.lead_id
  left outer join current_assigment__agent_notes_cte t2 on t1.id = t2.lead_id
  left outer join current_assigment_mlo_cte t3 on t1.id = t3.lead_id
  left outer join {{ ref('stg_lead_status_agg') }} slsag on  t1.id = slsag.lead_id
  left outer join user_assignments_cte ua1 on t1.id = ua1.lead_id
  left outer join stg_lead_data ld on t1.id = ld.lead_id
  left outer join stg_lead_current_status_inactive inds on t1.id = inds.id
  left outer join {{ ref('stg_notes_content') }} note_content on t1.id = note_content.lead_id
  left outer join {{ ref('stg_lead_details') }} cls on  t1.id = cls.lead_id 
  left outer join stg_lead_current_status_sales_closed cp on t1.id = cp.lead_id
  {% if is_incremental() %}
  left join inner_reference ir on t1.id = ir.lead_id 
  {% endif %}
  WHERE cls.HB_Status <> 'Inactive Test Referral'
  {% if is_incremental() %}
   AND t1.id in (select new_lead_id from updated_cte)
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
, ph_cte AS (
  select
    bank_name as ph_bank_name,
      level_1_manager,
      level_1_manager_email,
      level_2_manager as ph_lm_name,
      level_2_manager_email as ph_lm_email,
      level_3_manager_name,
      level_3_manager_email,
      level_3_manager_region,
      level_4_manager_name as ph_slm_name,
      level_4_manager_email as ph_slm_manager,
      level_4_manager_region,
      level_5_manager_name as ph_dd_name,
      level_5_manager_email as ph_dd_email,
      level_5_manager_region as ph_dd_division,
      mlo_nmlsid as lo_nmlsid,
      mlo_email_join as ph_lo_email
      from {{ ref('all_partner_hierarchy') }} ph

)
, int_cte as (
  select
  *
  from leads_data_v2 nc
  left join ph_cte ph on case when nc.bank_name <>'Citizens' then ph.ph_lo_email = nc.lo_email else ph.lo_nmlsid = nc.nmlsid end
),
att_cte as(
  select
    i.*,
    n.attachment_code,
    n.attachment_reason,
    COALESCE(lsud.system_invite_date,'1900-01-01'::timestamp) AS system_invite_date,
    CASE
      WHEN i.bank_name = 'Citizens' and i.date >'2023-02-21' THEN lsud.pre_system_enroll_date
      WHEN i.bank_name = 'PennyMac' and i.date>='2023-07-11' then lsud.pre_system_enroll_date
      ELSE i.date 
    END as system_enroll_date
  from
    int_cte i
  left join {{ ref('stg_attachment_reasons') }} n
  ON i.id=n.lead_id
  left join {{ ref('stg_lead_status_dates') }} lsud 
  on i.id = lsud.lead_id
)
--, final_cte AS (
--
--  SELECT ld.*,cbsa.cbmsa FROM int_cte ld
--  left join {{ ref('cbsa_locations') }} cbsa
--  on ld.zip = cbsa.zip
--
--)
SELECT *,
    COALESCE({{ local_convert_timezone('system_invite_date','CST') }},'1900-01-01'::timestamp) as reporting_invite_date,
    {{ local_convert_timezone('system_enroll_date','CST') }} as reporting_enroll_date
     FROM att_cte
  WHERE agent_submitted = 'true'
ORDER BY id desc
