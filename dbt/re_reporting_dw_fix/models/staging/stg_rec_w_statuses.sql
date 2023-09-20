{{
  config(
    materialized = 'incremental',
    unique_key = ['id','bank_id','status', 'event_step', 'created'],
	indexes=[
      {'columns': ['id'], 'type': 'btree'},
	  {'columns': ['bank_id'], 'type': 'hash'},
	  {'columns': ['updated_at'], 'type': 'btree'}
	],
    tags=['rec']
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
    SELECT lead_id, max(created) as most_recent FROM {{ ref('brokerage_assignments') }}
    GROUP BY lead_id
)
, current_assigment_cte AS (
    SELECT lead_id, id FROM {{ ref('current_assignments') }}
    WHERE lower(role) = 'agent'
)
, brokerage_assignments_cte AS (
    SELECT
      ba.lead_id,
      ba.brokerage_code,
      b.full_name,
      concat(pup.first_name,' ',pup.last_name) as RC_Name,
      pup.first_name, pup.last_name,
      pup.email as RC_Email, 
      pup.phone as RC_Phone,
      {% if target.type == 'snowflake' %}
      pup.data:address.city as rc_city,
      pup.data:address.state as rc_state,
      {% else %}
      pup.data->'address'->'city' as rc_city,
      pup.data->'address'->'state' as rc_state,
      {% endif %}
      z.coverage,
      bq.brokerageRoutingMethod
   from {{ ref('brokerage_assignments') }}  ba
   join most_recent_cte mt on mt.lead_id = ba.lead_id and mt.most_recent = ba.created
   left join {{ ref('brokerages') }} b on b.brokerage_code = ba.brokerage_code
   left join {{ ref('brokerage_user') }} bu on bu.brokerage_code = ba.brokerage_code
   left outer join brokerage_cte bq on ba.brokerage_code = bq.brokerage_code
   left outer join brokerage_coverage_zips_cte z on ba.brokerage_code = z.brokerage_code
   left join {{ ref('partner_user_profiles') }} pup on pup.email = bu.email
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
, current_assigment__agent_notes_cte  AS (
    SELECT 
          ca.lead_id,
          ca.profile_aggregate_id,
          pup.email,
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

, lead_data_cte AS (
{% if target.type == 'snowflake' %}
    select lead_id, data:mloSubmission::VARCHAR as MLOSubmission
    from {{ ref('leads_data') }}
{% else %}
    select lead_id, data->>'mloSubmission' as MLOSubmission
    from {{ ref('leads_data') }}
{% endif %}
)

, user_assignments_raw_cte AS (
  SELECT
    lead_id,
    ({{ calculate_time_interval('min(case when role = \'REFERRAL_COORDINATOR\' then created end)', '-', '7', 'hour') }}) as RC_assign_time,
    ({{ calculate_time_interval('min(case when role = \'AGENT\' then created end)', '-', '7', 'hour') }}) as agent_assign_time,
    (min(case when role = 'AGENT' then created end)) as agent_assign_time_unadjusted,
    ({{ calculate_time_interval('min(case when role = \'MLO\' then created end)', '-', '7', 'hour') }}) as MLO_assign_time,
    min(case when role = 'MLO' then created end) as MLO_assign_time_unadjusted
  FROM {{ ref('user_assignments') }}
  GROUP BY lead_id
)

, user_assignments_cte AS (

{% if target.type == 'snowflake' %}
    SELECT *,
    (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 as agentaccepttime,
    case  when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 is null then 'unassigned'
                    when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 < 21 then '0-20 min'
                    when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 < 61 then '21-60 min'
                    when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 < 121 then '1-2 hrs'
                    when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 < 241 then '2-4 hrs'
                    when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 < 481 then '4-8 hrs'
                    when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 < 1441 then '8-24 hrs'
                    when (DATEDIFF(second, '1970-01-01', agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', MLO_assign_time_unadjusted))/60 < 2880 then '24-48 hrs'
                    else '48 hrs+'
                    end	as agentaccepttimebuckets
    FROM user_assignments_raw_cte
{% else %}
    SELECT *,
    EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 as agentaccepttime,
    case  when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 is null then 'unassigned'
                    when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 < 21 then '0-20 min'
                    when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 < 61 then '21-60 min'
                    when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 < 121 then '1-2 hrs'
                    when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 < 241 then '2-4 hrs'
                    when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 < 481 then '4-8 hrs'
                    when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 < 1441 then '8-24 hrs'
                    when EXTRACT(EPOCH FROM (agent_assign_time_unadjusted - MLO_assign_time_unadjusted))/60 < 2880 then '24-48 hrs'
                    else '48 hrs+'
                    end	as agentaccepttimebuckets
    FROM user_assignments_raw_cte
{% endif %}
)
, int_cte AS (
    SELECT
        date({{ calculate_time_interval('t1.created', '-', '7', 'hour') }}) as date,
        slsag.enrollDate,
        to_char(slsag.enrollDate,'Day') as enrollmentweekday,
        ({{ calculate_time_interval('invd.inviteDate', '-', '7', 'hour') }}) as invitedDate,
        t1.id,
        t1.first_name as client_first_name,
        t1.last_name as client_last_name,
        t1.email as client_email,
        replace(t1.phone,'+','') as client_phone,
        t2.fullName as agent_name,
        case when t2.email is null then null else t2.email end as agent_email,
        t2.phone as agent_phone,
        t3.fullName as lo_name,
        case when t3.email is null then null else t3.email end as lo_email,
        t3.phone as lo_phone,
        t1.purchase_location,
        t1.sell_location,
        t1.current_location,
        case
            when t1.purchase_time_frame = 1 then 90
            when t1.purchase_time_frame = 2 then 180
            else 365 
        end as purchase_time_frame,
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
        case 
            when t1.transaction_type = 'PURCHASE' then 'BUY'
            when t1.transaction_type = 'BOTH' then 'BUY'
            else t1.transaction_type 
        end as transaction_type,
        t1.price_range_lower_bound,
        t1.price_range_upper_bound,
        cls.HB_Status,
        cls.Category as HB_Status_Category,
        ld.CitizensSalesForceEnrollment,
        case 
            when lower (ld.CitizensSalesForceEnrollment) = 'true' then 'true'
            when lower (ld.MLOSubmission) = 'true' then 'true'
            when ld.MLOSubmission is null then 'False'
            else ld.MLOSubmission 
        end as MLOSubmission,
        {% if target.type == 'snowflake' %}
        case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:zip::VARCHAR else nl.normalized_purchase_location:zip::VARCHAR end as zip,
        case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:city::VARCHAR else nl.normalized_purchase_location:city::VARCHAR end as city,
        case when t1.transaction_type = 'SELL' then nl.normalized_sell_location:state::VARCHAR else nl.normalized_purchase_location:state::VARCHAR end as state,
        {% else %}
        case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'zip' else nl.normalized_purchase_location->>'zip' end as zip,
        case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'city' else nl.normalized_purchase_location->>'city' end as city,
        case when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'state' else nl.normalized_purchase_location->>'state' end as state,
        {% endif %}
        lsu.status,
        lsu.created,
        lsu.category as Status_Category,
        ua1.agent_assign_time,
        ua1.agent_assign_time_unadjusted,
        ua1.MLO_assign_time,
        ua1.MLO_assign_time_unadjusted,
        slsag.first_contact_time,
        note_content.OutofNetwork,
        slsag.AgentCount,
        case 
            when slsag.AgentCount = 1 then 'Single Assignment'
            when slsag.AgentCount > 1 then 'Multiple Assignments'
            else 'Unassigned'
        end as AgentAssigments,
        slsag.first_contact_delay_from_enrollment,
        {% if target.type == 'snowflake' %}
        ua1.agentaccepttime,
        (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 as enrolltoaccepttime,
        case when ua1.agent_assign_time_unadjusted is null then 1 else 0 end as is_unassigned,
        ua1.agentaccepttimebuckets,
        case  when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 is null then 'unassigned'
                        when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 < 21 then '0-20 min'
                        when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 < 61 then '21-60 min'
                        when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 < 121 then '1-2 hrs'
                        when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 < 241 then '2-4 hrs'
                        when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 < 481 then '4-8 hrs'
                        when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 < 1441 then '8-24 hrs'
                        when (DATEDIFF(second, '1970-01-01', ua1.agent_assign_time_unadjusted) - DATEDIFF(second, '1970-01-01', slsag.enrollDate))/60 < 2880 then '24-48 hrs'
                        else '48 hrs+'
                        end	as enrolltoaccepttimebuckets,
        {% else %}
        /*EXTRACT(EPOCH FROM (ctc.first_contact_time_unadjusted - fat.RC_assigned_time_unadjusted))/60/60 - EXTRACT(EPOCH FROM (ls1.RC_accept_time_unadjusted - fat.RC_assigned_time_unadjusted))/60/60 as accept_to_contact_delay,*/
        ua1.agentaccepttime,
        EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 as enrolltoaccepttime,
        case when ua1.agent_assign_time_unadjusted is null then 1 else 0 end as is_unassigned,
        ua1.agentaccepttimebuckets,	
        case  when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 is null then 'unassigned'
                        when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 < 21 then '0-20 min'
                        when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 < 61 then '21-60 min'
                        when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 < 121 then '1-2 hrs'
                        when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 < 241 then '2-4 hrs'
                        when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 < 481 then '4-8 hrs'
                        when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 < 1441 then '8-24 hrs'	
                        when EXTRACT(EPOCH FROM (ua1.agent_assign_time_unadjusted-slsag.enrollDate))/60 < 2880 then '24-48 hrs'	
                        else '48 hrs+'	
                        end	as enrolltoaccepttimebuckets,
        {% endif %}
        t1.bank_id,
        t3.lo_aggregate_id,
        t3.lo_first_name,
        t3.lo_last_name,
        t3.nmlsid,
        t2.OutofNetworkBrokerage,
        t2.AgentFirstName,
        t2.AgentLastName,
        bn.bank_name,
        t1.updated as updated_at,
        t1.agent_submitted,
        RANK () OVER (PARTITION BY lsu.lead_id ORDER BY lsu.created) event_step
    FROM {{ ref('leads') }} t1
        left outer join {{ ref('lead_status_updates') }} lsu on lsu.lead_id = t1.id
        left outer join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = t1.id
        left outer join current_assigment_cte ca on ca.lead_id = t1.id
        left outer join brokerage_assignments_cte bac on bac.lead_id=t1.id
        left outer join current_assigment__agent_notes_cte t2 on t2.lead_id = t1.id
        left outer join current_assigment_mlo_cte t3 on t3.lead_id = t1.id
        left outer join {{ ref('stg_lead_details') }} cls on cls.lead_id = t1.id
        left outer join {{ ref('stg_lead_current_status_invited_at') }} invd on invd.lead_id = t1.id
        left outer join {{ ref('stg_lead_data') }} ld on ld.lead_id = t1.id
        left outer join user_assignments_cte ua1 on ua1.lead_id = t1.id
        left outer join {{ ref('stg_lead_status_agg') }} slsag on  t1.id = slsag.lead_id
        left outer join {{ ref('stg_notes_content') }} note_content on t1.id = note_content.lead_id
        left outer join {{ ref('stg_lead_banks') }} bn on t1.bank_id = bn.bank_id
    {% if is_incremental() %}
    WHERE t1.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)
, final_cte AS (
  SELECT
    i.*,
    COALESCE(lsud.system_invite_date,'1900-01-01'::timestamp) AS system_invite_date,
    CASE
      WHEN i.bank_name = 'Citizens' and i.date >'2023-02-21' THEN lsud.pre_system_enroll_date
      ELSE i.date
    END as system_enroll_date
  FROM  int_cte i
  LEFT JOIN {{ ref('stg_lead_status_dates') }} lsud
  on i.id = lsud.lead_id
),
dedup AS(
SELECT DISTINCT
    agent_submitted,
    date,
    enrolldate,
    enrollmentweekday,
    inviteddate,
    id,
    client_first_name,
    client_last_name,
    client_email,
    client_phone,
    agent_name,
    agent_email,
    agent_phone,
    lo_name,
    lo_email,
    lo_phone,
    purchase_location,
    sell_location,
    current_location,
    purchase_time_frame,
    prequal,
    avg_price,
    transaction_type,
    price_range_lower_bound,
    price_range_upper_bound,
    hb_status,
    hb_status_category,
    citizenssalesforceenrollment,
    mlosubmission,
    zip,
    city,
    state,
    status,
    created,
    status_category,
    agent_assign_time,
    agent_assign_time_unadjusted,
    mlo_assign_time,
    mlo_assign_time_unadjusted,
    first_contact_time,
    outofnetwork,
    agentcount,
    agentassigments,
    cast(outofnetworkbrokerage AS TEXT) as outofnetworkbrokerage,
    first_contact_delay_from_enrollment,
    agentaccepttime,
    enrolltoaccepttime,
    is_unassigned,
    agentaccepttimebuckets,
    enrolltoaccepttimebuckets,
    bank_id,
    lo_aggregate_id,
    lo_first_name,
    lo_last_name,
    nmlsid,
    agentfirstname,
    agentlastname,
    bank_name,
    updated_at,
    event_step,
    system_invite_date,
    system_enroll_date,
    COALESCE({{ local_convert_timezone('system_invite_date','CST') }},'1900-01-01'::timestamp) as reporting_invite_date,
    {{ local_convert_timezone('system_enroll_date','CST') }} as reporting_enroll_date FROM final_cte
)
SELECT 
    agent_submitted,
    date,
    enrolldate,
    enrollmentweekday,
    inviteddate,
    id,
    client_first_name,
    client_last_name,
    client_email,
    client_phone,
    agent_name,
    agent_email,
    agent_phone,
    lo_name,
    lo_email,
    lo_phone,
    purchase_location,
    sell_location,
    current_location,
    purchase_time_frame,
    prequal,
    avg_price,
    transaction_type,
    price_range_lower_bound,
    price_range_upper_bound,
    hb_status,
    hb_status_category,
    citizenssalesforceenrollment,
    mlosubmission,
    zip,
    city,
    state,
    status,
    created,
    status_category,
    agent_assign_time,
    agent_assign_time_unadjusted,
    mlo_assign_time,
    mlo_assign_time_unadjusted,
    first_contact_time,
    outofnetwork,
    agentcount,
    agentassigments,
    {{ parse_json('outofnetworkbrokerage') }} as outofnetworkbrokerage,
    first_contact_delay_from_enrollment,
    agentaccepttime,
    enrolltoaccepttime,
    is_unassigned,
    agentaccepttimebuckets,
    enrolltoaccepttimebuckets,
    bank_id,
    lo_aggregate_id,
    lo_first_name,
    lo_last_name,
    nmlsid,
    agentfirstname,
    agentlastname,
    bank_name,
    updated_at,
    event_step,
    system_invite_date,
    system_enroll_date,
    reporting_invite_date,
    reporting_enroll_date 
    FROM 
        dedup