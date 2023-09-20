{{
  config(
    materialized = 'incremental',
    unique_key = ['id','bank_id'],
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
    lateral flatten (input => pup.phones) fp
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
          pup.email,
          coalesce(amp.MobilePhone,aop.OfficePhone) as phone,
          concat(pup.first_name,' ',pup.last_name) as fullName
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
      {% if target.type == 'snowflake' %}
      pup.data:nmlsid::VARCHAR as NMLSid
      {% else %}
      pup.data->>'nmlsid' as NMLSid
      {% endif %}
    FROM {{ ref('current_assignments') }} ca
    LEFT JOIN {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = ca.profile_aggregate_id
    LEFT OUTER JOIN amp_cte mmp on mmp.aggregate_id = pup.aggregate_id
    LEFT OUTER JOIN aop_cte mop on mop.aggregate_id = pup.aggregate_id
    WHERE ca.role = 'MLO'

)

, final_cte AS (
    select
        date(t1.created - interval '7 Hours') as created,
        (invd.inviteDate - interval '7 Hours') as invitedDate,
        t1.id,
        t1.first_name as client_first_name,
        t1.last_name as client_last_name,
        t1.email as client_email,
        replace(t1.phone,'+','') as client_phone,
        t2.fullName as agent_name,
        t2.email as agent_email,
        t2.phone as agent_phone,
        t3.fullName as lo_name,
        t3.email as lo_email,
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
        cls.Category,
        cls.LenderClosedWith,
        ld.CitizensSalesForceEnrollment,
        case 
            when lower (ld.CitizensSalesForceEnrollment) = 'true' then 'true'
            when lower (ld.MLOSubmission) = 'true' then 'true'
            when ld.MLOSubmission is null then 'False'
            else ld.MLOSubmission 
        end as MLOSubmission,
        {% if target.type == 'snowflake' %}
        case
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location:zip::VARCHAR
            else nl.normalized_purchase_location:zip::VARCHAR
        end as zip,
        case
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location:city::VARCHAR
            else nl.normalized_purchase_location:city::VARCHAR
        end as city,
        case
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location:state::VARCHAR
            else nl.normalized_purchase_location:state::VARCHAR
        end as state,
        {% else %}
        case 
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'zip' 
            else nl.normalized_purchase_location->>'zip' 
        end as zip,
        case 
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'city' 
            else nl.normalized_purchase_location->>'city' 
        end as city,
        case 
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location->>'state' 
            else nl.normalized_purchase_location->>'state' 
        end as state,
        {% endif %}
        t1.bank_id,
        t1.updated as updated_at
    FROM {{ ref('leads') }} t1
    left outer join {{ ref('normalized_lead_locations') }} nl on nl.lead_id = t1.id
    left outer join current_assigment_cte ca on ca.lead_id = t1.id
    left outer join brokerage_assignments_cte bac on bac.lead_id=t1.id
    left outer join current_assigment__agent_notes_cte t2 on t2.lead_id = t1.id
    left outer join current_assigment_mlo_cte t3 on t3.lead_id = t1.id
    left outer join {{ ref('stg_lead_details') }} cls on cls.lead_id = t1.id
    left outer join {{ ref('stg_lead_current_status_invited_at') }} invd on invd.lead_id = t1.id
    left outer join {{ ref('stg_lead_data') }} ld on ld.lead_id = t1.id
    {% if is_incremental() %}
    WHERE t1.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}

)
SELECT distinct 
* 
FROM final_cte