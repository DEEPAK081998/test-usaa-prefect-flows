{{
  config(
    materialized = 'incremental',
    unique_key = ['id','bank_id'],
	indexes=[
      {'columns': ['id'], 'type': 'btree'},
	  {'columns': ['bank_id'], 'type': 'hash'},
	  {'columns': ['updated_at'], 'type': 'btree'}
	],
    tags=['rec'],
    enabled=false
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
   from {{ source('public', 'profile_assignments') }}  ba
   join most_recent_cte mt on mt.lead_id = ba.lead_id and mt.most_recent = ba.created
   left join {{ ref('brokerages') }} b on b.aggregate_id = ba.profile_aggregate_id
   left join {{ ref('brokerage_user') }} bu on bu.brokerage_code = b.brokerage_code
   left outer join brokerage_cte bq on b.brokerage_code = bq.brokerage_code
   left outer join brokerage_coverage_zips_cte z on b.brokerage_code = z.brokerage_code
   left join {{ ref('partner_user_profiles') }}  pup on pup.email = bu.email
)
, agent_notes_cte AS (
    SELECT pp.aggregate_id,pur.role,pp.email, pp.data::json->'additionalNotes' as AgentNotes
    , pp.first_name || ' ' || pp.last_name as fullName,pp.phone 
    FROM {{ ref('partner_user_profiles') }}  pp
        JOIN {{ ref('partner_user_relationships') }}  pr 
        ON pp.aggregate_id = pr.child_profile_id
        LEFT JOIN {{ ref('partner_user_roles') }}  pur
        on pp.id = pur.user_profile_id
    --WHERE pr.parent_profile_id= 1011
)

, current_assigment__agent_notes_cte  AS (
    SELECT ca.lead_id, p.email, p.phone,
           p.fullName as fullName,
           p.AgentNotes as OutofNetworkBrokerage
    FROM {{ ref('current_assignments') }}  ca
    LEFT OUTER JOIN agent_notes_cte p on ca.profile_aggregate_id = p.aggregate_id
    WHERE p.role = 'AGENT'
)

, current_assigment_mlo_cte AS (
    SELECT
     lead_id, pp.email, pp.phone as phone,
      pp.first_name || ' ' || pp.last_name  as fullName,
     pp.data::json->>'nmlsid' as NMLSid
    FROM {{ ref('current_assignments') }}  ca
    join {{ ref('partner_user_profiles') }}  pp
    on ca.profile_aggregate_id = pp.aggregate_id
    JOIN {{ ref('partner_user_relationships') }}  pr 
        ON pp.aggregate_id = pr.child_profile_id
    WHERE role = 'MLO'
)

, final_cte AS (
    select
        date(t1.created - interval '7 Hours'),
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
                    then t1.price_range_upper_bound::decimal 
                when t1.price_range_upper_bound is null or t1.price_range_lower_bound = 0
                    then t1.price_range_lower_bound::decimal 
                else ((t1.price_range_lower_bound::decimal +t1.price_range_upper_bound::decimal)/2) 
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
        case 
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'zip' 
            else nl.normalized_purchase_location::json->>'zip' 
        end as zip,
        case 
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'city' 
            else nl.normalized_purchase_location::json->>'city' 
        end as city,
        case 
            when t1.transaction_type = 'SELL' then nl.normalized_sell_location::json->>'state' 
            else nl.normalized_purchase_location::json->>'state' 
        end as state,
        t1.bank_id,
        t1.updated as updated_at
    FROM {{ ref('leads') }}  t1
    left outer join normalized_lead_locations nl on nl.lead_id = t1.id
    left outer join current_assigment_cte ca on ca.lead_id = t1.id
    left outer join brokerage_assignments_cte bac on bac.lead_id=t1.id
    left outer join current_assigment__agent_notes_cte t2 on t2.lead_id = t1.id
    left outer join current_assigment_mlo_cte t3 on t3.lead_id = t1.id
    left outer join {{ ref('stg_lead_statuses_details') }} cls on cls.lead_id = t1.id
    left outer join {{ ref('stg_lead_current_status_invited_at') }} invd on invd.lead_id = t1.id
    left outer join {{ ref('stg_lead_data') }} ld on ld.lead_id = t1.id
    {% if is_incremental() %}
    WHERE t1.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}

)
SELECT * FROM final_cte