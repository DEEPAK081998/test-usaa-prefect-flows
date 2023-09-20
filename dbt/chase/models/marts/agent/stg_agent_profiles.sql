{{
  config(
    materialized = 'table',
    enabled=true
    )
}}
select capr.*,
pup.aggregate_id
 from {{ ref('agent_completed_profile_report') }} capr
 left join {{ ref('partner_user_profiles') }}  pup on pup.id = capr.id