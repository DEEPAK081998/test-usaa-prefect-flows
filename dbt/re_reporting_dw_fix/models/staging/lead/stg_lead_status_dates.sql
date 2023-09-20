{{
  config(
    materialized = 'view',
    )
}}
SELECT
    lead_id,
    MIN(case when (status ='Invited Unaccepted' and lsu.role='MLO') or lower(status) like ('invite%') then lsu.created end) as System_Invite_Date,
    MIN(case when (status ='New Waiting Agent Select' and lsu.role='MLO') or lower(status) not like ('invite%') then lsu.created end) as System_Enroll_Date,
    MIN(case when lower(status) not like ('invite%') and category in ('PropertySell','PropertySearch') then lsu.created end) as pre_System_Enroll_Date,
    MIN(case when (status ='Invited Unaccepted' and lsu.role='MLO') or lower(status) like ('invite%') then lsu.created end) as reporting_invite_date,
    MIN(case when (status ='New Waiting Agent Select' and lsu.role='MLO') or lower(status) not like ('invite%') then lsu.created end) as reporting_enroll_date
FROM 
    {{ ref('lead_status_updates') }} lsu
left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = lsu.profile_aggregate_id
--where role = 'MLO'
GROUP BY lead_id
order by lead_id asc