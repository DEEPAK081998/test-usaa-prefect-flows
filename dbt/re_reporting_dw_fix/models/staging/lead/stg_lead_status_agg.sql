{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id',
    )
}}
WITH 
{% if is_incremental() %}
  updated_leads_cte AS (
    SELECT distinct lead_id FROM {{ ref('lead_status_updates') }}
    {%- if not var('incremental_catch_up') %}
    WHERE updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')+ interval '-2 day'
    {% else %}
    WHERE updated_at >= CURRENT_DATE + interval '-60 day'
    {% endif %}
  ),
{% endif %}

final_cte AS (
SELECT
    lead_id
    ,MAX(CASE WHEN role = 'AGENT' and category in ('PropertySell','PropertySearch') THEN created END) as last_agent_update
    ,MAX(CASE WHEN role = 'REFERRAL_COORDINATOR' and category in ('PropertySell','PropertySearch')THEN created END) as last_rc_update
    ,MAX(CASE WHEN role = 'REFERRAL_COORDINATOR' and lower(status) in ('new new referral')THEN created END) AS RC_assigned_time_unadjusted
    ,MAX(CASE WHEN category like 'Property%' and status = 'Pending Offer Accepted' THEN created END) AS lastPendingDate
    ,(MIN(CASE WHEN role = 'REFERRAL_COORDINATOR' and lower(status) in ('unassigned waiting agent assignment','new waiting agent assigment') THEN created END)  - interval '7 hour') as RC_accept_time
    ,MIN(CASE WHEN role = 'REFERRAL_COORDINATOR' and lower(status) in ('unassigned waiting agent assignment','new waiting agent assigment') THEN created END) as RC_accept_time_unadjusted
    ,(MIN(CASE WHEN category = 'Activities' and status = 'Outreach Click to Call' THEN created END)  - interval '7 hour' ) as first_contact_time
    ,MIN(CASE WHEN category = 'Activities' and status = 'Outreach Click to Call' THEN created END) as first_contact_time_unadjusted
    ,(MIN(CASE WHEN category = 'Activities' and status = 'Outreach Click to Call' and role = 'AGENT' THEN created END)  - interval '7 hour' ) as first_agent_contact_time
    ,MIN(CASE WHEN category = 'Activities' and status = 'Outreach Click to Call' and role = 'AGENT' THEN created END) as first_agent_contact_time_unadjusted
    ,COUNT(CASE WHEN category in ('PropertySell','PropertySearch') AND status = 'Active Agent Assigned' THEN 1 END) as AgentCount
    ,MIN(CASE WHEN category in ('PropertySell','PropertySearch') AND lower(status) like ('new new referral%') THEN created END) as enrollDate
    ,min(CASE WHEN status = 'Active Actively Searching' THEN created END) as actively_searching_status_time
    ,min(CASE WHEN status like '%Pending%' THEN created END) as pending_status_time
    ,MAX(updated_at) as updated_at
FROM
{{ ref('lead_status_updates') }}
{% if is_incremental() %}
  WHERE lead_id in (select updated_leads_cte.lead_id from updated_leads_cte)
{% endif %}
GROUP BY lead_id
)
{% if target.type == 'snowflake' %}
SELECT
*
,(DATEDIFF(second, '1970-01-01', first_contact_time_unadjusted) - DATEDIFF(second, '1970-01-01', RC_assigned_time_unadjusted))/60/60 as first_contact_delay_from_enrollment
,(DATEDIFF(second, '1970-01-01', first_contact_time_unadjusted) - DATEDIFF(second, '1970-01-01', RC_assigned_time_unadjusted))/60/60 - (DATEDIFF(second, '1970-01-01', RC_accept_time_unadjusted) - DATEDIFF(second, '1970-01-01', RC_assigned_time_unadjusted))/60/60 as accept_to_contact_delay
,(DATEDIFF(second, '1970-01-01', RC_accept_time_unadjusted) - DATEDIFF(second, '1970-01-01', RC_assigned_time_unadjusted))/60/60 as Accept_Time_Delay_Hrs
FROM final_cte
{% else %}
SELECT
*
,EXTRACT(EPOCH FROM (first_contact_time_unadjusted - RC_assigned_time_unadjusted))/60/60 as first_contact_delay_from_enrollment
,EXTRACT(EPOCH FROM (first_contact_time_unadjusted - RC_assigned_time_unadjusted))/60/60 - EXTRACT(EPOCH FROM (RC_accept_time_unadjusted - RC_assigned_time_unadjusted))/60/60 as accept_to_contact_delay
,EXTRACT(EPOCH FROM (RC_accept_time_unadjusted - RC_assigned_time_unadjusted))/60/60 as Accept_Time_Delay_Hrs
FROM final_cte
{% endif %}