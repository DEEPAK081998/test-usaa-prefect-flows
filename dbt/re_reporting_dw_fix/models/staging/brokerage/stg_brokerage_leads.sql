{{
  config(
    materialized = 'incremental',
    unique_key = ['id','brokerage_code','rc_name'],
    indexes=[
	  {'columns': ['id'], 'type': 'btree'},
      {'columns': ['brokerage_code'], 'type': 'btree'}
	]
    )
}}
SELECT DISTINCT
    t1.id,
    bac.brokerage_code,
    bac.full_name,
    bac.RC_name,
    bac.RC_Email,
    bac.RC_phone,
    bac.rc_city,
    bac.rc_state,
    bac.rc_zip,
    bac.coverage,
    bac.brokeragequalification,
    bac.agentdirectDate,
    bac.agentcount,
    bac.eligibleAgentCount,
    case when lower(cls.HB_Status) like '%unassigned%' then 1 else 0 end as unassignedEnrollments,
    case when lower(cls.HB_Status) like '%new%' then 1 else 0 end as unacceptedEnrollments,
    {{ current_date_time() }} AS updated_at
FROM
            {{ ref('leads') }} t1
            left outer join {{ ref('stg_brokerage_assigments') }} bac on bac.lead_id = t1.id
            left outer join {{ ref('stg_lead_current_lead_statuses_agg') }} cls on cls.lead_id = t1.id
WHERE
    t1.id not in {{ TestLeads() }} --Excludes TestLeads/KnownFakes
    and t1.bank_id not in ('25A363D4-0BE0-4001-B9FB-A2F8BA91170C') --Excludes TD
    and t1.created > '2017-09-01'::date
    and cls.HB_Status <> 'Inactive Test Referral'
    {% if is_incremental() %}
      and t1.updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}