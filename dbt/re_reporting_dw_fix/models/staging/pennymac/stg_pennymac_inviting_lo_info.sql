with cte AS (
    With lo_cte AS (
    select ca.lead_id,
    ca.profile_aggregate_id as mlo_aggregate_id,
    CONCAT(pup.first_name , ' ' , pup.last_name) AS inviting_lo,
    pup.first_name as inviting_lo_firstname,
    pup.last_name as inviting_lo_lastname,
    replace(replace(pup.email,'@pennymac.com','@pnmac.com'),'@pennymac','@pnmac.com') as inviting_lo_email, 
    row_number() over (partition by ca.lead_id order by ca.created asc) as row_id,
    {% if target.type == 'snowflake' %}
    CAST(pup.phones[0].phoneNumber::VARCHAR AS TEXT) as inviting_lo_phone,
    parse_json(data) as data,
    CAST(pup.data:nmlsid::VARCHAR AS TEXT) as mlo_nmlsid,
    {% else %}
    pup.phones->0->>'phoneNumber' as inviting_lo_phone,
    pup.data->>'nmlsid' as mlo_nmlsid,
    {% endif %}
    pup.aggregate_id as inviting_lo_aggregate_id
    from {{ ref('current_assignments') }} ca 
    left join partner_user_profiles pup on pup.aggregate_id = ca.profile_aggregate_id
    where ca.role = 'MLO'
)
,
inviting_lo_cte AS (
SELECT 
    lsu.lead_id,
    loc.inviting_lo_email,
    loc.inviting_lo,
    loc.inviting_lo_firstname,
    loc.inviting_lo_lastname,
    loc.inviting_lo_phone,
    loc.mlo_aggregate_id as inviting_aggregate_id,
    loc.mlo_nmlsid as inviting_nmlsid,
    loc.inviting_lo_aggregate_id 
  
FROM 
    {{ ref('lead_status_updates') }} lsu
    left join lo_cte loc on loc.lead_id = lsu.lead_id
where lsu.status like '%Invited%' and row_id = 1
order by lead_id ASC
)
select * from inviting_lo_cte
)

select cte.*
from cte cte
left join leads_data_v3 t1 on t1.id = cte.lead_id 
where t1.bank_name = 'PennyMac'
