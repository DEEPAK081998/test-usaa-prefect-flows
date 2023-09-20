WITH activities AS(     
select    
    a.id,
    opened,
    "to",
    "from",
    delivered,
    delivery_id,
    CASE WHEN (pup.first_name IS NOT NULL AND pup.last_name IS NOT NULL) THEN 
        CONCAT(pup.first_name, ' ', pup.last_name) ELSE NULL END AS customer_name,
    campaign,
    campaign_id,
    cio_id,
    reason,
    href,
    a.{{  special_column_name_formatter('name') }},
    a.{{  special_column_name_formatter('type') }},
    {{  special_column_name_formatter('timestamp') }},
    customer_id,
    delivery_type,
    cust_email,
    cust_cio_id,
    lb.bank_name,
    pup.first_name,
    pup.last_name,
    pup.email as profile_email,
    pup.phone profile_phone,
    leads.id AS lead_id,
    leads.bank_id,
    pur.role,
    c.id as historical_campaign_id
from
    {{ ref('base_cio_activities') }} a
LEFT JOIN {{ ref('partner_user_profiles') }} pup
ON a.customer_id=cast(pup.aggregate_id AS TEXT)
LEFT JOIN {{ ref('leads') }} leads
on a.customer_id=cast(leads.aggregate_id AS TEXT)
LEFT JOIN {{ ref('stg_lead_banks') }} lb
on pup.partner_id=lb.bank_id
LEFT JOIN {{ ref('partner_user_roles') }} pur
on pup.id=pur.user_profile_id 
LEFT JOIN {{ ref('base_cio_campaigns') }} c
ON a.campaign=c.name
)
select 
    id,
    opened,
    a.{{ keyword_formatter('to') }},
    a.{{ keyword_formatter('from') }},
    delivered,
    delivery_id,
    customer_name,
    campaign,
    coalesce(cast(campaign_id as bigint), historical_campaign_id) as campaign_id,
    cio_id,
    reason,
    href,
    a.name,
    a.type,
    a.timestamp,
    customer_id,
    delivery_type,
    cust_email,
    cust_cio_id,
    bank_name,
    first_name,
    last_name,
    profile_email,
    profile_phone,
    lead_id,
    bank_id,
    a.role
from activities a