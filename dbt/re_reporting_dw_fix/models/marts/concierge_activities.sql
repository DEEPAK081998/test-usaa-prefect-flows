SELECT
  lsu.lead_id
  ,date({{ calculate_time_interval('t1.created', '-', '7', 'hour') }}) as enrollDate
  ,date({{ calculate_time_interval('lsu.created', '-', '7', 'hour') }}) as updateDate
  ,lsu.category
  ,lsu.status as activity
  ,pup.email as concierge
  {% if target.type == 'snowflake' %}
  ,pup.first_name as conciergeFirstName
  ,pup.last_name as conciergeLastName
  ,lsu.data:additionalComments::VARCHAR as additionalComments
  {% else %}
  ,pup.first_name as conciergeFirstName
  ,pup.last_name as conciergeLastName
  ,lsu.data->>'additionalComments'
  {% endif %}
  ,lb.bank_name
FROM {{ ref('lead_status_updates') }} lsu JOIN {{ ref('leads') }} t1 ON lsu.lead_id = t1.id
LEFT join {{ ref('partner_user_profiles') }} pup
ON lsu.profile_aggregate_id = pup.aggregate_id
JOIN {{ ref('stg_lead_banks') }} lb ON t1.bank_id = lb.bank_id
WHERE lsu.category = 'Activities' AND lsu.role = 'ADMIN'