{{
  config(
    materialized = 'incremental',
    unique_key = ['msg_id','status']
    )
}}
SELECT 
    msg_id,
    status,
    subject,
    to_email,
    opens_count,
    clicks_count,
    email_no,
    first_event_time,
    last_event_time,
    'true' AS is_invite
FROM 
    {{ ref('stg_pmc_sendgrid_invites') }}
UNION
SELECT
    msg_id,
    status,
    subject,
    to_email,
    opens_count,
    clicks_count,
    email_no,
    first_event_time,
    last_event_time,
    'false' AS is_invite
FROM
    {{ ref('stg_pmc_sg_followup') }}
  {% if is_incremental() %}
      WHERE last_event_time >= coalesce((select max(last_event_time) from {{ this }}), '1900-01-01')
    
  {% endif %}