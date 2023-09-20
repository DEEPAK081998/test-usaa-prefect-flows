{{
  config(
    enabled=false
    )
}}
WITH invites as(
    SELECT
        to_email,
        subject,
        min(last_event_time) as invite_date,
        max(last_event_time) as last_event_date,
        max(opens_count) as opens_count,
        max(clicks_count) as clicks_count
    FROM {{ source('public', 'freedom_sendgrid') }}
    GROUP BY to_email, subject
)
SELECT 
    to_email,
    MAX(invite_date) as invite_date,
    MAX(last_event_date) last_event_date,
    SUM(opens_count) opens_count,
    SUM(clicks_count) clicks_count
FROM invites
GROUP BY to_email
ORDER BY invite_date DESC