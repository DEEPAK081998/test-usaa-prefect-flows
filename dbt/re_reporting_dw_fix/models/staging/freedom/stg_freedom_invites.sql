{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
	  tags=['mlo_invites']
    )
}}
WITH hist_invites AS(
{% if target.type == 'snowflake' %}
    SELECT * FROM {{ source('public', 'freedom_historical_invites') }}
{% else %}
    SELECT * FROM {{ source('dbolling_public', 'freedom_historical_invites') }}
{% endif %}
),
freedom_emails AS(
  SELECT DISTINCT 
    to_email
  FROM {{ ref('freedom_daily_sg') }}
),
freedom_dynamo_table AS(
  SELECT fdt.*
  FROM {{ ref('dynamodb_invite_table') }} fdt
  INNER JOIN freedom_emails
  ON lower(fdt.email) = freedom_emails.to_email
),
total_invites AS(
    SELECT *
    FROM hist_invites
    FULL JOIN freedom_dynamo_table fdt
    ON hist_invites.msg_id = fdt.invitation_id

),
merge_invites AS(
    SELECT
        coalesce(msg_id, invitation_id) AS id,
        coalesce(to_email, email) AS email,
        coalesce(first_event_time, invitation_dynamo) AS invite_date,
        coalesce(split_part(inviting_la,' ',1),lofirstname) AS lofirstname,
        coalesce(split_part(inviting_la,' ',2),lolastname) AS lolastname,
        agent,
        purchase_specialist,
        mlo_submission,
        sms_invite_permission,
        role,
        loemail,
        nmlsid,
        lastname,
        sendgrid,
        firstname,
        shorthash,
        utmsource,
        disclosure,
        phonenumber,
        utmcampaign,
        lophonenumber,
        mlosubmission,
        customerconnect,
        currentbankcustomer,
        confirmationrequired,
        updated_at
        
    FROM
        total_invites
  {% if is_incremental() %}
    where updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
  {% endif %}  
)
SELECT * FROM merge_invites