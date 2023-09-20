{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
	  tags=['mlo_invites']
    )
}}
WITH
 
sofi_emails AS(
  SELECT DISTINCT 
    to_email
  FROM 
    {{ ref('stg_sofi_sg_invites') }}
),

sofi_dynamo_table AS(
  SELECT dit.*
  FROM {{ ref('dynamodb_invite_table') }} dit
  INNER JOIN sofi_emails
  ON lower(dit.email) = lower(sofi_emails.to_email)
),
cols_invites AS(
    SELECT
        invitation_id AS id,
        email,
        invitation_dynamo AS invite_date,
        lofirstname,
        lolastname,
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
        --created_at,
        updated_at
    FROM
        sofi_dynamo_table
  {% if is_incremental() %}
    where updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
  {% endif %}  
)
SELECT * FROM cols_invites
