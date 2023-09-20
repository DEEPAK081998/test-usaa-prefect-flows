{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
	  tags=['mlo_invites']
    )
}}
WITH
 
pmc_emails AS(
  SELECT DISTINCT 
    to_email
  FROM 
    {{ ref('stg_pmc_sendgrid_invites') }}
),
pmc_lo_invite_emails AS(
  SELECT DISTINCT
    to_email
  FROM 
    {{ ref('base_pmc_loinvite_sg') }}
),
pmc_union_email AS(
  SELECT 
    *
  FROM 
    pmc_emails
  UNION ALL 
  SELECT
    *
  FROM 
    pmc_lo_invite_emails
),
pmc_all_emails AS(
  SELECT
    DISTINCT(to_email)
  FROM 
    pmc_union_email
),
pennymac_dynamo_table AS(
  SELECT dit.*
  FROM {{ ref('dynamodb_invite_table') }} dit
  INNER JOIN pmc_all_emails
  ON lower(dit.email) = lower(pmc_all_emails.to_email)
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
        pennymac_dynamo_table
  {% if is_incremental() %}
    where updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
  {% endif %}  
),
total_sg AS(
select 
	to_email,
	SUM(opens_count) as total_opens,
	SUM(clicks_count) as total_clicks
from {{ ref('stg_pmc_sendgrid_invites') }} spsi 
group by 
	to_email
union ALL
select 
	to_email,
	SUM(opens_count) as total_opens,
	SUM(clicks_count) as total_clicks
from {{ ref('base_pmc_loinvite_sg') }}  bpls   
group by 
	to_email
),
rolled_sg as(
select 
	to_email,
	SUM(total_opens) as total_opens,
	SUM(total_clicks) as total_clicks
from total_sg
group by 
	to_email
order by to_email
)
select 
	pmci.*,
	sg.total_opens,
	sg.total_clicks
from 
	cols_invites pmci
left join rolled_sg sg
on lower(pmci.email)=sg.to_email

