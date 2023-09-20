{{
  config(
	materialized = 'incremental',
	unique_key = 'invitation_id',
	tags=['mlo_invites']
	)
}}

WITH source_table AS (
{% if target.type == 'snowflake' %}
  SELECT REPLACE(CAST (data:invitationId AS TEXT), '"','')::text AS invitation_id,* FROM {{ source('public', 'dynamodb_mlo_invites') }}
{% else %}
  SELECT REPLACE(CAST (data->'invitationId' AS TEXT), '"','')::text AS invitation_id,* FROM {{ source('public', 'dynamodb_mlo_invites') }}
{% endif %}
  WHERE _airbyte_ab_id NOT IN {{AirbyteIdsToIgnore()}}
  	{% if is_incremental() %}
	  AND _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01') 
	{% endif %}
)
{% if is_incremental() %}
, updated_invitations_cte AS (
	SELECT s.*
	FROM source_table s left join {{ this }} t 
	on s.invitation_id = t.invitation_id
	WHERE t.invitation_id is null 
)
{% endif %}

, deduped AS(
	SELECT
		invitation_id,
		{% if target.type == 'snowflake' %}
		REPLACE(CAST (data:agent AS TEXT), '"','') AS agent,
		REPLACE(CAST(data:invitedBy.email AS TEXT),'"','') AS purchase_specialist,
		REPLACE(CAST (data:mloSubmission AS TEXT), '"','') AS mlo_submission,
		REPLACE(CAST (data:smsInvitePermission AS TEXT), '"','') AS sms_invite_permission,
		{% else %}
		REPLACE(CAST (data->'agent' AS TEXT), '"','') AS agent,
		REPLACE(CAST(data->'invitedBy'->'email' AS TEXT),'"','') AS purchase_specialist,
		REPLACE(CAST (data->'mloSubmission' AS TEXT), '"','') AS mlo_submission,
		REPLACE(CAST (data->'smsInvitePermission' AS TEXT), '"','') AS sms_invite_permission,
		{% endif %}
		role,
		email,
		loemail,
		nmlsid,
		lastname,
		sendgrid,
		firstname,
		shorthash,
		utmsource,
		disclosure,
		lolastname,
		lofirstname,
		phonenumber,
		utmcampaign,
		lophonenumber,
		mlosubmission,
		customerconnect,
		currentbankcustomer,
		confirmationrequired,
		MIN(_airbyte_emitted_at) AS invitation_dynamo,
		{{ current_date_time() }} AS updated_at
	FROM 
	{% if is_incremental() %}
	updated_invitations_cte
	{% else %}
	source_table
	{% endif %}
	GROUP BY
		invitation_id,	agent,	mlo_submission,	sms_invite_permission,	role,	email,	nmlsid,	loemail,
		lastname,	sendgrid,	firstname,	shorthash,	utmsource,	disclosure,	purchase_specialist,
		lolastname,	lofirstname,	phonenumber,	utmcampaign,	lophonenumber,	mlosubmission,
		customerconnect,	currentbankcustomer,	confirmationrequired
	ORDER BY invitation_dynamo ASC
)
SELECT
    *
FROM deduped 
