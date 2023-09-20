{{
  config(
	materialized = 'incremental',
	unique_key = 'invitation_id',
	tags=['mlo_invites'],
	enabled = false
	)
}}

WITH source_table AS (
{% if is_incremental() %}
  SELECT * FROM {{ source('public', 'dynamodb_mlo_invites') }}
  WHERE _airbyte_emitted_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% else %}
	{%- set table_list = get_mlo_invite_bkp_tables() -%}
	{%- set schema_dest = destination_schema() -%}
	{%- for table in table_list %}
	  SELECT * FROM {{ schema_dest }}.{{ table }}
	  {% if not loop.last %}
		UNION ALL
	  {% endif %}

	  {% if loop.last %}
		UNION ALL
		SELECT * FROM {{ source('public', 'dynamodb_mlo_invites') }}
	  {% endif %}
	{% endfor %}
{% endif %}
)
{% if is_incremental() %}
, updated_invitations_cte AS (
	SELECT *
	FROM source_table
	WHERE REPLACE(CAST (data::json->'invitationId' AS TEXT), '"','') NOT IN (
		SELECT invitation_id FROM {{ this }}
	)
)
{% endif %}

, deduped AS(
	SELECT
		REPLACE(CAST (data::json->'invitationId' AS TEXT), '"','') AS invitation_id,
		REPLACE(CAST (data::json->'agent' AS TEXT), '"','') AS agent,
		REPLACE(CAST(data::json->'invitedBy'->'email' AS TEXT),'"','') AS purchase_specialist,
		REPLACE(CAST (data::json->'mloSubmission' AS TEXT), '"','') AS mlo_submission,
		REPLACE(CAST (data::json->'smsInvitePermission' AS TEXT), '"','') AS sms_invite_permission,
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
		now() AS updated_at
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