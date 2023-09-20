WITH
unroll AS (
SELECT * ,
    {% if target.type == 'snowflake' %}
	data:sofi AS sofi_invitation
    {% else %}
	data ->'sofi' AS sofi_invitation
	{% endif %}
FROM {{ ref('leads_data') }} )
,lead_data AS(
SELECT * FROM unroll
WHERE sofi_invitation IS NOT NULL
)
,lead_data_unpack AS(
SELECT *,
    {% if target.type == 'snowflake' %}
	CAST(sofi_invitation:partyId AS VARCHAR) AS leads_partyId,
	CAST(sofi_invitation:leadId AS VARCHAR) AS leads_leadId
    {% else %}
	CAST(sofi_invitation->'partyId' AS VARCHAR) AS leads_partyId,
	CAST(sofi_invitation->'leadId' AS VARCHAR) AS leads_leadId
	{% endif %}
FROM lead_data
)
select * from lead_data_unpack