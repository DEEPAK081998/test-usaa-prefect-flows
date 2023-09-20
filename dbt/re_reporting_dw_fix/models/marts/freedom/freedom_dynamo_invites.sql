{{
  config(
    materialized = 'view',
   
    )
}}
WITH freedom_dynamo_invites AS(
    SELECT 
        id AS invitation_id,
        email,
        loemail AS lo_email,
        CONCAT(firstname, ' ', lastname) AS customer_name,
        lophonenumber AS lo_phone,
        CONCAT(lofirstname, ' ', lolastname) AS lo_name,
        purchase_specialist,
        invite_date AS enrolleddate,
        phonenumber as client_phone,
        firstname as client_first_name,
        email AS client_email,
        lastname as client_last_name,
        'TOF' AS program,
        'Freedom' AS client,
        NULL AS purchase_location,
        NULL AS normalizedcity,
        NULL AS normalizedstate,
        NULL AS normalizedzip,
        NULL AS agentfirstname,
        NULL AS agentlastname,
        NULL AS agent_phone,
        NULL AS agent_email,
        NULL AS enrollmenttype,
        lofirstname AS lo_first_name,
        lolastname AS lo_last_name
    

    FROM {{ ref('stg_freedom_invites') }}
    
),
freedom_leads AS(
SELECT 
	email,
	created,
	bank_id
FROM 
	{{ ref('leads') }}
WHERE bank_id in ('3405dc7c-e972-4bc4-a3da-cb07e822b7c6','482c83b7-12f3-4098-a846-b3091a33f966')
	)
SELECT fdi.* 
FROM freedom_dynamo_invites fdi
FULL JOIN freedom_leads 
ON lower(fdi.client_email)=lower(freedom_leads.email)
WHERE freedom_leads.created IS NULL
 