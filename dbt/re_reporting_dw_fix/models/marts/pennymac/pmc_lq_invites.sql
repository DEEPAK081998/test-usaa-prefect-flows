{{
  config(
	  tags=['pmc']
    )
}}

-- contains invites (that click sg) and registrations that have not enrolled.  

WITH 
registered_customers AS(
    select 
        pup.id,
        pup.created AS enrolleddate,
        pup.first_name AS client_first_name,
        pup.last_name AS client_last_name,
        pup.email AS client_email,
        {% if target.type == 'snowflake' %}
        REPLACE(CAST(pup.phones[0].phoneNumber AS TEXT),'"','') AS client_phone,
        CAST(pup.data:purchaseLocation AS TEXT) AS purchase_location,
        CAST(pup.data:loFirstName AS TEXT) AS lo_first_name,
        CAST(pup.data:loLastName AS TEXT) AS lo_last_name,
        CAST(pup.data:loPhoneNumber AS TEXT) AS lo_phone,
        CAST(pup.data:loEmail AS TEXT) AS lo_email,
        pup.data:agent::VARCHAR as has_agent,
        {% else %}
        REPLACE(CAST(pup.phones->0->'phoneNumber' AS TEXT),'"','') AS client_phone,
        CAST(pup.data->'purchaseLocation' AS TEXT) AS purchase_location,
        CAST(pup.data->'loFirstName' AS TEXT) AS lo_first_name,
        CAST(pup.data->'loLastName' AS TEXT) AS lo_last_name,
        CAST(pup.data->'loPhoneNumber' AS TEXT) AS lo_phone,
        CAST(pup.data->'loEmail' AS TEXT) AS lo_email,
        pup.data->>'agent' as has_agent,
        {% endif %}
        t1.created as enrolled_Date,
        t1.updated as updated_at
      FROM        
        {{ ref('partner_user_profiles') }}  pup
    FULL join {{ ref('leads') }} t1 on t1.email = pup.email
    {% if target.type == 'snowflake' %}
    where UPPER(pup.partner_id) = 'E2A46D0A-6544-4116-8631-F08D749045AC'
    {% else %}
    where pup.partner_id = 'E2A46D0A-6544-4116-8631-F08D749045AC'
    {% endif %}
    and t1.created is null

),
  customer_filter AS(
	SELECT
		rgc.*,
		pur.role 
	FROM registered_customers rgc
		INNER JOIN {{ ref('partner_user_roles') }} pur
		ON rgc.id=pur.user_profile_id
	WHERE role='CUSTOMER'
  
),
reg_wo_enroll AS(
SELECT 
	enrolleddate,
	CAST(id AS TEXT) AS id,
	client_first_name,
	client_last_name,
	client_email,
	client_phone,
	REPLACE(purchase_location, '"','') AS purchase_location,
	NULL AS normalizedcity,
	NULL AS normalizedstate,
	NULL AS normalizedzip,
	NULL AS agentfirstname,
	NULL AS agentlastname,
	NULL AS agent_phone,
	NULL AS agent_email,
	NULL AS enrollmenttype,
	lo_first_name,
	lo_last_name,
	lo_phone,
	lo_email
FROM customer_filter
ORDER BY enrolleddate
),
clicked_customers AS(
  SELECT 
    to_email,
    SUM(clicks_count) total_clicks
  FROM 
    {{ ref('stg_pmc_sendgrid_invites') }}
  GROUP BY to_email HAVING SUM(clicks_count)>0
  UNION ALL 
  SELECT 
    to_email,
    SUM(clicks_count) total_clicks
  FROM 
    {{ ref('base_pmc_loinvite_sg') }}
   
  GROUP BY to_email HAVING SUM(clicks_count)>0
),
dedup_customers AS(
  SELECT 
    DISTINCT(to_email)
  FROM  
    clicked_customers
),
lq_invites AS(
  SELECT 
    {% if target.type == 'snowflake' %}
    TO_TIMESTAMP_NTZ(invite_date) AS enrolleddate,
    {% else %}
    invite_date AS enrolleddate,
    {% endif %}
    spi.id,
    firstname AS client_first_name,
    lastname AS client_last_name,
    spi.email AS client_email,
    phonenumber AS client_phone,
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
    lolastname AS lo_last_name,
    lophonenumber AS lo_phone,
    loemail AS lo_email
  FROM  {{ ref('stg_pmc_invites') }} spi
  LEFT JOIN dedup_customers cc
  ON cc.to_email=spi.email
),
non_enr_invites AS(
	SELECT
		lqi.*
	FROM
		lq_invites lqi
	FULL JOIN {{ ref('leads') }} t1
	ON t1.email = lqi.client_email
	WHERE t1.email IS NULL
),
reg_plus_invites AS(
	SELECT 
		*
	FROM
		reg_wo_enroll
	UNION
	SELECT
		* 
	FROM
		non_enr_invites
),
dedup AS(
	SELECT 
		*,
		ROW_NUMBER() OVER(
		Partition By client_email ORDER BY enrolleddate) AS rn
	FROM
		reg_plus_invites
)

SELECT * FROM dedup
WHERE rn = 1