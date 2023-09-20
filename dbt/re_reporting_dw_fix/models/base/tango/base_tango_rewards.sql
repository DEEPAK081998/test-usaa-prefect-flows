WITH unpack AS(
{% if target.type == 'snowflake' %}
    SELECT
        etid,
        utid,
        sender:email::VARCHAR AS sender_email,
        sender:lastName::VARCHAR AS sender_last_name,
        sender:firstName::VARCHAR AS sender_first_name,
        status,
        recipient:email::VARCHAR AS customer_email,
        recipient:lastName::VARCHAR AS customer_last_name,
        recipient:firstName::VARCHAR AS customer_first_name,
        dateissued,
        linenumber,
        rewardname,
        emailstatus,
        ordersource,
        orderstatus,
        amountissued:value as value,
        amountissued:currencyCode as currency_code,
        accountnumber,
        expirationdate,
        referenceorderid,
        accountidentifier,
        customeridentifier,
        referencelineitemid
    FROM
       {{ source('public', 'raw_tango__line_items') }}
{% else %}
    SELECT
        etid,
        utid,
        sender->>'email' AS sender_email,
        sender->>'lastName' AS sender_last_name,
        sender->>'firstName' AS sender_first_name,
        status,
        recipient->>'email' AS customer_email,
        recipient->>'lastName' AS customer_last_name,
        recipient->>'firstName' AS customer_first_name,
        dateissued,
        linenumber,
        rewardname,
        emailstatus,
        ordersource,
        orderstatus,
        amountissued->'value' as value,
        amountissued->'currencyCode' as currency_code,
        accountnumber,
        expirationdate,
        referenceorderid,
        accountidentifier,
        customeridentifier,
        referencelineitemid
    FROM 
       {{ source('public', 'raw_tango__line_items') }}
{% endif %}
    ),
leads AS(
    SELECT
        id,
        client_email
    FROM 
        {{ ref('leads_data_v3') }}
    WHERE major_status='Closed'
),
dedup AS(
	SELECT
		distinct unpack.*
	from 
		unpack
)
select 
	dedup.*,
	l.id as lead_id
from dedup
left join leads l
on lower(dedup.customer_email)=lower(l.client_email)
where id is not NULL