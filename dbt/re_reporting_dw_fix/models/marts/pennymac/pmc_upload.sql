
SELECT
leads.id as "HomeStory ID",
leads.created,
first_name as "First Name",
last_name as "Last Name",
{% if target.type == 'snowflake' %}
normalized_purchase_location:city::VARCHAR as "Property City",
normalized_purchase_location:state::VARCHAR as "Property State",
{% else %}
normalized_purchase_location->>'city' as "Property City",
normalized_purchase_location->>'state' as "Property State",
{% endif %}
email as "Email",
phone as "Cell Phone",
'Purchase' as "Sales Group",
'9597-WS-9597' as "Marketing Source Code",
'TRUE' as "Received from HomeStory",
'HomeStory Online Enrollment' as "Campaign Name",
'Hubspot' as "Lead Source",
'Preview Purchase Call 1' as "Status",
'' as "Estimated Payment Savings",
'' as "Property Address",
'' as "Property Zip Code",
'' as "Dialing Zip Code",
'' as "Property County",
'' as "Mailing Address",
'' as "Mailing City",
'' as "Mailing State",
'' as "Mailing Zip Code",
'' as "Current UPB",
'' as "Current Rate (%)",
'' as "Marketing Rate",
'' as "MI Amount",
'' as "Home Phone",
'' as "1st P&I",
'' as "Escrowed T&I",
'' as "First Name (Co-Borrower)",
'' as "Last Name (Co-Borrower)",
'' as "Age of Loan",
'' as "Due Date",
'' as "Escrow Balance",
'' as "Original Loan Amount",
'' as "Note Date",
'' as "Maturity Date",
'' as "MLS Listing Price",
'' as "MLS Listing Date",
'' as "Bedroom Count",
'' as "Bathroom Count",
'' as "Year Built (Home Listed)",
'' as "Square Footage",
'' as "MLS Agent Name",
{{ local_convert_timezone('leads.created','CETDST') }} as created_datetime_cst
FROM {{ ref('leads') }},{{ ref('normalized_lead_locations') }}
WHERE leads.id=normalized_lead_locations.lead_id
AND bank_id='E2A46D0A-6544-4116-8631-F08D749045AC'
AND leads.id NOT IN (24696)
AND transaction_type='BUY'