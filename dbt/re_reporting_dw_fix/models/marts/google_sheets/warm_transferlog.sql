SELECT 
    {{  special_column_name_formatter('Date') }} AS date,
    partner,
    "Loan Officer Name" AS loan_officer_name,
    nmlsid,
    "Client Name" AS client_name,
    enrolled,
    CAST(leadid AS INT) AS leadid,
    declined,
    "Decline Reason" AS decline_reason,
    "LO Contact Attempted " AS lo_contact_attempted,
    "Contact Made" AS contact_made  
FROM
    {{ ref('base_warm_transfer_log') }}
WHERE {{  special_column_name_formatter('Date') }} IS NOT NULL