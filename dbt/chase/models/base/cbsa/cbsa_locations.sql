SELECT 
    st,
    lpad(zip, 5, '0') as zip,
    city,
    cbmsa,
    county
FROM    
    {{ source('public', 'raw_cbsa_locations') }}

    