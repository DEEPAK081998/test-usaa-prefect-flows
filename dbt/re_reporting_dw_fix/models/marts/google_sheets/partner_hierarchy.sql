SELECT
    update_date,
    bank_name,
    lo_nmlsid,
    lo_name,
    lo_email,
    lo_phone,
    lm_name,
    lm_email,
    slm_name,
    slm_manager,
    dd_name,
    dd_email, 
    division,
    lo_firstname,
    lo_lastname
FROM 
    {{ ref('base_partner_hierarchy') }}