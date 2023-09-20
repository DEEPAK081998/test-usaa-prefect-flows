SELECT
    dd_name,
    lm_name,
    division,
    slm_name,
    lo_nmlsid,
    updated_date,
    opportunity_date,
    opportunity_name,
    opportunity_range,
    opportunity_value
FROM 
    {{ ref('base_partner_opportunities') }}
