{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id',
    indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
      {'columns': ['updated_at'], 'type': 'btree'},
	  ],
    )
}}
SELECT
  lead_id,
  data::json->>'mloSubmission' as MLOSubmission,
  data::json->>'salesforceEnroll' as CitizensSalesForceEnrollment,
  data::json->'currentBankCustomer' as BankCustomer ,
  data::json->'velocifyId' as velocifyId,
  data::json->'branchName' as branchName,
  data::json->'employeeName' as branchEmployee,
  data::json->'ECI' as customerECI,
  data::json->'closingProcess'->'rebate' as rebate_obj,
  data::json->'closingProcess'->'documentation' as documentation_obj,
  updated as updated_at
FROM {{ ref('leads_data') }} 
{% if is_incremental() %}
WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}


