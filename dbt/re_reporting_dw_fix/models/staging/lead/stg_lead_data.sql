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
  {% if target.type == 'snowflake' %}
  data:mloSubmission::VARCHAR as MLOSubmission,
  data:salesforceEnroll::VARCHAR as CitizensSalesForceEnrollment,
  data:currentBankCustomer as BankCustomer ,
  data:velocifyId as velocifyId,
  data:branchName as branchName,
  data:employeeName as branchEmployee,
  data:ECI as customerECI,
  data:closingProcess.rebate as rebate_obj,
  data:closingProcess.documentation as documentation_obj,
  {% else %}
  data->>'mloSubmission' as MLOSubmission,
  data->>'salesforceEnroll' as CitizensSalesForceEnrollment,
  data->'currentBankCustomer' as BankCustomer ,
  data->'velocifyId' as velocifyId,
  data->'branchName' as branchName,
  data->'employeeName' as branchEmployee,
  data->'ECI' as customerECI,
  data->'closingProcess'->'rebate' as rebate_obj,
  data->'closingProcess'->'documentation' as documentation_obj,
  {% endif %}
  updated as updated_at
FROM {{ ref('leads_data') }}
{% if is_incremental() %}
WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01') + interval '-2 day'
{% endif %}


