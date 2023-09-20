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
WITH CTE AS (
{% if target.type == 'snowflake' %}
SELECT
  lead_id,
  {#- Closing portal referral info -#}
  REGEXP_REPLACE(nullif(replace(data:closingProcess.moneyReceived.amountReceived::VARCHAR,',',''),''),'[^0-9\\.]+', '', '1', '0') as referralamountreceived,
  (data:closingProcess.moneyReceived.dateReceived::VARCHAR)::bigint as referralfeedate,
  data:closingProcess.moneyReceived.accountReceived::VARCHAR as bankaccount,
  {#- Closing portal closing details -#}
  fcd.value:createdAt::VARCHAR::bigint as Updatedate,
  REGEXP_REPLACE(nullif(replace(fcd.value:salePrice::VARCHAR,',',''),''),'[^0-9\\.]+', '', '1', '0') as SalePrice,
  fcd.value:propertyAddressOrMLS::VARCHAR as Address,
  fcd.value:transactionState::VARCHAR as TransactionState,
  REGEXP_REPLACE(nullif(replace(fcd.value:agentCommissionAmount::VARCHAR,',',''),''),'[^0-9\\.]+', '', '1', '0') as AgentCommissionAmount,
  fcd.value:agentCommissionPercentage::VARCHAR as Commissionpercent,
  fcd.value:isBuyer::VARCHAR as BuyTransaction,
  (data:closingProcess.paidDate::VARCHAR)::bigint as Rewardpaiddate,
  fcd.value:closeDate::VARCHAR as closedate,
  updated as updated_at
FROM {{ ref('leads_data') }} ld,
lateral flatten(input => ld.data:closingProcess.closingDetails) fcd
{% if is_incremental() %}
WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY lead_id) = 1
{% else %}
SELECT
  DISTINCT ON (lead_id)
  lead_id,
  {#- Closing portal referral info -#}
  REGEXP_REPLACE(nullif(replace(data->'closingProcess'->'moneyReceived'->>'amountReceived',',',''),''),'[^0-9\.]+', '','g') as referralamountreceived,
  (data->'closingProcess'->'moneyReceived'->>'dateReceived')::bigint as referralfeedate,
  data->'closingProcess'->'moneyReceived'->>'accountReceived' as bankaccount,
  {#- Closing portal closing details -#}
  (json_array_elements(data->'closingProcess'->'closingDetails')->>'createdAt')::bigint as Updatedate,
  REGEXP_REPLACE(nullif(replace(json_array_elements(data->'closingProcess'->'closingDetails')->>'salePrice',',',''),''),'[^0-9\.]+', '','g') as SalePrice,
  json_array_elements(data->'closingProcess'->'closingDetails')->>'propertyAddressOrMLS' as Address,
  json_array_elements(data->'closingProcess'->'closingDetails')->>'transactionState' as TransactionState,
  REGEXP_REPLACE(nullif(replace(json_array_elements(data->'closingProcess'->'closingDetails')->>'agentCommissionAmount',',',''),''),'[^0-9\.]+', '','g') as AgentCommissionAmount,
  json_array_elements(data->'closingProcess'->'closingDetails')->>'agentCommissionPercentage' as Commissionpercent,
  json_array_elements(data->'closingProcess'->'closingDetails')->>'isBuyer' as BuyTransaction,
  (data->'closingProcess'->>'paidDate')::bigint as Rewardpaiddate,
  json_array_elements(data->'closingProcess'->'closingDetails')->>'closeDate' as closedate,
  updated as updated_at
FROM {{ ref('leads_data') }}
{% if is_incremental() %}
WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
{% endif %}
{% endif %}
)
select
 lead_id,
  CAST(
    Case
      WHEN referralamountreceived = '' OR NULL then null
      else referralamountreceived
    END
    as decimal
 ) as referralamountreceived,
 referralfeedate,
 bankaccount,
 Updatedate,
 CAST(
    Case
      WHEN SalePrice = '' OR NULL then null
      else SalePrice
    END
    as numeric
 ) as SalePrice,
 Address,
 TransactionState,
 CAST(
    Case
      WHEN AgentCommissionAmount = '' OR NULL then null
      else AgentCommissionAmount
    END
    as decimal
 ) as AgentCommissionAmount,
 Commissionpercent,
 BuyTransaction,
 Rewardpaiddate,
 closedate,
 updated_at from cte
