{{
  config(
    materialized = 'incremental',
    unique_key = 'lead_id',
    indexes=[
      {'columns': ['lead_id'], 'type': 'hash'},
      {'columns': ['updated_at'], 'type': 'btree'},
	  ],
    enabled=false
    )
}}
WITH CTE AS (
SELECT
  DISTINCT ON (lead_id)
  lead_id,
  {#- Closing portal referral info -#}
  REGEXP_REPLACE(nullif(replace(data::json->'closingProcess'->'moneyReceived'->>'amountReceived',',',''),''),'[^0-9\.]+', '','g') as referralamountreceived,
  (data::json->'closingProcess'->'moneyReceived'->>'dateReceived')::bigint as referralfeedate,
  data::json->'closingProcess'->'moneyReceived'->>'accountReceived' as bankaccount,
  {#- Closing portal closing details -#}
  (json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'createdAt')::bigint as Updatedate,
  REGEXP_REPLACE(nullif(replace(json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'salePrice',',',''),''),'[^0-9\.]+', '','g') as SalePrice,
  json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'propertyAddressOrMLS' as Address,
  json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'transactionState' as TransactionState,
  REGEXP_REPLACE(nullif(replace(json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'agentCommissionAmount',',',''),''),'[^0-9\.]+', '','g') as AgentCommissionAmount,
  json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'agentCommissionPercentage' as Commissionpercent,
  json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'isBuyer' as BuyTransaction,
  (data::json->'closingProcess'->>'paidDate')::bigint as Rewardpaiddate,
  json_array_elements(data::json[]->'closingProcess'->'closingDetails')->>'closeDate' as closedate,
  updated as updated_at
FROM {{ ref('leads_data') }} 
{% if is_incremental() %}
WHERE updated >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
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


