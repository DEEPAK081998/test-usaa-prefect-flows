{{
  config(
    materialized = 'view',
    )
}}
WITH leads_cte AS(
	SELECT
		DISTINCT(ld.lead_id),
        (json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'createdAt')::bigint as update_date,
		json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'lender' as lender,
		json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'closeDate' as closedate,
        cast(nullif(replace(json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'salePrice',',',''),'')as numeric) as SalePrice,
        json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'propertyAddressOrMLS' as Address,
        json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'transactionState' as TransactionState,
        cast(nullif(replace(json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'agentCommissionAmount',',',''),'') as decimal) as AgentCommissionAmount,
        cast(nullif(nullif(replace(json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'agentCommissionPercentage',',',''),''),'N/A') as decimal) as Commissionpercent,
        /*json_array_elements(ld.data::json->'closingProcess'->'rebate'->'statuses')->>'type' as rewardtype,
        json_array_elements(ld.data::json->'closingProcess'->'rebate'->'statuses')->'user'->>'id' as senderID,
        json_array_elements(ld.data::json->'closingProcess'->'rebate'->'statuses')->'user'->>'fullName' as senderName,
        (json_array_elements(ld.data::json->'closingProcess'->'rebate'->'statuses')->>'createdAt')::bigint as rewardcreateddate,*/
        json_array_elements(ld.data::json->'closingProcess'->'closingDetails')->>'isBuyer' as BuyTransaction
	FROM
		{{ ref('leads_data') }}  ld

)
,
normalize_dates AS(
	SELECT
		lc.lead_id,
		max(lc.update_date) as max_date,
		case when lc.closedate::text ~* '^[0-9]{13}$' then
						 to_timestamp(lc.closedate::bigint/1000)
						 when lc.closedate = '0' then null
						 when lc.closedate = '' then null
					else
						 to_timestamp(substring(lc.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') end as closedate,
		lender,
        SalePrice,
        Address,
        TransactionState,
        AgentCommissionAmount,
        Commissionpercent,
        BuyTransaction

	FROM
		leads_cte lc
    WHERE update_date is not null
    group by lc.lead_id, lc.closedate,lc.lender, lc.SalePrice, lc.Address,lc.transactionState,lc.agentCommissionAmount, lc.Commissionpercent,lc.BuyTransaction


)
,
final AS (
SELECT
	distinct(lead_id),
    max_date,
	lender AS lenderclosedwith,
	closedate,
    SalePrice,
    Address,
    TransactionState,
    AgentCommissionAmount,
    Commissionpercent,
    BuyTransaction
FROM normalize_dates
GROUP BY lead_id, closedate, lender,max_date, SalePrice,Address,TransactionState, AgentCommissionAmount,Commissionpercent,BuyTransaction
)
,
final_f AS (
select *, 
ROW_NUMBER() over (PARTITION BY 
lead_id order by max_date DESC) as row_id
from final 
)
select 
	ff.lead_id,
	ff.max_date,
	ff.lenderclosedwith,
	ff.closedate,
    (dmc.datemarkedclosed - interval '5 hours') as date_marked_closed_final,
    (rr.reward_ready_date - interval '5 hours') as reward_ready_date_final,
    (ro.reward_ready_ordered_date - interval '5 hours') as reward_ready_ordered_date_final,
    ff.SalePrice,
    ff.Address,
    ff.TransactionState,
    ff.AgentCommissionAmount,
    ff.Commissionpercent,
    ff.BuyTransaction,
	ff.row_id
from final_f ff
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(min(lsu.created)) as datemarkedclosed
	FROM {{ ref('lead_status_updates') }}  lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status like 'Closed%'
    group by lsu.lead_id) dmc on dmc.lead_id = ff.lead_id
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(min(lsu.created)) as reward_ready_date
	FROM {{ ref('lead_status_updates') }}  lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status = 'Closed Reward Ready'
    group by lsu.lead_id) rr on rr.lead_id = ff.lead_id
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(min(lsu.created)) as reward_ready_ordered_date
	FROM {{ ref('lead_status_updates') }}  lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status = 'Closed Reward Ready Ordered'
    group by lsu.lead_id) ro on ro.lead_id = ff.lead_id
where ff.row_id = 1