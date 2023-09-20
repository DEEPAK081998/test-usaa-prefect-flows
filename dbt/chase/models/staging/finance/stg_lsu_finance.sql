with lsu_cte AS (select 
lead_id,
created,
data->>'closeDate' as closeDate,
data->>'salePrice' as lsu_SalePrice,
data->>'agentCommissionAmount' as agentCommissionAmount,
data->>'agentCommissionPercentage' as agentCommissionPercent,
data->>'lender' as lender,
data->>'propertyAddressOrMLS' as address,
data->>'rewardAmount' as rewardAmount
 from {{ ref('lead_status_updates') }} where status = 'Closed Closed' and data->>'closeDate' is not null
)
, 
lsu_final_cte AS (
    select
    lead_id,
    created,
    regexp_replace(lc.lsu_salePrice,'([^0-9.])','') as saleprice,
    nullif(
          regexp_replace(
             lc.agentCommissionAmount,
             E'^.*[^\\d.].*$',
             'x'),
          'x') as commission,
    nullif(
          regexp_replace(
             agentCommissionPercent,
             E'^.*[^\\d.].*$',
             'x'),
          'x') as commissionpercent,
        lc.address,
        lc.rewardAmount,
        lc.lender,
        replace(lc.closedate,'/','-') as closedate,
    ROW_NUMBER() over (PARTITION BY 
    lead_id order by created DESC) as row_id
    from lsu_cte lc
)
select
    lead_id,
    cast(nullif(replace(saleprice,',',''),'') as decimal) as lsu_saleprice,
    cast(nullif(commission,'') as decimal) as commission,
    cast(nullif(commissionpercent,'') as decimal) as commissionpercent,
    address,
    lender,
    --closedate,
    case when closedate::text ~* '^[0-9]{13}$' then
						 to_timestamp(closedate::bigint/1000)
						 when closedate = '0' then null
						 when closedate = '' then null
					else
						 to_timestamp(substring(closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') end as closedate,
    row_id
from lsu_final_cte
where row_id = 1 
