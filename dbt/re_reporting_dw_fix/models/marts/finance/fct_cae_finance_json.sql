{{
  config(
    materialized = 'incremental',
    unique_key = ['lead_id'],
    )
}}
WITH
 final_cte AS (
SELECT
DISTINCT
cte.lead_id as lead_id,
cte.referralamountreceived,
cte.referralfeedate,
cte.bankaccount,
cte.Updatedate,
cte.SalePrice,
cte.Address,
cte.TransactionState,
cte.AgentCommissionAmount,
row_number() OVER(partition by cte.lead_id Order by Updatedate DESC) as updatedfield,
case
    when cte.Commissionpercent = '' then null
    when cte.Commissionpercent = 'N/A' then null
    else nullif(cte.Commissionpercent,'N/A')::decimal
end as CommissionPercent,
cte.BuyTransaction,
rscte.rewardtype as rewardType,
rscte.senderID,
rscte.senderName,
rscte.rewardcreateddate as rewardCreatedDate,
rscte.rebatetype as rebatetype,
rcte.reviewer1id as Reviewer1ID,
rcte.finalreviewname as Finalreviewname,
rcte.finalreviewdate as finalreviewDate,
rcte.finalreviewid as finalreviewID,
rcte.reviewer1name as reviewer1Name,
rcte.reviewer1date as reviewer1Date,
rcte.reviewer2id as Reviewer2ID,
rcte.reviewer2name as reviewer2Name,
rcte.reviewer2date as reviewer2Date,
cte.Rewardpaiddate as rewardpaidDate,
case
    when cte.closedate is not null THEN
case 
    {% if target.type == 'snowflake' %}
    when RLIKE(cte.closedate::text, '^[0-9]{13}$', 'i') then to_timestamp(cte.closedate::bigint/1000)
    when cte.closedate = '0' then null
    when cte.closedate = '' then null
    else  to_timestamp(REGEXP_SUBSTR(cte.closedate, '\\d+[-_]\\d+[-_]\\d+$'), 'yyyy-MM-dd')
    {% else %}
    when cte.closedate::text ~* '^[0-9]{13}$' then to_timestamp(cte.closedate::bigint/1000)
    when cte.closedate = '0' then null
    when cte.closedate = '' then null
    else  to_timestamp(substring(cte.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd')
    {% endif %}
end 
end as transactionclosedate_unw
FROM {{ ref('stg_lead_data_finance') }} cte
left join {{ ref('stg_lead_data_reviewer') }} rcte on rcte.lead_id = cte.lead_id
left join {{ ref('stg_lead_data_reward') }} rscte on rscte.lead_id = cte.lead_id
WHERE cte.Updatedate is not null
)
SELECT
    *,max(cte.transactionclosedate_unw) over(partition by cte.lead_id) as transactionclosedate
FROM final_cte cte 
WHERE cte.updatedfield =1