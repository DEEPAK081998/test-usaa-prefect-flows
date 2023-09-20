With leads_cte as (
select ld.lead_id,
(json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'createdAt')::bigint as Updatedate,
ld.data->'closingProcess'->>'expectedReferralFee' as expected_fee,
json_array_elements(data->'closingProcess'->'closingDetails')->>'isBuyer' as is_buyer,
json_array_elements(data->'closingProcess'->'closingDetails')->>'isSeller' as is_seller,
json_array_elements(data->'closingProcess'->'closingDetails')->>'closeDate' as closedate,
json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'lender' as lender,
json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'propertyAddressOrMLS' as Address,
json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'transactionState' as TransactionState,
json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'salePrice' as salePrice,
json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'agentCommissionAmount' as AgentCommissionAmount,
json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'agentCommissionPercentage' as commissionpercent,
ld.data->'closingProcess'->'moneyReceived'->>'amountReceived' as referralamountreceived,
(ld.data->'closingProcess'->'moneyReceived'->>'dateReceived')::bigint as referralfeedate,
ld.data->'closingProcess'->'moneyReceived'->>'accountReceived' as bankaccount,
(ld.data->'closingProcess'->'moneyReceivedMulti'->0->>'dateReceived')::bigint as feedate1,
(ld.data->'closingProcess'->'moneyReceivedMulti'->0->>'amountReceived') as referral_fee_1,
(ld.data->'closingProcess'->'moneyReceivedMulti'->1->>'dateReceived')::bigint as feedate2,
(ld.data->'closingProcess'->'moneyReceivedMulti'->1->>'amountReceived') as referral_fee_2,
ld.data->'closingProcess'->'moneyReceived'->>'difference' as difference,
ld.data->'closingProcess'->'rebate'->>'type' as rebatetype,
ld.data->'closingProcess'->'documentation'->'received'->'user'->>'fullName' as finalreviewname,
(ld.data->'closingProcess'->'documentation'->'received'->>'checkedAt')::bigint as finalreviewdate,
ld.data->'closingProcess'->'documentation'->'reviewers'->0->'user'->>'fullName' as reviewer1name,
(ld.data->'closingProcess'->'documentation'->'reviewers'->0->>'checkedAt')::bigint as reviewer1date,
ld.data->'closingProcess'->'documentation'->'reviewers'->1->'user'->>'fullName' as reviewer2name,
(ld.data->'closingProcess'->'documentation'->'reviewers'->1->>'checkedAt')::bigint as reviewer2date
from {{ ref('leads_data') }} ld
where ld.data->'closingProcess' is not null
)
,
leads_info_no_array AS (
    select ld.lead_id,
ld.data->'closingProcess'->>'expectedReferralFee' as expected_fee,
case when ld.data->'closingProcess'->'moneyReceived'->>'amountReceived' = '' then null else ld.data->'closingProcess'->'moneyReceived'->>'amountReceived' end as referralamountreceived,
(ld.data->'closingProcess'->'moneyReceived'->>'dateReceived')::bigint as referralfeedate,
ld.data->'closingProcess'->'moneyReceived'->>'accountReceived' as bankaccount,
ld.data->'closingProcess'->'moneyReceived'->>'difference' as difference,
ld.data->'closingProcess'->'rebate'->>'type' as rebatetype,
ld.data->'closingProcess'->'documentation'->'received'->'user'->>'fullName' as finalreviewname,
(ld.data->'closingProcess'->'documentation'->'received'->>'checkedAt')::bigint as finalreviewdate,
ld.data->'closingProcess'->'documentation'->'reviewers'->0->'user'->>'fullName' as reviewer1name,
(ld.data->'closingProcess'->'documentation'->'reviewers'->0->>'checkedAt')::bigint as reviewer1date,
ld.data->'closingProcess'->'documentation'->'reviewers'->1->'user'->>'fullName' as reviewer2name,
(ld.data->'closingProcess'->'documentation'->'reviewers'->1->>'checkedAt')::bigint as reviewer2date
from {{ ref('leads_data') }} ld
)
,
expected_fee_cte AS (
    select ld.lead_id,
case when ld.data->'closingProcess'->>'expectedReferralFee' = '' then null else ld.data->'closingProcess'->>'expectedReferralFee' end as expected_fee
from {{ ref('leads_data') }} ld
)
,
leads_info_no_array_final AS (
    select lina.lead_id,
    case when lina.expected_fee = '' then null else lina.expected_fee::decimal end as expected_fee,
    cast(case when lina.referralamountreceived = '' then null else lina.referralamountreceived end as decimal) as referralamountreceived,
    to_timestamp(lina.referralfeedate/1000) as referralfeedate,
    lina.bankaccount
    from leads_info_no_array lina 
)
,
normalize_dates AS(
	SELECT
		lc.lead_id,
        max(lc.Updatedate) as max_date,
        lc.expected_fee,
        lc.is_buyer,
        lc.is_seller,
		case when lc.closedate::text ~* '^[0-9]{13}$' then
						 to_timestamp(lc.closedate::bigint/1000)
						 when lc.closedate = '0' then null
						 when lc.closedate = '' then null
					else
						 to_timestamp(substring(lc.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') end as closedate,
		lender,
        salePrice,
        Address,
        TransactionState,
        AgentCommissionAmount,
        Commissionpercent,
        referralamountreceived,
        referralfeedate,
        bankaccount,
        difference,
        finalreviewname,
        finalreviewdate,
        reviewer1name,
        reviewer1date,
        reviewer2name,
        reviewer2date,
        feedate1,
        feedate2,
        referral_fee_1,
        referral_fee_2

	FROM
		leads_cte lc
    group by lc.lead_id, lc.expected_fee, lc.is_buyer, lc.is_seller, lc.closedate, lender,
        lc.salePrice,
        lc.Address,
        lc.TransactionState,
        lc.AgentCommissionAmount,
        lc.Commissionpercent,
        lc.referralamountreceived,
        lc.referralfeedate,
        lc.bankaccount,
        lc.difference,
        lc.finalreviewname,
        lc.finalreviewdate,
        lc.reviewer1name,
        lc.reviewer1date,
        lc.reviewer2name,
        lc.reviewer2date,
        lc.feedate1,
        lc.feedate2,
        referral_fee_1,
        referral_fee_2
        
    
    
)
,
final AS (
SELECT
	distinct(lead_id),
    max_date,
    cast(case when expected_fee = '' then null else expected_fee end as decimal) as expected_fee,
    is_buyer,
    is_seller,
	lender AS lenderclosedwith,
	closedate,
    salePrice,
    regexp_replace(salePrice,'([^0-9.])','') as saleprice1,
    regexp_replace(difference,'([^0-9.])','') as difference,
    Address,
    TransactionState,
    AgentCommissionAmount,
    Commissionpercent,
    referralamountreceived,
    to_timestamp(referralfeedate/1000) as referralfeedate,
    bankaccount,
    finalreviewname,
    finalreviewdate,
    reviewer1name,
    to_timestamp(reviewer1date/1000) as reviewer1date,
    reviewer2name,
    reviewer2date,
    feedate1,
    feedate2,
    referral_fee_1,
    referral_fee_2
FROM normalize_dates

)
,
final_f AS (
select *, 
ROW_NUMBER() over (PARTITION BY 
lead_id order by max_date DESC) as row_id
from final 
)
,
final_finance AS (
    select *
    from final_f
    where row_id = 1
)
,
rewards_cte AS (SELECT
		DISTINCT(ld.lead_id),
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'type' as rewardtype,
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'amount' as rewardamount_final,
        (ld.data->'closingProcess'->'rebate'->>'paidDate')::bigint as rewardpaidDate,
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->'user'->>'id' as senderID,
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->'user'->>'fullName' as senderName,
        (json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'createdAt')::bigint as rewardcreateddate
FROM {{ ref('leads_data') }} ld
)
,
final_reward_cte AS (
    select
        rc.lead_id,
        max(rewardcreateddate) as max_reward_created_date,
        rewardpaidDate,
        rewardtype,
        rewardamount_final,
        senderID,
        senderName
    from rewards_cte rc
    group by rc.lead_id,rewardtype,senderID,senderName, rewardpaiddate, rewardamount_final
)
,
rewards_final AS (
    select
        distinct(lead_id),
        to_timestamp(max_reward_created_date/1000) as max_reward_created_date,
        to_timestamp(rewardpaidDate/1000) as rewardPaidDate,
        rewardtype,
        rewardamount_final,
        senderID,
        senderName
    from final_reward_cte
    group by lead_id,max_reward_created_date,rewardtype,senderID,senderName,rewardpaiddate,rewardamount_final
)
,
final_final AS (
select *, 
ROW_NUMBER() over (PARTITION BY 
lead_id order by max_reward_created_date DESC) as row_id
from rewards_final
)
,
rewards_final_cte AS (
select 
    rff.lead_id,
    rff.max_reward_created_date,
    rff.rewardpaiddate,
    rff.rewardtype,
    rff.rewardamount_final,
    rff.senderID,
    rff.senderName
from final_final rff 

where rff.row_id = 1
)
,
alt_closing_process AS (
    select ld.lead_id,
(ld.data->'closingProcess'->'closingDetails'->>'createdAt')::bigint as Updatedate,
ld.data->'closingProcess'->>'expectedReferralFee' as expected_fee,
data->'closingProcess'->'closingDetails'->>'isBuyer' as is_buyer,
data->'closingProcess'->'closingDetails'->>'isSeller' as is_seller,
data->'closingProcess'->'closingDetails'->>'closeDate' as closedate,
ld.data->'closingProcess'->'closingDetails'->>'lender' as lender,
ld.data->'closingProcess'->'closingDetails'->>'propertyAddressOrMLS' as Address,
ld.data->'closingProcess'->'closingDetails'->>'transactionState' as TransactionState,
ld.data->'closingProcess'->'closingDetails'->>'salePrice' as salePrice,
ld.data->'closingProcess'->'closingDetails'->>'agentCommissionAmount' as AgentCommissionAmount,
ld.data->'closingProcess'->'closingDetails'->>'agentCommissionPercentage' as commissionpercent,
ld.data->'closingProcess'->'moneyReceived'->>'amountReceived' as referralamountreceived,
(ld.data->'closingProcess'->'moneyReceived'->>'dateReceived')::bigint as referralfeedate,
ld.data->'closingProcess'->'moneyReceived'->>'accountReceived' as bankaccount,
ld.data->'closingProcess'->'moneyReceived'->>'difference' as difference,
ld.data->'closingProcess'->'rebate'->>'type' as rebatetype,
ld.data->'closingProcess'->'documentation'->'received'->'user'->>'fullName' as finalreviewname,
(ld.data->'closingProcess'->'documentation'->'received'->>'checkedAt')::bigint as finalreviewdate,
ld.data->'closingProcess'->'documentation'->'reviewers'->0->'user'->>'fullName' as reviewer1name,
(ld.data->'closingProcess'->'documentation'->'reviewers'->0->>'checkedAt')::bigint as reviewer1date,
ld.data->'closingProcess'->'documentation'->'reviewers'->1->'user'->>'fullName' as reviewer2name,
(ld.data->'closingProcess'->'documentation'->'reviewers'->1->>'checkedAt')::bigint as reviewer2date
from leads_data ld
where  ld.data->'closingProcess' is not null
)
,
alt_normalize_dates AS(
	SELECT
		ac.lead_id,
        max(ac.Updatedate) as max_date,
        ac.expected_fee,
        ac.is_buyer,
        ac.is_seller,
		case when ac.closedate::text ~* '^[0-9]{13}$' then
						 to_timestamp(ac.closedate::bigint/1000)
						 when ac.closedate = '0' then null
						 when ac.closedate = '' then null
					else
						 to_timestamp(substring(ac.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') end as closedate,
		lender,
        salePrice,
        Address,
        TransactionState,
        AgentCommissionAmount,
        Commissionpercent,
        referralamountreceived,
        referralfeedate,
        bankaccount,
        difference,
        finalreviewname,
        finalreviewdate,
        reviewer1name,
        reviewer1date,
        reviewer2name,
        reviewer2date

	FROM
		alt_closing_process ac
    group by ac.lead_id, ac.expected_fee, ac.is_buyer, ac.is_seller, ac.closedate, lender,
        ac.salePrice,
        ac.Address,
        ac.TransactionState,
        ac.AgentCommissionAmount,
        ac.Commissionpercent,
        ac.referralamountreceived,
        ac.referralfeedate,
        ac.bankaccount,
        ac.difference,
        ac.finalreviewname,
        ac.finalreviewdate,
        ac.reviewer1name,
        ac.reviewer1date,
        ac.reviewer2name,
        ac.reviewer2date
    
    
)
,
alt_final AS (
SELECT
	distinct(lead_id),
    max_date,
    cast(case when expected_fee = '' then null else  expected_fee end as decimal) as expected_fee,
    is_buyer,
    is_seller,
	lender AS lenderclosedwith,
	closedate,
    salePrice,
    regexp_replace(salePrice,'([^0-9.])','') as saleprice2,
    regexp_replace(difference,'([^0-9.])','') as difference,
    Address,
    TransactionState,
    AgentCommissionAmount,
    Commissionpercent,
    referralamountreceived,
    to_timestamp(referralfeedate/1000) as referralfeedate,
    bankaccount,
    finalreviewname,
    finalreviewdate,
    reviewer1name,
    to_timestamp(reviewer1date/1000) as reviewer1date,
    reviewer2name,
    reviewer2date
FROM alt_normalize_dates

)
,
alt_final_f AS (
select *, 
ROW_NUMBER() over (PARTITION BY 
lead_id order by max_date DESC) as row_id
from alt_final 
)
,
alt_final_finance AS (
    select *
    from alt_final_f
    where row_id = 1
)
,
rc_cte AS (
        select ca.lead_id,
        ca.profile_aggregate_id,
        rop.rc_office_phone,
        rmp.rc_mobile_phone,
        pup.email as rc_email,
        concat(pup.first_name,' ',pup.last_name) as rc_name
        from current_assignments ca 
        left join partner_user_profiles pup on pup.aggregate_id = ca.profile_aggregate_id
        left outer join (
            select aggregate_id, min(PhoneNumber) as rc_office_phone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from partner_user_profiles) pop
                                      where lower(pop.Phonetype) = 'office'
                                    group by aggregate_id) rop on rop.aggregate_id = ca.profile_aggregate_id
        left outer join (
            select aggregate_id, min(PhoneNumber) as rc_mobile_phone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from partner_user_profiles) pop
                                      where lower(pop.Phonetype) = 'mobile'
                                    group by aggregate_id) rmp on rmp.aggregate_id = ca.profile_aggregate_id
    where ca.role = 'REFERRAL_COORDINATOR'
)
,
transaction_state_cte AS (
    select lead_id,
json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'transactionState' as TransactionState_alt
from leads_data ld
)
,
transaction_state_final AS (
    select distinct on (lead_id) * from transaction_state_cte
    where TransactionState_alt is not null
)
,
t_state_final as (
    select t1.id,
    coalesce(f.transactionState,tsf.TransactionState_alt) as TransactionState
    from 
    {{ ref('chase_enrollments_test') }} t1 
    left join final_finance f on f.lead_id = t1.id 
    left join transaction_state_final tsf on tsf.lead_id = t1.id
)
/*query for the rebate amount when lsu status = Upcoming Rebate Estimated*/
,
lsu_rebate_status_cte AS (
select lead_id,
    status,
    created,
    data->>'amount' as rebate_estimate,
    row_number() OVER (partition by lead_id order by created desc) as row_id
from lead_status_updates 
where status = 'Upcoming Rebate Estimated'
)
,
lsu_rebate_final AS (
select 
    lead_id,
    cast(rebate_estimate as numeric) as rebate_estimate 
from lsu_rebate_status_cte
where row_id = 1
)
,
/*creating new cte to join in backfill data to final select statement*/
final_cte AS (
select 
distinct t1.id,
    (dmc.datemarkedclosed - interval '5 hours') as date_marked_closed_final,
    t1.client_name,
    t1.client_email,
    t1.client_phone,
    t1.agent_name,
    t1.agent_email,
    coalesce(t1.agentmobilephone,t1.agentofficephone) as agent_phone,
    t1.brokerage_code,
    t1.brokerageemail,
    t1.full_name,
    rc.rc_name as rcname,
    rc.rc_email as rcemail,
    rc.rc_office_phone as rc_office_phone,
    rc.rc_mobile_phone as rc_mobile_phone,
    case when f.is_buyer = 'true' then 'BUY'
    when f.is_buyer = 'false' then 'SELL' else l.transaction_type end as transaction_type,
    'CAE' as bank_name,
    tsf.transactionState as tstate_final,
    aff.transactionState as ldstate1,
    f.TransactionState as ldstate,
    coalesce(tsf.TransactionState,t1.normalizedstate) as TransactionState,
    /*f.Address,
    lf.address,*/
    coalesce(f.Address,lf.address) as address,
    f.closedate,
    lf.closedate as lsu_close_date,
    coalesce(f.closedate,lf.closedate) as close_date_final,
    coalesce(cast(nullif(replace(f.saleprice1,',',''),'') as decimal),lf.lsu_saleprice) as saleprice,
    coalesce(cast(nullif(f.AgentCommissionAmount,'') as decimal),lf.commission) as agent_commission,
    coalesce(cast(nullif(f.Commissionpercent,'') as decimal),lf.commissionpercent) as commission_percent,
    coalesce(f.lenderclosedwith,lf.lender) as lenderclosedwith,
    l.documentation_received,
    coalesce(f.reviewer1name,aff.reviewer1name) as reviewer1name,
    coalesce(f.reviewer1date,aff.reviewer1date) as reviewer1date,
    coalesce(f.reviewer2name,aff.reviewer2name) as reviewer2name,
    coalesce(to_timestamp(f.reviewer2date/1000),to_timestamp(aff.reviewer2date/1000)) as reviewer2date,
    extract(days from(coalesce(f.reviewer1date,aff.reviewer1date) - f.closedate)) as reviewer1delay,
    coalesce(f.referralfeedate,linaf.referralfeedate) as referralfeedate,
    coalesce(cast(f.referralamountreceived as decimal),linaf.referralamountreceived) as referralamountreceived,
    coalesce(f.bankaccount,linaf.bankaccount) as bankaccount,
    cast(f.difference as decimal) as difference,
    coalesce(f.expected_fee,linaf.expected_fee) as expected_fee,
    extract(days from (f.referralfeedate - f.closedate)) as referralfeedelay,
    case when coalesce(tsf.TransactionState,t1.normalizedstate) in ('TN','Tennessee') then 'Giftcard'
    when coalesce(tsf.TransactionState,t1.normalizedstate) in ('KS','Kansas') then '$1,000 Giftcard'
    when coalesce(tsf.TransactionState,t1.normalizedstate) in ('OR','Oregon','MS','Mississippi','OK','Oklahoma') AND f.is_buyer = 'true' then 'No Reward'
    when coalesce(tsf.TransactionState,t1.normalizedstate) in ('OR','Oregon','MS','Mississippi','OK','Oklahoma') AND f.is_buyer = 'false' then 'Broker Rebate'
    when coalesce(tsf.TransactionState,t1.normalizedstate) in ('NJ','New Jersey') then 'Broker Rebate'
    when coalesce(tsf.TransactionState,t1.normalizedstate) in ('LA','Louisiana','AK','Alaska','MO','Missouri','IA','Iowa') then 'No Reward'
    else 'Reward Eligible'
    end as state_restrictions,
    case when coalesce(cast(nullif(replace(f.saleprice1,',',''),'') as decimal),lf.lsu_saleprice) >0 AND coalesce(cast(nullif(replace(f.saleprice1,',',''),'') as decimal),lf.lsu_saleprice) <= 499999 AND coalesce(tsf.TransactionState,t1.normalizedstate) NOT IN ('LA','Louisiana','AK','Alaska','MO','Missouri','IA','Iowa') OR coalesce(tsf.TransactionState,t1.normalizedstate) in ('KS','Kansas') then 1000
    when coalesce(cast(nullif(replace(f.saleprice1,',',''),'') as decimal),lf.lsu_saleprice) >=500000 AND coalesce(cast(nullif(replace(f.saleprice1,',',''),'') as decimal),lf.lsu_saleprice) <= 1199999 AND coalesce(tsf.TransactionState,t1.normalizedstate) NOT IN ('LA','Louisiana','AK','Alaska','MO','Missouri','IA','Iowa','KS','Kansas') then 2500
    when coalesce(cast(nullif(replace(f.saleprice1,',',''),'') as decimal),lf.lsu_saleprice) >=1200000 AND coalesce(tsf.TransactionState,t1.normalizedstate) NOT IN ('LA','Louisiana','AK','Alaska','MO','Missouri','IA','Iowa','KS','Kansas') then 5000
    else 0
    end as rebate_amount,
    rff.rewardpaiddate,
    cast(l.rebate_paid as text) as rebate_paid,
    rff.rewardtype,
    rff.rewardamount_final,
    rff.senderID,
    rff.senderName,
    t1.majorstatus,
    t1.hb_status,
    t1.date as enrolldate,
	to_timestamp(f.max_date/1000) as max_upadate_date,
    (rr.reward_ready_date - interval '5 hours') as reward_ready_date_final,
    (ro.reward_ready_ordered_date - interval '5 hours') as reward_ready_ordered_date_final,
    rff.max_reward_created_date,
    dr.documentation_received_date,
    f.feedate1,
    f.feedate2,
    f.referral_fee_1,
    f.referral_fee_2,
    cft.dateissued,
	cft.reward_received,
	cft.currency_code,
	cft.referenceorderid,
    cft.referencelineitemid,
    cft.lsu_reference_id,
    cft.lsu_value,
    cft.campaign,
    cft.lsu_reward_amount,
    lrf.rebate_estimate,
	f.row_id
from {{ ref('chase_enrollments_test') }} t1
left join final_finance f on f.lead_id = t1.id
left join rewards_final_cte rff on rff.lead_id = t1.id
left join leads l on l.id = t1.id
left join {{ ref('stg_lsu_finance') }} lf on lf.lead_id = t1.id
left join alt_final_finance aff on aff.lead_id = t1.id
left join rc_cte rc on rc.lead_id = t1.id
left join leads_info_no_array_final linaf on linaf.lead_id = t1.id
left join expected_fee_cte efc on efc.lead_id = t1.id 
left join t_state_final tsf on tsf.id = t1.id
left join {{ ref('cae_finance_tango') }} cft on cft.lead_id = t1.id
left join lsu_rebate_final lrf on lrf.lead_id = t1.id
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(min(lsu.created)) as datemarkedclosed
	FROM {{ ref('lead_status_updates') }} lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status like 'Closed%'
    group by lsu.lead_id) dmc on dmc.lead_id = t1.id
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(min(lsu.created)) as reward_ready_date
	FROM {{ ref('lead_status_updates') }} lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status = 'Closed Reward Ready'
    group by lsu.lead_id) rr on rr.lead_id = t1.id
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(min(lsu.created)) as reward_ready_ordered_date
	FROM {{ ref('lead_status_updates') }} lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status = 'Closed Reward Ready Ordered'
    group by lsu.lead_id) ro on ro.lead_id = t1.id 
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(max(lsu.created)) as documentation_received_date
	FROM {{ ref('lead_status_updates') }} lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status = 'Closed Documentation Received'
    group by lsu.lead_id) dr on dr.lead_id = t1.id 
where t1.majorstatus = 'Closed' 
order by t1.id desc
)
select 
    fc.id,
    fc.date_marked_closed_final,
    fc.client_name,
    fc.client_email,
    fc.client_phone,
    fc.agent_name,
    fc.agent_email,
    fc.agent_phone,
    fc.brokerage_code,
    fc.brokerageemail,
    fc.full_name,
    fc.rcname,
    fc.rcemail,
    fc.rc_office_phone,
    fc.rc_mobile_phone,
    fc.transaction_type,
    fc.bank_name,
    fc.tstate_final,
    fc.ldstate1,
    fc.ldstate,
    fc.TransactionState,
    fc.address,
    fc.closedate,
    fc.lsu_close_date,
    fc.close_date_final,
    fc.saleprice,
    fc.agent_commission,
    fc.commission_percent,
    fc.lenderclosedwith,
    fc.documentation_received,
    fc.reviewer1name,
    fc.reviewer1date,
    fc.reviewer2name,
    fc.reviewer2date,
    fc.reviewer1delay,
    fc.referralfeedate,
    fc.referralamountreceived,
    fc.bankaccount,
    fc.difference,
    coalesce(fc.expected_fee,bf.expected_fee) as expected_fee,
    fc.referralfeedelay,
    fc.state_restrictions,
    coalesce(fc.rebate_amount,bf.rebate_owed) as rebate_amount,
    coalesce(fc.rewardpaiddate,bf.rebate_paid_date) as rewardpaiddate,
    coalesce(fc.rebate_paid,bf.rebate_paid_backfill) as rebate_paid,
    coalesce(fc.rewardtype,bf._type_of_reward_) as rewardtype,
    fc.rewardamount_final,
    fc.senderID,
    fc.senderName,
    fc.majorstatus,
    fc.hb_status,
    fc.enrolldate,
	fc.max_upadate_date,
    fc.reward_ready_date_final,
    fc.reward_ready_ordered_date_final,
    fc.max_reward_created_date,
    fc.documentation_received_date,
    fc.feedate1,
    fc.feedate2,
    fc.referral_fee_1,
    fc.referral_fee_2,
    fc.dateissued,
	fc.reward_received,
	fc.currency_code,
	fc.referenceorderid,
    fc.referencelineitemid,
    fc.lsu_reference_id,
    fc.lsu_value,
    fc.campaign,
    fc.lsu_reward_amount,
    fc.rebate_estimate,
	fc.row_id
FROM final_cte fc
left join {{ ref('stg_cae_finance_backfill') }} bf on bf.lead_id = fc.id
