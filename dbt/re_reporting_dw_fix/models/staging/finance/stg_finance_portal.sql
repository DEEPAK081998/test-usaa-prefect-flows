With 
leads_cte as (
{% if target.type == 'snowflake' %}
    select
        ld.lead_id,
        (flat_ld.value:created::VARCHAR)::bigint as Updatedate,
        ld.data:closingProcess.expectedReferralFee::VARCHAR as expected_fee,
        flat_ld.value:isBuyer::VARCHAR as is_buyer,
        flat_ld.value:isSeller::VARCHAR as is_seller,
        flat_ld.value:closeDate::VARCHAR as closedate,
        flat_ld.value:lender::VARCHAR as lender,
        flat_ld.value:propertyAddressOrMLS::VARCHAR as Address,
        flat_ld.value:transactionState::VARCHAR as TransactionState,
        flat_ld.value:salePrice::VARCHAR as salePrice,
        flat_ld.value:agentCommissionAmount::VARCHAR as AgentCommissionAmount,
        flat_ld.value:agentCommissionPercentage::VARCHAR as commissionpercent,
        ld.data:closingProcess.moneyReceived.amountReceived::VARCHAR as referralamountreceived,
        (ld.data:closingProcess.moneyReceived.dateReceived::VARCHAR)::bigint as referralfeedate,
        ld.data:closingProcess.moneyReceived.accountReceived::VARCHAR as bankaccount,
        (ld.data:closingProcess.moneyReceivedMulti[0].dateReceived::VARCHAR)::bigint as feedate1,
        (ld.data:closingProcess.moneyReceivedMulti[0].amountReceived::VARCHAR) as referral_fee_1,
        (ld.data:closingProcess.moneyReceivedMulti[1].dateReceived::VARCHAR)::bigint as feedate2,
        (ld.data:closingProcess.moneyReceivedMulti[1].amountReceived::VARCHAR) as referral_fee_2,
        ld.data:closingProcess.moneyReceived.difference::VARCHAR as difference,
        ld.data:closingProcess.rebate.type::VARCHAR as rebatetype,
        ld.data:closingProcess.documentation.received.user.fullName::VARCHAR as finalreviewname,
        (ld.data:closingProcess.documentation.received.checkedAt::VARCHAR)::bigint as finalreviewdate,
        ld.data:closingProcess.documentation.reviewers[0].user.fullName::VARCHAR as reviewer1name,
        (ld.data:closingProcess.documentation.reviewers[0].checkedAt::VARCHAR)::bigint as reviewer1date,
        ld.data:closingProcess.documentation.reviewers[1].user.fullName::VARCHAR as reviewer2name,
        (ld.data:closingProcess.documentation.reviewers[1].checkedAt::VARCHAR)::bigint as reviewer2date
    from {{ ref('leads_data') }} ld,
	lateral flatten(input => ld.data:closingProcess.closingDetails) flat_ld
    where ld.data:closingProcess is not null
{% else %}
    select 
        ld.lead_id,
        (json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'created')::bigint as Updatedate,
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
{% endif %}
)
,
leads_info_no_array AS (
{% if target.type == 'snowflake' %}
    select
        ld.lead_id,
        ld.data:closingProcess.expectedReferralFee::VARCHAR as expected_fee,
        ld.data:closingProcess.moneyReceived.amountReceived::VARCHAR as referralamountreceived,
        (ld.data:closingProcess.moneyReceived.dateReceived::VARCHAR)::bigint as referralfeedate,
        ld.data:closingProcess.moneyReceived.accountReceived::VARCHAR as bankaccount,
        ld.data:closingProcess.moneyReceived.difference::VARCHAR as difference,
        ld.data:closingProcess.rebate.type::VARCHAR as rebatetype,
        ld.data:closingProcess.documentation.received.user.fullName::VARCHAR as finalreviewname,
        (ld.data:closingProcess.documentation.received.checkedAt::VARCHAR)::bigint as finalreviewdate,
        ld.data:closingProcess.documentation.reviewers[0].user.fullName::VARCHAR as reviewer1name,
        (ld.data:closingProcess.documentation.reviewers[0].checkedAt::VARCHAR)::bigint as reviewer1date,
        ld.data:closingProcess.documentation.reviewers[1].user.fullName::VARCHAR as reviewer2name,
        (ld.data:closingProcess.documentation.reviewers[1].checkedAt::VARCHAR)::bigint as reviewer2date
    from {{ ref('leads_data') }} ld
{% else %}
    select 
        ld.lead_id,
        ld.data->'closingProcess'->>'expectedReferralFee' as expected_fee,
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
    from {{ ref('leads_data') }} ld
{% endif %}
)
,
expected_fee_cte AS (
{% if target.type == 'snowflake' %}
    select
        ld.lead_id,
        cast(ld.data:closingProcess.expectedReferralFee::VARCHAR as decimal) as expected_fee
    from {{ ref('leads_data') }} ld
{% else %}
    select 
        ld.lead_id,
        cast(ld.data->'closingProcess'->>'expectedReferralFee' as decimal) as expected_fee
    from {{ ref('leads_data') }} ld
{% endif %}
)
,
leads_info_no_array_final AS (
    select lina.lead_id,
    cast(lina.expected_fee as decimal) as expected_fee,
    cast(lina.referralamountreceived as decimal) as referralamountreceived,
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
        {% if target.type == 'snowflake' %}
        case when RLIKE(lc.closedate::text, '^[0-9]{13}$', 'i') then
						 to_timestamp(lc.closedate::bigint/1000)
						 when lc.closedate = '0' then null
						 when lc.closedate = '' then null
					else
						 to_timestamp(REGEXP_SUBSTR(lc.closedate,'\\d+[-_]\\d+[-_]\\d+$'), 'yyyy-MM-dd') end as closedate,
        {% else %}
		case when lc.closedate::text ~* '^[0-9]{13}$' then
						 to_timestamp(lc.closedate::bigint/1000)
						 when lc.closedate = '0' then null
						 when lc.closedate = '' then null
					else
						 to_timestamp(substring(lc.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') end as closedate,
	    {% endif %}
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
        cast(expected_fee as decimal) as expected_fee,
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
rewards_cte_org AS (
{% if target.type == 'snowflake' %}
    SELECT
		DISTINCT(ld.lead_id),
        flat_ld.value:type::VARCHAR as rewardtype,
        flat_ld.value:amount::VARCHAR as rewardamount_final,
        (ld.data:closingProcess.rebate.paidDate::VARCHAR)::bigint as rewardpaidDate,
        flat_ld.value:user.id::VARCHAR as senderID,
        flat_ld.value:user.fullName::VARCHAR as senderName,
        (flat_ld.value:created::VARCHAR)::bigint as rewardcreateddate
    FROM {{ ref('leads_data') }} ld,
	lateral flatten(input => ld.data:closingProcess.rebate.statuses) flat_ld
{% else %}
    SELECT
		DISTINCT(ld.lead_id),
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'type' as rewardtype,
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'amount' as rewardamount_final,
        (ld.data->'closingProcess'->'rebate'->>'paidDate')::bigint as rewardpaidDate,
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->'user'->>'id' as senderID,
        json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->'user'->>'fullName' as senderName,
        (json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'created')::bigint as rewardcreateddate
    FROM {{ ref('leads_data') }} ld
{% endif %}
),
final_reward_cte AS (
    select
        rc.lead_id,
        max(rewardcreateddate) as max_reward_created_date,
        rewardpaidDate,
        rewardtype,
        rewardamount_final,
        senderID,
        senderName
    from rewards_cte_org rc
    group by rc.lead_id,rewardtype,senderID,senderName, rewardpaiddate, rewardamount_final
),
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
),
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
lsu_cte AS (
{% if target.type == 'snowflake' %}
    select
        lead_id,
        created,
        data:closeDate::VARCHAR as closeDate,
        data:salePrice::VARCHAR as lsu_SalePrice,
        data:agentCommissionAmount::VARCHAR as agentCommissionAmount,
        data:agentCommissionPercentage::VARCHAR as agentCommissionPercent,
        data:lender::VARCHAR as lender,
        data:propertyAddressOrMLS::VARCHAR as address,
        data:rewardAmount::VARCHAR as rewardAmount
    from {{ ref('lead_status_updates') }} where status = 'Closed Closed' and data:closeDate::VARCHAR is not null
{% else %}
    select 
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
{% endif %}
)
, 
lsu_final_cte AS (
    select
    lead_id,
    created,
    regexp_replace(lc.lsu_salePrice,'([^0-9.])','') as saleprice,
    {% if target.type == 'snowflake' %}
    nullif(
          regexp_replace(
             lc.agentCommissionAmount,
             '^.*[^\\d.].*$',
             'x'),
          'x') as commission,
    nullif(
          regexp_replace(
             agentCommissionPercent,
             '^.*[^\\d.].*$',
             'x'),
          'x') as commissionpercent,
    {% else %}
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
    {% endif %}
        lc.address,
        lc.rewardAmount,
        lc.lender,
        replace(lc.closedate,'/','-') as closedate,
    ROW_NUMBER() over (PARTITION BY 
    lead_id order by created DESC) as row_id
    from lsu_cte lc
)
,
lsu_final AS (
    select
    lead_id,
    cast(nullif(replace(saleprice,',',''),'') as decimal) as lsu_saleprice,
    cast(nullif(commission,'') as decimal) as commission,
    cast(nullif(commissionpercent,'') as decimal) as commissionpercent,
    address,
    lender,
    closedate,
    row_id
from lsu_final_cte
where row_id = 1 
)
,
alt_closing_process AS (
{% if target.type == 'snowflake' %}
    select
        ld.lead_id,
        (ld.data:closingProcess.closingDetails.created::VARCHAR)::bigint as Updatedate,
        ld.data:closingProcess.expectedReferralFee::VARCHAR as expected_fee,
        data:closingProcess.closingDetails.isBuyer::VARCHAR as is_buyer,
        data:closingProcess.closingDetails.isSeller::VARCHAR as is_seller,
        data:closingProcess.closingDetails.closeDate::VARCHAR as closedate,
        ld.data:closingProcess.closingDetails.lender::VARCHAR as lender,
        ld.data:closingProcess.closingDetails.propertyAddressOrMLS::VARCHAR as Address,
        ld.data:closingProcess.closingDetails.transactionState::VARCHAR as TransactionState,
        ld.data:closingProcess.closingDetails.salePrice::VARCHAR as salePrice,
        ld.data:closingProcess.closingDetails.agentCommissionAmount::VARCHAR as AgentCommissionAmount,
        ld.data:closingProcess.closingDetails.agentCommissionPercentage::VARCHAR as commissionpercent,
        ld.data:closingProcess.moneyReceived.amountReceived::VARCHAR as referralamountreceived,
        (ld.data:closingProcess.moneyReceived.dateReceived::VARCHAR)::bigint as referralfeedate,
        ld.data:closingProcess.moneyReceived.accountReceived::VARCHAR as bankaccount,
        ld.data:closingProcess.moneyReceived.difference::VARCHAR as difference,
        ld.data:closingProcess.rebate.type::VARCHAR as rebatetype,
        ld.data:closingProcess.documentation.received.user.fullName::VARCHAR as finalreviewname,
        (ld.data:closingProcess.documentation.received.checkedAt::VARCHAR)::bigint as finalreviewdate,
        ld.data:closingProcess.documentation.reviewers[0].user.fullName::VARCHAR as reviewer1name,
        (ld.data:closingProcess.documentation.reviewers[0].checkedAt::VARCHAR)::bigint as reviewer1date,
        ld.data:closingProcess.documentation.reviewers[1].user.fullName::VARCHAR as reviewer2name,
        (ld.data:closingProcess.documentation.reviewers[1].checkedAt::VARCHAR)::bigint as reviewer2date
    from {{ ref('leads_data') }} ld
    where  ld.data:closingProcess is not null
{% else %}
    select 
        ld.lead_id,
        (ld.data->'closingProcess'->'closingDetails'->>'created')::bigint as Updatedate,
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
    from {{ ref('leads_data') }} ld
    where  ld.data->'closingProcess' is not null
{% endif %}
)
,
alt_normalize_dates AS(
	SELECT
		ac.lead_id,
        max(ac.Updatedate) as max_date,
        ac.expected_fee,
        ac.is_buyer,
        ac.is_seller,
        {% if target.type == 'snowflake' %}
        case when RLIKE(ac.closedate::text, '^[0-9]{13}$', 'i') then
						 to_timestamp(ac.closedate::bigint/1000)
						 when ac.closedate = '0' then null
						 when ac.closedate = '' then null
					else
						 to_timestamp(REGEXP_SUBSTR(ac.closedate,'\\d+[-_]\\d+[-_]\\d+$'), 'yyyy-MM-dd') end as closedate,
        {% else %}
		case when ac.closedate::text ~* '^[0-9]{13}$' then
						 to_timestamp(ac.closedate::bigint/1000)
						 when ac.closedate = '0' then null
						 when ac.closedate = '' then null
					else
						 to_timestamp(substring(ac.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') end as closedate,
	    {% endif %}
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
    cast(expected_fee as decimal) as expected_fee,
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
    from {{ ref('current_assignments') }} ca 
    left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = ca.profile_aggregate_id
    left outer join (
            select aggregate_id, min(PhoneNumber) as rc_office_phone
                               from (
                                        {% if target.type == 'snowflake' %}
                                        select aggregate_id, fp.value:phoneType::VARCHAR as PhoneType,
                                                fp.value:phoneNumber::VARCHAR as phoneNumber
                                        from {{ ref('partner_user_profiles') }} pup,
                                        lateral flatten(input => pup.phones) fp
                                        {% else %}
                                        select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                                json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                        from {{ ref('partner_user_profiles') }}
                                       {% endif %}
                                       ) pop
                                      where lower(pop.Phonetype) = 'office'
                                    group by aggregate_id) rop on rop.aggregate_id = ca.profile_aggregate_id
    left outer join (
            select aggregate_id, min(PhoneNumber) as rc_mobile_phone
                               from (
                                        {% if target.type == 'snowflake' %}
                                        select aggregate_id, fp.value:phoneType::VARCHAR as PhoneType,
                                            fp.value:phoneNumber::VARCHAR as phoneNumber
                                        from {{ ref('partner_user_profiles') }} pup,
                                        lateral flatten(input => pup.phones) fp
                                        {% else %}
                                        select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                        from {{ ref('partner_user_profiles') }}
                                        {% endif %}
                                       ) pop
                                      where lower(pop.Phonetype) = 'mobile'
                                    group by aggregate_id) rmp on rmp.aggregate_id = ca.profile_aggregate_id
    where ca.role = 'REFERRAL_COORDINATOR'
)
,
transaction_state_cte AS (
{% if target.type == 'snowflake' %}
    select lead_id,
    flat_ld.value:transactionState::VARCHAR as TransactionState_alt
    from {{ ref('leads_data') }} ld,
	lateral flatten(input => ld.data:closingProcess.closingDetails) flat_ld
{% else %}
    select lead_id,
    json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'transactionState' as TransactionState_alt
    from {{ ref('leads_data') }} ld
{% endif %}
)
,
transaction_state_final AS (
    select
    {% if target.type != 'snowflake' %}
    distinct on (lead_id)
    {% endif %}
    * from transaction_state_cte
    where TransactionState_alt is not null
    {% if target.type == 'snowflake' %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY lead_id) = 1
    {% endif %}

)
,
t_state_final as (
    select t1.id,
    coalesce(f.transactionState,tsf.TransactionState_alt) as TransactionState
    from 
    {{ ref('leads_data_v3') }} t1 
    left join final_finance f on f.lead_id = t1.id 
    left join transaction_state_final tsf on tsf.lead_id = t1.id
)
select 
distinct t1.id,
    (dmc.datemarkedclosed - interval '5 hours') as date_marked_closed_final,
    t1.client_name,
    t1.client_email,
    t1.client_phone,
    t1.agent_name,
    t1.agent_email,
    t1.agent_phone,
    t1.brokerage_code,
    t1.full_name,
    rc.rc_name as rcname,
    rc.rc_email as rcemail,
    rc.rc_office_phone as rc_office_phone,
    rc.rc_mobile_phone as rc_mobile_phone,
    case when f.is_buyer = 'true' then 'BUY'
    when f.is_buyer = 'false' then 'SELL' else l.transaction_type end as transaction_type,
    t1.bank_name,
    tsf.transactionState as tstate_final,
    aff.transactionState as ldstate1,
    f.TransactionState as ldstate,
    coalesce(tsf.TransactionState,t1.state) as TransactionState,
    /*f.Address,
    lf.address,*/
    coalesce(f.Address,lf.address) as address,
    f.closedate,
    lf.closedate as lsu_close_date,
    coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice) as saleprice,
    coalesce(cast(f.AgentCommissionAmount as decimal),lf.commission) as agent_commission,
    coalesce(cast(nullif(f.Commissionpercent,'') as decimal),lf.commissionpercent) as commission_percent,
    coalesce(f.lenderclosedwith,lf.lender) as lenderclosedwith,
    l.documentation_received,
    coalesce(f.reviewer1name,aff.reviewer1name) as reviewer1name,
    coalesce(f.reviewer1date,aff.reviewer1date) as reviewer1date,
    coalesce(f.reviewer2name,aff.reviewer2name) as reviewer2name,
    coalesce(to_timestamp(f.reviewer2date/1000),to_timestamp(aff.reviewer2date/1000)) as reviewer2date,
    {% if target.type == 'snowflake' %}
    DATEDIFF('day', f.closedate, coalesce(f.reviewer1date,aff.reviewer1date)) as reviewer1delay,
    {% else %}
    extract(days from(coalesce(f.reviewer1date,aff.reviewer1date) - f.closedate)) as reviewer1delay,
    {% endif %}
    coalesce(f.referralfeedate,linaf.referralfeedate) as referralfeedate,
    coalesce(cast(f.referralamountreceived as decimal),linaf.referralamountreceived) as referralamountreceived,
    coalesce(f.bankaccount,linaf.bankaccount) as bankaccount,
    cast(f.difference as decimal) as difference,
    coalesce(f.expected_fee,linaf.expected_fee) as expected_fee,
    {% if target.type == 'snowflake' %}
    DATEDIFF('day', f.closedate, f.referralfeedate) as referralfeedelay,
    {% else %}
    extract(days from (f.referralfeedate - f.closedate)) as referralfeedelay,
    {% endif %}
    case when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<100000 then 350
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice) >=100000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<150000 AND t1.bank_name not like '%Freedom%' then 650
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=150000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<250000 AND t1.bank_name not like '%Freedom%' then 900
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=250000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<350000 AND t1.bank_name not like '%Freedom%' then 1000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=350000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<450000 AND t1.bank_name not like '%Freedom%' then 1250
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=450000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<500000 AND t1.bank_name not like '%Freedom%' then 1750
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=500000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<550000 AND t1.bank_name not like '%Freedom%' then 2000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=550000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<600000 AND t1.bank_name not like '%Freedom%' then 2300
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=600000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<700000 AND t1.bank_name not like '%Freedom%' then 2400
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=700000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<800000 AND t1.bank_name not like '%Freedom%' then 2750
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=800000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<900000 AND t1.bank_name not like '%Freedom%' then 3100
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=900000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1000000 AND t1.bank_name not like '%Freedom%' then 3500
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1000000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1100000 AND t1.bank_name not like '%Freedom%' then 4000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1100000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1200000 AND t1.bank_name not like '%Freedom%' then 4350
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1200000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1300000 AND t1.bank_name not like '%Freedom%' then 4750
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1300000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1400000 AND t1.bank_name not like '%Freedom%' then 5500
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1400000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1500000 AND t1.bank_name not like '%Freedom%' then 6000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1500000 AND t1.bank_name not like '%Freedom%' then 6500
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<100000 AND t1.bank_name like '%Freedom%' then 350
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice) >=100000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<150000 AND t1.bank_name like '%Freedom%' then 650
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=150000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<250000 AND t1.bank_name like '%Freedom%' then 900
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=250000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<350000 AND t1.bank_name like '%Freedom%' then 1000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=350000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<450000 AND t1.bank_name like '%Freedom%' then 1250
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=450000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<500000 AND t1.bank_name like '%Freedom%' then 1750
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=500000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<550000 AND t1.bank_name like '%Freedom%' then 2000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=550000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<600000 AND t1.bank_name like '%Freedom%' then 2300
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=600000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<700000 AND t1.bank_name like '%Freedom%' then 2400
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=700000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<800000 AND t1.bank_name like '%Freedom%' then 2750
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=800000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<900000 AND t1.bank_name like '%Freedom%' then 3100
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=900000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1000000 AND t1.bank_name like '%Freedom%' then 3500
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1000000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1100000 AND t1.bank_name like '%Freedom%' then 4000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1100000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1200000 AND t1.bank_name like '%Freedom%' then 4350
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1200000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1300000 AND t1.bank_name like '%Freedom%' then 4750
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1300000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1400000 AND t1.bank_name like '%Freedom%' then 5500
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1400000 AND coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)<1500000 AND t1.bank_name like '%Freedom%' then 6000
    when coalesce(cast(nullif(f.saleprice1,'') as decimal),lf.lsu_saleprice)>=1500000 AND t1.bank_name like '%Freedom%' then 6500
    else null end as rebate_amount,
    case when coalesce(f.TransactionState,t1.state) in ('TN','Tennessee') then 'Yes-Giftcard'
    when coalesce(tsf.TransactionState,t1.state) in ('KS','Kansas') then 'Yes-$1,000 Giftcard'
    when coalesce(tsf.TransactionState,t1.state) in ('OR','Oregon','MS','Mississippi','OK','Oklahoma','AL','Alabama') then 'Yes'
    when coalesce(tsf.TransactionState,t1.state) in ('NJ','New Jersey') then 'ALT'
    when coalesce(tsf.TransactionState,t1.state) in ('LA','Louisiana','AK','Alaska','MO','Missouri','IA','Iowa') then 'No Reward'
    else 'No'
    end as state_restrictions,
    rff.rewardpaiddate,
    l.rebate_paid,
    rff.rewardtype,
    rewardamount_final,
    rff.senderID,
    rff.senderName,
    t1.major_status,
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
	f.row_id
from {{ ref('leads_data_v3') }} t1
left join final_finance f on f.lead_id = t1.id
left join rewards_final_cte rff on rff.lead_id = t1.id
left join leads l on l.id = t1.id
left join lsu_final lf on lf.lead_id = t1.id
left join alt_final_finance aff on aff.lead_id = t1.id
left join rc_cte rc on rc.lead_id = t1.id
left join leads_info_no_array_final linaf on linaf.lead_id = t1.id
left join expected_fee_cte efc on efc.lead_id = t1.id 
left join t_state_final tsf on tsf.id = t1.id
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(max(lsu.created)) as datemarkedclosed
	FROM {{ ref('lead_status_updates') }} lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status like 'Closed Closed'
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
where t1.major_status = 'Closed' 
order by t1.id desc