with 
brokerage_name_cte as (
    select 
        pup.aggregate_id, pup.email,
        {% if target.type == 'snowflake' %}
        case when pup.brokerage_code is null then pup.data:brokerage.fullName::VARCHAR else b.full_name end as brokerage_name
        {% else %}
        case when pup.brokerage_code is null then pup.data->'brokerage'->>'fullName' else b.full_name end as brokerage_name
        {% endif %}
    from {{ ref('partner_user_profiles') }} pup 
    left join {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code
), 
historic_backfill as (
	select
		program_name,
		lead_id::VARCHAR as lead_id,
		MAX(TO_DATE(_rebate_paid_date_, 'MM/dd/yyyy')) as rewardpaiddate,
		MAX(TO_DATE(date_approved, 'MM/dd/yyyy')) as reviewer1date,
		MAX(reviewer_1_approval) as reviewer1name,
		MAX(TO_DATE(date_approved_2, 'MM/dd/yyyy')) as reviewer2date,
		MAX(reviewer_2_approval) as reviewer2name,
		MAX(_pay_via_) as "_pay_via_",
		MAX(documentation_received) as documentation_received,
		MAX(_referral_fee_expected_ ) as _referral_fee_expected_,
		MAX(lender_closes_with ) as lender_closes_with,
        MAX(_final_approval_for_payment_ ) as _final_approval_for_payment_
	from
		public.historic_finance_data_Aug2023
	where 
		(UPPER(program_name) != 'CHASE'
		or  (UPPER(program_name) = 'CHASE' and coalesce(part_of_new_chase_program,'No' ) = 'No'))
		and lead_id is not null
		and LENGTH(lead_id) >= 4
	group by
		program_name,
		lead_id
)
, final_cte as (
    select 
        t1.id,
        t1.date_marked_closed_final,
        t1.client_name,
        t1.client_email,
        t1.client_phone,
        t1.agent_name,
        t1.agent_email,
        t1.agent_phone,
        t1.brokerage_code,
        t1.full_name,
        t1.rcname,
        t1.rcemail,
        t1.rc_office_phone,
        t1.rc_mobile_phone,
        t1.transaction_type,
        t1.bank_name,
        t1.tstate_final,
        t1.ldstate1,
        t1.ldstate,
        t1.transactionstate,
        t1.address,
        t1.closedate,
        t1.lsu_close_date,
        t1.saleprice,
        t1.agent_commission,
        t1.commission_percent,
        t1.lenderclosedwith,
        t1.documentation_received,
        coalesce(t1.reviewer1name, t5.reviewer1name) as reviewer1name,
        coalesce(t1.reviewer1date, t5.reviewer1date) as reviewer1date,
        t1.reviewer2name,
        t1.reviewer2date,
        t1.reviewer1delay,
        t1.referralfeedate,
        t1.referralamountreceived,
        t1.bankaccount,
        t1.difference,
        t1.expected_fee,
        t1.referralfeedelay,
        t1.rebate_amount,
        t1.state_restrictions,
        coalesce(t1.rewardpaiddate, t5.rewardpaiddate) as rewardpaiddate,
        t1.rebate_paid,
        t1.rewardtype,
        t1.rewardamount_final,
        t1.senderid,
        t1.sendername,
        t1.major_status,
        t1.hb_status,
        t1.enrolldate,
        t1.max_upadate_date,
        t1.reward_ready_date_final,
        t1.reward_ready_ordered_date_final,
        t1.max_reward_created_date,
        t1.documentation_received_date,
        t1.feedate1,
        t1.feedate2,
        t1.referral_fee_1,
        t1.referral_fee_2,
        t1.row_id,
        t2.referral_fee_type,
        t3.aggregate_id,
        t3.role,
        t3.name,
        t3.email,
        t3.dateissued,
        t3.phone,
        t3.reward_received,
        t3.currency_code,
        t3.referenceorderid,
        t3.referencelineitemid,
        t3.lsu_reference_id,
        t3.lsu_value,
        t3.campaign,
        t3.lsu_reward_amount,
        t3.rewardamount_final as rewardamount_final_rewards,
        t3.bank_name as bank_name_rewards,
        t4.brokerage_name,
        concat_ws(
        	', ', 
        	case when t1.reviewer1name is null and t5.reviewer1name is not null then 'reviewer1name' else null end,
        	case when t1.reviewer1date is null and t5.reviewer1date is not null then 'reviewer1date' else null end,
        	case when t1.rewardpaiddate is null and t5.rewardpaiddate is not null then 'rewardpaiddate' else null end
    	) as backfilled_columns  
    from {{ ref('stg_finance_portal') }} t1
    left join  {{ ref('stg_fee_type') }} t2
    on t1.id = t2.lead_id
    left join {{ ref('stg_rewards') }} t3
    on t1.id = t3.lead_id
    left join brokerage_name_cte t4
    on t1.agent_email = t4.email
    left join historic_backfill t5
    on t1.bank_name = t5.program_name
    and t1.id::VARCHAR = t5.lead_id::VARCHAR
)
select * from final_cte