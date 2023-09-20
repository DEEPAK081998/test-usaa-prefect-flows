with closed_status as(
	select 
	 lead_id,
	 pup.email,
	 lsu.data->'amountCharged'->>'value' as lsu_value,
	 lsu.data->>'referenceOrderID' as lsu_reference_id,
	 lsu.data->>'campaign' as campaign,
	 lsu.data->>'rewardAmount' as lsu_reward_amount
	from 
		{{ ref('lead_status_updates') }} lsu 
        left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = lsu.profile_aggregate_id
	where 	
		status = 'Closed Reward Ready Ordered'
),
reward as(
select lead_id, json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'amount' as rewardamount_final 
from {{ ref('leads_data') }} ld
),
reward_amount as(
select * from reward where rewardamount_final is not null
),
add_agg_id AS(
  select
	pup.aggregate_id,
	pur.role,
	cs.lead_id,
	CONCAT(btr.customer_first_name, ' ', btr.customer_last_name) as name,
	btr.customer_email as email,
	btr.dateissued,
	pup.phone as phone,
	cast(btr.value as float) as reward_received,
	cast(btr.currency_code AS text) as currency_code,
	btr.referenceorderid,
    btr.referencelineitemid,
    cs.lsu_reference_id,
    cs.lsu_value,
    cs.campaign,
    cs.lsu_reward_amount,
    ra.rewardamount_final,
    ldv.bank_name
	--btr.*,
from 
{{ ref('base_tango_rewards') }} btr 
left join {{ ref('partner_user_profiles') }} pup
on lower(btr.customer_email)=lower(pup.email)
left join {{ ref('partner_user_roles') }} pur
on pup.id=pur.user_profile_id 
left join closed_status cs 
on btr.referenceorderid=cs.lsu_reference_id
left join reward_amount ra
on btr.lead_id = ra.lead_id
left join leads_data_v3 ldv 
on btr.lead_id=ldv.id
)
select distinct on (referencelineitemid) * from add_agg_id