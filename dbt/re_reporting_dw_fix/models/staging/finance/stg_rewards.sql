with closed_status as(
{% if target.type == 'snowflake' %}
	select
	 lsu.lead_id,
	 pup.email,
	 lsu.data:amountCharged.value::VARCHAR as lsu_value,
	 lsu.data:referenceOrderID::VARCHAR as lsu_reference_id,
	 lsu.data:campaign::VARCHAR as campaign,
	 lsu.data:rewardAmount::VARCHAR as lsu_reward_amount
	from
		{{ ref('lead_status_updates') }} lsu
	LEFT JOIN {{ ref('partner_user_profiles') }} pup
	ON lsu.profile_aggregate_id = pup.aggregate_id
	where
		status = 'Closed Reward Ready Ordered'
{% else %}
	select 
	 lsu.lead_id,
	 pup.email,
	 lsu.data->'amountCharged'->>'value' as lsu_value,
	 lsu.data->>'referenceOrderID' as lsu_reference_id,
	 lsu.data->>'campaign' as campaign,
	 lsu.data->>'rewardAmount' as lsu_reward_amount
	from 
		{{ ref('lead_status_updates') }} lsu 
	LEFT JOIN {{ ref('partner_user_profiles') }} pup
	ON lsu.profile_aggregate_id = pup.aggregate_id
	where 	
		status = 'Closed Reward Ready Ordered'
{% endif %}
),
reward as(
{% if target.type == 'snowflake' %}
select lead_id, flat_ld.value:amount::VARCHAR as rewardamount_final
from {{ ref('leads_data') }} ld,
lateral flatten(input => ld.data:closingProcess.rebate.statuses) flat_ld
{% else %}
select lead_id, json_array_elements(ld.data->'closingProcess'->'rebate'->'statuses')->>'amount' as rewardamount_final 
from {{ ref('leads_data') }} ld
{% endif %}
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
left join {{ ref('leads_data_v3') }} ldv 
on btr.lead_id=ldv.id
)
select
{% if target.type != 'snowflake' %}
distinct on (referencelineitemid)
{% endif %}
* from add_agg_id
{% if target.type == 'snowflake' %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY referencelineitemid ORDER BY referencelineitemid) = 1
{% endif %}
