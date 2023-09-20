with closed_status as(
	select 
	 lsu.lead_id,
	 pup.email,
	 {% if target.type == 'snowflake' %}
	 lsu.data:amountCharged.value::VARCHAR as lsu_value,
	 lsu.data:referenceOrderID::VARCHAR as lsu_reference_id,
	 lsu.data:campaign::VARCHAR as campaign,
	 lsu.data:rewardAmount::VARCHAR as lsu_reward_amount
	 {% else %}
	 lsu.data->'amountCharged'->>'value' as lsu_value,
	 lsu.data->>'referenceOrderID' as lsu_reference_id,
	 lsu.data->>'campaign' as campaign,
	 lsu.data->>'rewardAmount' as lsu_reward_amount
	 {% endif %}
	from 
		{{ ref('lead_status_updates') }} lsu 
	LEFT JOIN {{ ref('partner_user_profiles') }} pup
	ON lsu.profile_aggregate_id = pup.aggregate_id
	where 	
		status = 'Closed Reward Ready Ordered'
),
add_agg_id AS(
  select
	pup.aggregate_id,
	pur.role,
	CASE WHEN cs.lead_id IS NOT NULL then cs.lead_id ELSE btr.lead_id END AS lead_id,
	CONCAT(btr.customer_first_name, ' ', btr.customer_first_name) as name,
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
    cs.lsu_reward_amount
	--btr.*,
from 
{{ ref('base_tango_rewards') }} btr 
left join {{ ref('partner_user_profiles') }}  pup
on lower(btr.customer_email)=lower(pup.email)
left join {{ ref('partner_user_roles') }} pur
on pup.id=pur.user_profile_id 
left join closed_status cs 
on btr.referenceorderid=cs.lsu_reference_id
)
{% if target.type == 'snowflake' %}
select * from add_agg_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY referencelineitemid ORDER BY referencelineitemid) = 1
{% else %}
select distinct on (referencelineitemid) * from add_agg_id
{% endif %}
 
