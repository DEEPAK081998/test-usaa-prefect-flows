with pmc_leads as (
	select 
		ldv.id as lead_id,
		ldv.agent_aggregate_id,
		ldv.zip,
		cl.county
	from
		{{ ref('leads_data_v3') }} ldv
		left join {{ ref('cbsa_locations') }} cl
			on ldv.zip = cl.zip
	where 
		lower(bank_name) = 'pennymac'
		and ldv.agent_aggregate_id is not null
		and agent_aggregate_id != '00000000-0000-0000-0000-000000000000'
		and (lower(client_name) not like '%test%')
),
agent_route_offer as (
	select
		fara.lead_id,
		fara.agent_aggregate_id,
		fara.agentactivity as offer_activity,
		fara.created as offer_timestamp,
		lead(fara.agent_aggregate_id, 1) over
			(partition by fara.lead_id order by fara.created) as lead_agent,
		lead(fara.agentactivity, 1) over
			(partition by fara.lead_id order by fara.created) as lead_activity,
		lead(fara.created, 1) over
			(partition by fara.lead_id order by fara.created) as lead_timestamp,
		pup.brokerage_code,
		pmc.zip,
		pmc.county
	from 
		{{ ref('fct_agent_referral_activities') }} fara
		join {{ ref('partner_user_profiles') }} pup
			on fara.agent_aggregate_id = pup.aggregate_id
		join pmc_leads pmc 
			on fara.lead_id = pmc.lead_id
	where 
		lower(fara.agentactivity) in ('offered','accepted')
		and fara.agent_aggregate_id != '00000000-0000-0000-0000-000000000000'	
),
agent_route_offer_accepted as (
	select
		lead_id,
		agent_aggregate_id,
		row_number() over (partition by lead_id order by offer_timestamp) as routing_line,
		offer_activity,
		offer_timestamp,
		case when
			lower(lead_activity) = 'accepted' and agent_aggregate_id = lead_agent then lead_activity
			end as accept_activity,
		case when
			lower(lead_activity) = 'accepted' and agent_aggregate_id = lead_agent then lead_timestamp
			end as accept_timestamp,
		brokerage_code,
		zip,
		county
	from 
		agent_route_offer
	where
		lower(offer_activity) = 'offered'
),
agent_route_accepted as (
	select distinct
		max(accept_timestamp) as activity_timestamp,
		lead_id,
		agent_aggregate_id
	from
		agent_route_offer_accepted
	where 
		lower(accept_activity) = 'accepted'
	group by
		lead_id,
		agent_aggregate_id
),
agent_accept_hard_assigned as (
	select
		pmc.lead_id,
		pmc.agent_aggregate_id,
		'Accepted' as accept_activity,
		pmc.zip,
		pmc.county
	from 
		agent_route_accepted ara
		right join PMC_leads pmc 
		on ara.lead_id = pmc.lead_id
			and ara.agent_aggregate_id = pmc.agent_aggregate_id
	where 
		ara.lead_id is null
),
agent_route_hard_assigned as (
	select
		row_number() over (partition by lsu.lead_id order by lsu.created desc) as row_id,
		lsu.created as accept_timestamp,
		lsu.lead_id,
		agent_aggregate_id,
		accept_activity,

		{% if target.type == 'snowflake' %}

		data:brokerage.brokerageCode::varchar as brokerage_code,

		{% else %}

		data -> 'brokerage' ->> 'brokerageCode' as brokerage_code,

		{% endif %}

		zip,
		county		
	from
		{{ ref('lead_status_updates') }} lsu
		join agent_accept_hard_assigned aaha
			on aaha.lead_id = lsu.lead_id
	where 
		lsu.status = 'Active Agent Assigned'
),
agent_hard_accept as (
	select 
		accept_timestamp,
		lead_id,
		agent_aggregate_id,
		accept_activity,
		brokerage_code,
		zip,
		county	
	from 
		agent_route_hard_assigned
	where
		row_id = 1
),
pan_agents as (
	select distinct

		{% if target.type == 'snowflake' %}

		profile_aggregate_id::varchar as agent_aggregate_id,

		{% else %}

		profile_aggregate_id::uuid as agent_aggregate_id,

		{% endif %}
		 
		created::timestamptz as created,
		deleted::timestamptz as deleted
	from 
		{{ source('public', 'raw_import_profile_qualifications') }}
	where 
		qualification ilike '%E2A46D0A-6544-4116-8631-F08D749045AC%'
),
pmc_agent_routing as (
select distinct
	coalesce(aroa.lead_id, aha2.lead_id) as lead_id,
	coalesce(aroa.agent_aggregate_id, aha2.agent_aggregate_id) as agent_aggregate_id,
	case when
		aha.lead_id is not null or aha2.lead_id is not null then 1
		else 0 
		end as hard_assigned,
	aroa.offer_activity,
	aroa.offer_timestamp,
	aroa.routing_line,
	coalesce(aroa.accept_activity,aha2.accept_activity) as accept_activity,
	coalesce(aroa.accept_timestamp,aha2.accept_timestamp) as accept_timestamp,
	coalesce(aroa.zip, aha2.zip) as zip,
	coalesce(aroa.county, aha2.county) as county,
	coalesce(aroa.brokerage_code,aha2.brokerage_code) as brokerage_code,
	case when coalesce(aroa.accept_timestamp,aha2.accept_timestamp) is not null then
		row_number() over
			(partition by coalesce(aroa.lead_id, aha2.lead_id) 
			order by coalesce(aroa.accept_timestamp,aha2.accept_timestamp) desc nulls last) 
		end as accept_row_id
from
	agent_route_offer_accepted aroa
	left join agent_hard_accept aha
		on aroa.lead_id = aha.lead_id
	full outer join agent_hard_accept aha2
		on aroa.lead_id = aha.lead_id
			and aroa.accept_timestamp = aha2.accept_timestamp
)
select 
	lead_id,
	pmc.agent_aggregate_id,
	hard_assigned,
	offer_activity,
	offer_timestamp,
	routing_line,
	accept_activity,
	accept_timestamp,
	case when 
		coalesce(accept_row_id,0) = 1 then 1
		else 0
		end as last_accept,
	zip,
	county,
	brokerage_code,
	case when
		pa.agent_aggregate_id is not null then 1
		else 0
		end as panagent
from 
	pmc_agent_routing pmc
	left join pan_agents pa 
		on pmc.agent_aggregate_id = pa.agent_aggregate_id
		and (((pmc.offer_timestamp between pa.created and pa.deleted)
			or (pmc.offer_timestamp >= pa.created and pa.deleted is null))
			or ((pmc.accept_timestamp between pa.created and pa.deleted)
			or (pmc.accept_timestamp >= pa.created and pa.deleted is null)))