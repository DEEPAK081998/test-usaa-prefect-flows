with routing_method AS (select lead_id, 
created,
data->>'routingMethod' as routing_method,
json_array_elements(data->'profileIds') as profiles
from {{ ref('lead_status_updates') }} 
where data->>'routingMethod' is not null 
order by lead_id desc 
)
,
row_id_cte AS (
select *,replace(cast(profiles as text),'"','') as profiles_join
 from routing_method --where row_id = 1
)
,
final AS (
select *,
row_number() over (partition by lead_id,profiles_join order by created desc ) as row_id
from row_id_cte
)
,
final_final AS (
select * from final 
where row_id = 1
)
,
phones_cte AS (
    select pup.aggregate_id, 
    pup.email,
    amp.agentMobilePhone,
    aop.agentOfficePhone
    from {{ ref('partner_user_profiles') }} pup
    left outer join (select aggregate_id, min(PhoneNumber) as agentMobilePhone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from {{ ref('partner_user_profiles') }}) pop
                                      where lower(pop.Phonetype) = 'mobilephone'
                                    group by aggregate_id)  amp on amp.aggregate_id = pup.aggregate_id
    left outer join (select aggregate_id, min(PhoneNumber) as agentOfficePhone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from {{ ref('partner_user_profiles') }}) pop
                                      where lower(pop.Phonetype) = 'office'
                                    group by aggregate_id)  aop on aop.aggregate_id = pup.aggregate_id
)
,
inviting_hla AS (
    select
    lead_id,
    profile_aggregate_id,
    CONCAT(pup.first_name , ' ' , pup.last_name) AS inviting_hla,
    pup.email AS inviting_lo_email
  
FROM 
    {{ ref('lead_status_updates') }} lsu
join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = lsu.profile_aggregate_id
where lower(status) like 'invite%' and role = 'HLA'
)
,
alt_routing AS (
    select lsu.lead_id,
    created,
    data->>'routingMethod' as routing_method,
    row_number() OVER (partition by lead_id order by created desc) as rownum
    from {{ ref('lead_status_updates') }} lsu
    where data->>'routingMethod' is not null 
)
,
alt_routing_final AS (
    select * from alt_routing 
    where rownum = 1
)
,
final_f AS (
select 
    fa.agent_aggregate_id,
    fa.lead_id,
    fa.updateID,
    fa.created,
    fa.status,
    fa.category,
    fa.twilioEvent,
    fa.program_name,
    fa.enrolleddate,
    fa.normalizedclosedate,
    fa.majorstatus,
    fa.inviteddate,
    fa.referralZip,
    fa.agentActivity,
    t1.client_name as customer_name,
    concat(pup.first_name,' ',pup.last_name) as agent_name,
    case when agentactivity = 'Accepted' then 'True'
    when agentactivity in ('Reject','Timeout') then 'False' else 'N/A' end as agent_accepted,
    coalesce(ff.routing_method,f1.routing_method) as routing_method,
    concat(ahh.hlafirstname,' ',ahh.hlalastname) as hla_name,
    ahh.hlaemail as hla_email,
    concat(ahh.lmfirstname,' ',ahh.lmlastname) as lm_name,
    ahh.lmemail,
    concat(ahh.slmfirstname,' ',ahh.slmlastname) as slm_name,
    ahh.slmemail,
    p.email as agent_email,
    coalesce(p.agentMobilePhone,p.agentOfficePhone) as agent_phone,
    ih.inviting_hla,
    1 as row_num
from {{ ref('fct_agent_referral_activities') }} fa 
left join final_final ff on ff.profiles_join::uuid = fa.agent_aggregate_id AND ff.lead_id = fa.lead_id
left join alt_routing_final f1 on f1.lead_id = fa.lead_id
left join agent_hla_hierarchy ahh on ahh.agentaggregateid = fa.agent_aggregate_id
left join phones_cte p on p.aggregate_id = fa.agent_aggregate_id
left join inviting_hla ih on ih.lead_id = fa.lead_id
left join {{ ref('partner_user_profiles') }} pup on pup.aggregate_id = fa.agent_aggregate_id
left join {{ ref('chase_enrollments_test') }} t1 on t1.id = fa.lead_id
where fa.agentactivity in ('Accepted','Reject','Timeout') and fa.agent_aggregate_id <> '81fbb29e-d690-40c0-8035-9aedc659fad5' 

UNION

select 
    t1.agentaggregateid AS agent_aggregate_id,
    t1.id as lead_id,
    lsu.id as  updateID,
    t1.created as created,
    t1.hb_status as status,
    lsu.category as category,
    lsu.data->>'twilioEvent' AS twilioEvent,
    stg.bank_name as program_name,
    t1.enrolleddate,
    t1.normalizedclosedate,
    t1.majorstatus,
    t1.inviteddate,
    t1.normalizedzip as referralZip,
    null as agentActivity,
    t1.client_name as customer_name,
    t1.agent_name,
    t1.agentassigned as agent_accepted,
    t1.routingtype as routing_method,
    concat(t1.hla_first_name,' ',t1.hla_last_name) as hla_name,
    t1.lo_email as hla_email,
    concat(hh.lmfirstname,' ',hh.lmlastname) as lm_name,
    t1.lmemail,
    concat(hh.slmfirstname,' ',hh.slmlastname) as lm_name,
    t1.slmemail,
    t1.agent_email,
    null as agent_phone,
    concat(t1.hla_first_name,' ',t1.hla_last_name) as inviting_hla,
    row_number() over (partition by t1.id order by t1.created desc) as row_num
FROM
{{ ref('chase_enrollments_test') }} t1
left join {{ ref('leads') }} t2 
   ON t1.id = t2.id
   left join {{ ref('lead_status_updates') }} lsu on lsu.lead_id = t1.id
left join {{ ref('stg_lead_banks') }} stg 
   on t2.bank_id = stg.bank_id
LEFT OUTER JOIN {{ ref('hla_hierarchy') }} hh on hh.hlaAggregateID = t1.hlaaggregateid
where t1.majorstatus like '%Invited%' and lsu.status = 'Invited Unaccepted'
)
select *
 from final_f where row_num = 1


