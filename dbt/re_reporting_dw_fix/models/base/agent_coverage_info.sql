with agent_language_cte AS
(
    select 
        distinct pup.aggregate_id,
        pup.id, 
        pup.first_name as AgentFirstName, 
        pup.last_name as AgentLastName,
        pup.email, 
        pup.eligibility_status, 
        pup.verification_status,
        concat(pup.first_name,' ',pup.last_name) as AgentName, 
        pup.email as AgentEmail, 
        pup.phone as AgentPhone, 
        pup.brokerage_code as BrokerageCode,
        {% if target.type == 'snowflake' %}
        case when pup.brokerage_code is null then pup.data:brokerage.fullName::VARCHAR else b.full_name end as brokerage_name,
        pup.data:languages::VARCHAR as languages,
        {% else %}
        case when pup.brokerage_code is null then pup.data->'brokerage'->>'fullName' else b.full_name end as brokerage_name, 
        pup.data->>'languages' as languages,
        {% endif %}
        pup.created
        
    from 
        {{ ref('partner_user_profiles') }} pup
    join {{ ref('partner_user_roles') }} pur on pur.user_profile_id = pup.id
    left join {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code
    where pur.role = 'AGENT'
)

,
cbsa_locations_cte as 
(
    select 
        distinct pcz.profile_id,
        cb.st,
        cb.county,
        count(distinct pcz.zip) as zipCount
    from {{ ref('profile_coverage_zips') }} pcz
    join {{ ref('cbsa_locations') }} cb on lpad(cb.zip,5,'0') = pcz.zip
    group by pcz.profile_id, cb.st,cb.county
)
,
agent_county_count as (
    select 
        distinct pcz.profile_id,
        count(DISTINCT cbsa.county) as countyCount
    from {{ ref('profile_coverage_zips') }} pcz
    join {{ ref('cbsa_locations') }} cbsa on lpad(cbsa.zip,5,'0') = pcz.zip
    group by pcz.profile_id
)
select 
    alc.aggregate_id,
    concat(alc.AgentFirstName,' ',alc.AgentLastName) as AgentName,
    alc.email,
    amp.agentMobilePhone,
    aop.agentOfficePhone,
    alc.brokeragecode,
    alc.brokerage_name,
    alc.languages,
    alc.verification_status,
    cbsa.st,
    cbsa.county,
    cbsa.zipCount,
    cc.countyCount
from agent_language_cte alc 
left join cbsa_locations_cte cbsa on cbsa.profile_id = alc.id
left join agent_county_count cc on cc.profile_id = alc.id
left outer join (select aggregate_id, min(PhoneNumber) as agentMobilePhone
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
                                      where lower(pop.Phonetype) = 'mobilephone'
                                    group by aggregate_id)  amp on amp.aggregate_id = alc.aggregate_id
    left outer join (select aggregate_id, min(PhoneNumber) as agentOfficePhone
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
                                    group by aggregate_id)  aop on aop.aggregate_id = alc.aggregate_id