select pz.profile_id, pup.email, pz.zip, cbsa.city, cbsa.st as state, cbsa.county,cbsa.cbmsa,
       pup.first_name, pup.last_name, pup.brokerage_code, pup.verification_status, eligibility_status, pup.phone, amp.agentMobilePhone, aop.agentOfficePhone,
       {% if target.type == 'snowflake' %}
       coalesce(b.full_name,pup.data:brokerage.name::VARCHAR) as brokerage_name
       {% else %}
       coalesce(b.full_name,pup.data->'brokerage'->>'name') as brokerage_name
       {% endif %}
  from {{ ref('profile_coverage_zips') }} pz 
  join {{ ref('partner_user_profiles') }} pup
    on pz.profile_id = pup.id
    left join {{ ref('brokerages') }} b on b.brokerage_code = pup.brokerage_code 
    left outer join {{ ref('cbsa_locations') }} cbsa on pz.zip = lpad(cbsa.zip,5,'0')
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
                                    group by aggregate_id)  amp on amp.aggregate_id = pup.aggregate_id
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
                                    group by aggregate_id)  aop on aop.aggregate_id = pup.aggregate_id