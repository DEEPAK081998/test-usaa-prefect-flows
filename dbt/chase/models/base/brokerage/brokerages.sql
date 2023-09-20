WITH rc_cte as(
SELECT pup.id, pup.brokerage_code,
                    ROW_NUMBER() OVER (partition by brokerage_code ORDER BY PUR.updated DESC) AS row_number,
                    pup.first_name, pup.last_name, concat(pup.first_name, ' ',pup.last_name) as RC_Name,
                    pup.email as RC_Email, 
                    pup.phone as RC_Phone,
                    pup.data->'address'->'city' as rc_city,
                    pup.data->'address'->'state' as rc_state,
                    aop.OfficePhone as RCOfficePhone,
                    amp.MobilePhone as RCMobilePhone
                FROM {{ ref('partner_user_profiles') }} pup 
                join {{ ref('partner_user_roles') }} pur on pup.id = pur.user_profile_id
                left outer join (select aggregate_id, min(PhoneNumber) as MobilePhone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from {{ ref('partner_user_profiles') }} ) pop
                                      where lower(pop.Phonetype) = 'mobilephone'
                                    group by aggregate_id)  amp on amp.aggregate_id = pup.aggregate_id
    left outer join (select aggregate_id, min(PhoneNumber) as OfficePhone
                               from (select aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType,
                                            json_array_elements(phones)->>'phoneNumber' as phoneNumber
                                       from {{ ref('partner_user_profiles') }} ) pop
                                      where lower(pop.Phonetype) = 'office'
                                    group by aggregate_id)  aop on aop.aggregate_id = pup.aggregate_id
               where role = 'REFERRAL_COORDINATOR'),
window_function as(
select * from rc_cte
where row_number = 1
)
select 
	b.id,
	broker_network,
	b.brokerage_code,
	name,
	full_name,
	enabled,
	created,
	updated,
	data,
	opening_hours_minutes_of_day,
	closing_hours_minutes_of_day,
	local_timezone,
	click_to_call_enabled,
	aggregate_id,
	referral_aggreement_email,
	verification_status,
	first_name,
	last_name,
	rc_name,
	rc_email,
	rc_phone,
	rc_city,
	rc_state,
	rcofficephone,
	rcmobilephone
from {{ ref('brokerages_incremental') }}  b
left join window_function wf
on b.brokerage_code = wf.brokerage_code