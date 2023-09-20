{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
      {'columns': ['email'], 'type': 'btree'},
	  {'columns': ['partner_id'], 'type': 'hash'},
	  {'columns': ['aggregate_id'], 'type': 'hash'},
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
    ],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','partner_user_profiles_pkey') }}")
	]
) }}
SELECT
	cast(id as bigint) as id,
	cast(partner_id as uuid) as partner_id,
	first_name,
	last_name,
	email,
	phone,
	{{ convert_timezone('created','CETDST') }} as created,
	{{ convert_timezone('updated','CETDST') }} as updated,
	cast(data as json) as data,
	cast(phones as json) as phones,
	brokerage_code,
	opening_hours_minutes_of_day,
	closing_hours_minutes_of_day,
	local_timezone,
	accepts_referrals_on_sundays,
	accepts_referrals_on_saturdays,
	receives_referrals_via_text_message,
	eligibility_status,
	training_completed,
	verification_status,
	cast(aggregate_id as uuid) AS aggregate_id,
	accepts_referral_fees,
	cae_legacy_agent,
	chase_standard_identifier,
	division_name,
	'enabled',
	last_offered
FROM {{source('public', 'raw_partner_user_profiles')}}
{% if is_incremental() %}
WHERE {{ convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
ORDER by 1 ASC