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
	 cast(partner_id as {{ uuid_formatter() }}) as partner_id,
	 first_name,
	 last_name,
	 email,
	 phone,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated,
	 {{ parse_json('data') }} as data,
	 {{ parse_json('phones') }} as phones,
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
	 cast(aggregate_id as {{ uuid_formatter() }}) AS aggregate_id
FROM {{source('public', 'raw_import_partner_user_profiles')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
ORDER by 1 ASC