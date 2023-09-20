{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['hash'], 'type': 'btree'},
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
	  {'columns': ['aggregate_id'], 'type': 'hash'},
	  {'columns': ['bank_id'], 'type': 'hash'},
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','leads_decrypted_pkey') }}")
	]
) }}
SELECT
	 cast(id as bigint) as id,
	 first_name,
	 last_name,
	 email,
	 phone,
	 purchase_location,
	 current_location,
     cast(purchase_time_frame as bigint) as purchase_time_frame,
	 prequal,
	 {{ local_convert_timezone('created','CETDST') }} as created,
	 {{ local_convert_timezone('updated','CETDST') }} as updated,
	 hash,
	 comments,
	 cast(price_range_lower_bound as bigint) as price_range_lower_bound,
	 cast(price_range_upper_bound as bigint) as price_range_upper_bound,
	 cast(bank_id as {{ uuid_formatter() }}) AS bank_id,
	 purchase_location_detail,
	 selling_address,
	 transaction_type,
	 cast(aggregate_id as {{ uuid_formatter() }}) AS aggregate_id,
	 sell_location,
	 brokerage_contacted,
	 contact_methods,
	 rc_assignable,
	 referral_fee_transaction,
	 consumer_confirmed,
	 rebate_paid,
	 documentation_received,
	 funds_received,
	 agent_submitted
FROM {{source('public', 'raw_import_leads')}}
{% if is_incremental() %}
WHERE {{ local_convert_timezone('updated','CETDST') }} >= coalesce((select max(updated) from {{ this }}), '1900-01-01')
{% endif %}
