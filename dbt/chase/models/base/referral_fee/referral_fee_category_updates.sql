{{ config(
	materialized='incremental',
	unique_key='id',
	indexes=[
	  {'columns': ['created'], 'type': 'btree'},
	  {'columns': ['updated'], 'type': 'btree'},
      {'columns': ['referral_fee_category_id'],'type': 'btree'}
	],
	post_hook=[
		after_commit("{{ add_primary_key(this,'id','referral_fee_category_pkey') }}")
	]
) }}
SELECT
    cast(body as json) as body,
    created,
    id,
    cast(referral_fee_category_id AS UUID) as referral_fee_category_id,
    updated
FROM 
    {{ source('public', 'raw_referral_fee_category_updates') }}