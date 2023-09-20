with cte AS (
{% if target.type == 'snowflake' %}
    select ld.lead_id,
    flat_ld_mrm.value:type::VARCHAR as referral_fee_type,
    (flat_ld_cd.value:created::VARCHAR)::bigint as Updatedate
    from {{ ref('leads_data') }} ld,
	lateral flatten(input => ld.data:closingProcess.moneyReceivedMulti) flat_ld_mrm,
	lateral flatten(input => ld.data:closingProcess.closingDetails) flat_ld_cd
{% else %}
    select ld.lead_id,
    json_array_elements(ld.data->'closingProcess'->'moneyReceivedMulti')->>'type' as referral_fee_type,
    (json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'created')::bigint as Updatedate
    from {{ ref('leads_data') }} ld
{% endif %}
)
,
final AS (
select *,
row_number()Over (partition by lead_id order by updatedate desc) as row_id
 from cte where referral_fee_type is not null 
order by lead_id desc 
)
select lead_id,
referral_fee_type 
from final where row_id = 1