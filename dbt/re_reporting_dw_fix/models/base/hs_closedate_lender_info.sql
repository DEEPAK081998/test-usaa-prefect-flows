WITH leads_cte AS(
{% if target.type == 'snowflake' %}
    SELECT
		DISTINCT(ld.lead_id),
        flat_ld.value:created::VARCHAR::bigint as update_date,
		flat_ld.value:lender::VARCHAR as lender,
		flat_ld.value:closeDate::VARCHAR as closedate
	FROM
		{{ ref('leads_data') }} ld,
		lateral flatten(input => ld.data:closingProcess.closingDetails) flat_ld
{% else %}
	SELECT
		DISTINCT(ld.lead_id),
        (json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'created')::bigint as update_date,
		json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'lender' as lender,
		json_array_elements(ld.data->'closingProcess'->'closingDetails')->>'closeDate' as closedate
	FROM
		{{ ref('leads_data') }} ld
{% endif %}
	),
normalize_dates AS(
	SELECT
		lc.lead_id,
		max(lc.update_date) as max_date,
		{% if target.type == 'snowflake' %}
		case when RLIKE(lc.closedate::text, '^[0-9]{13}$', 'i') then
						 to_timestamp(lc.closedate::bigint/1000)
						 when lc.closedate = '0' then null
						 when lc.closedate = '' then null
					else
						 to_timestamp(REGEXP_SUBSTR(lc.closedate,'\\d+[-_]\\d+[-_]\\d+$'), 'yyyy-MM-dd') end as closedate,
		{% else %}
		case when lc.closedate::text ~* '^[0-9]{13}$' then
						 to_timestamp(lc.closedate::bigint/1000)
						 when lc.closedate = '0' then null
						 when lc.closedate = '' then null
					else
						 to_timestamp(substring(lc.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd') end as closedate,
	    {% endif %}
		lender
	FROM
		leads_cte lc
    group by lc.lead_id, lc.closedate,lc.lender
    
)
,
final AS (
SELECT
	distinct(lead_id),
    max_date,
	lender AS lenderclosedwith,
	closedate
FROM normalize_dates
GROUP BY lead_id, closedate, lender,max_date
)
,
final_f AS (
select *, 
ROW_NUMBER() over (PARTITION BY 
lead_id order by max_date DESC) as row_id
from final 
)
select 
	ff.lead_id,
	ff.max_date,
	ff.lenderclosedwith,
	ff.closedate,
    (dmc.datemarkedclosed - interval '5 hours') as date_marked_closed_final,
	ff.row_id
from final_f ff
LEFT OUTER JOIN (
    SELECT
		lsu.lead_id,
		(min(lsu.created)) as datemarkedclosed
	FROM {{ ref('lead_status_updates') }} lsu
	WHERE category in ('PropertySell','PropertySearch')
    and status like 'Closed%'
    group by lsu.lead_id) dmc on dmc.lead_id = ff.lead_id
    
    
    where ff.row_id = 1 
