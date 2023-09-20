{{
  config(
	materialized = 'view',
	)
}}

WITH
leads_cte AS(
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
)

, normalize_dates AS(
	SELECT
		lc.lead_id,
		max(lc.update_date) as max_date,
		case
		    {% if target.type == 'snowflake' %}
		    when RLIKE(lc.closedate::text, '^[0-9]{13}$', 'i') then to_timestamp(lc.closedate::bigint/1000)
		    {% else %}
			when lc.closedate::text ~* '^[0-9]{13}$' then to_timestamp(lc.closedate::bigint/1000)
			{% endif %}
			when lc.closedate = '0' then null
			when lc.closedate = '' then null
			{% if target.type == 'snowflake' %}
			else to_timestamp(REGEXP_SUBSTR(lc.closedate,'\\d+[-_]\\d+[-_]\\d+$'), 'yyyy-MM-dd')
			{% else %}
			else to_timestamp(substring(lc.closedate,'\d+[-_]\d+[-_]\d+$'), 'yyyy-MM-dd')
			{% endif %}
		end as closedate,
		lender
	FROM
		leads_cte lc
    group by lc.lead_id, lc.closedate,lc.lender
)

, final AS (
	SELECT
		distinct(lead_id),
		max_date,
		lender AS lenderclosedwith,
		closedate
	FROM normalize_dates
	GROUP BY lead_id, closedate, lender,max_date
)

, lead_status_updates_cte AS (
	SELECT
		lsu.lead_id,
		(min(lsu.created)) as datemarkedclosed
	FROM {{ ref('lead_status_updates') }} lsu
	WHERE category in ('PropertySell','PropertySearch') and status like 'Closed%'
    GROUP BY lsu.lead_id
)

,final_f AS (
	SELECT *,
	ROW_NUMBER() over (PARTITION BY	lead_id order by max_date DESC) as row_id
	FROM final
)

SELECT
	ff.lead_id,
	ff.max_date,
	ff.lenderclosedwith,
	ff.closedate,
    {{ calculate_time_interval('dmc.datemarkedclosed', '-', '5', 'hour') }} as date_marked_closed_final,
	ff.row_id
FROM final_f ff
LEFT OUTER JOIN lead_status_updates_cte dmc on dmc.lead_id = ff.lead_id
WHERE ff.row_id = 1
