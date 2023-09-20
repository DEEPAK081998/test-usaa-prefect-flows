{{ config(
    materialized = 'table'
) }}

WITH update_counts AS (
    SELECT
        ce.id AS lead_id,
        ce.bank_id,
        ca.profile_aggregate_id AS agent_aggregate_id,
        ca.created,

        {% if target.type == 'snowflake' %}
        CASE
            WHEN major_status = 'Inactive' THEN TO_DATE(inactivedate)
            WHEN major_status = 'Closed' THEN
                (CASE WHEN normalizedclosedate IS NOT NULL THEN TO_DATE(normalizedclosedate) ELSE TO_DATE(lsuc.created_closed_status) END)
            WHEN hb_status = 'On Hold On Hold' THEN TO_DATE(HB_Status_Time)
            ELSE CURRENT_DATE()
        END AS timer_stop_date,
        
        {% else %}
        CASE
            WHEN major_status = 'Inactive' THEN inactivedate
            WHEN major_status = 'Closed' THEN 
                (case when normalizedclosedate is not null then normalizedclosedate else lsuc.created_closed_status end)
            WHEN hb_status = 'On Hold On Hold' THEN HB_Status_Time
            ELSE CURRENT_DATE
        END AS timer_stop_date,
        
        {% endif %}
 
        COALESCE(lsuc.update_count,0) as number_of_updates
    FROM
        {{ ref("leads_data_v3") }} ce
        JOIN {{ ref('current_assignments') }} ca
        ON ce.id = ca.lead_id
        LEFT OUTER JOIN (
            SELECT
                lead_id,
                MAX(CASE WHEN status LIKE ('Closed Closed') THEN created END) as created_closed_status,
                COUNT(*) AS update_count
            FROM
                {{ ref('lead_status_updates') }} lsu
            WHERE
                category LIKE 'Property%'
                AND LOWER(status) NOT LIKE ('%assigned%')
                AND (status LIKE ('Inactive%')
                OR status LIKE ('Active%')
                OR status LIKE ('Closed Closed')
                OR status LIKE ('Pending%')
                OR status LIKE ('On Hold%'))
            GROUP BY
                lead_id) lsuc
                ON ce.id = lsuc.lead_id
            WHERE
                ca.role = 'AGENT'
),
update_compliance as (
    SELECT
        lead_id,
        bank_id,
        agent_aggregate_id,
        created,

        {% if target.type == 'snowflake' %}

        CASE WHEN number_of_updates >= (DATEDIFF('day',created,timer_stop_date) / 14 )
            THEN 1
            ELSE 0
        END AS referral_update_compliance,

        DATEDIFF('day',created,timer_stop_date) / 14 AS referral_update_weeks
        
        {% else %}

		CASE WHEN number_of_updates >= (EXTRACT(days FROM timer_stop_date - created))/14 :: INT THEN 1 ELSE 0 END AS referral_update_compliance,

		EXTRACT(days FROM timer_stop_date - created)/14 :: INT AS referral_update_weeks

        {% endif %}

	FROM update_counts		
),
referral_compliance as (
    SELECT
        agent_aggregate_id,
        bank_id,

        COUNT(lead_id) as total_referrals,
        SUM(referral_update_compliance) AS total_referrals_in_compliance,
        
        {% if target.type == 'snowflake' %}
        COUNT(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 30 THEN lead_id END ) as last30days_total_referrals,
        SUM(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 30 THEN referral_update_compliance END ) as last30days_total_referrals_in_compliance,
        COUNT(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 90 THEN lead_id END ) as last90days_total_referrals,
        SUM(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 90 THEN referral_update_compliance END ) as last90days_total_referrals_in_compliance,
        COUNT(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 180 THEN lead_id END ) as last180days_total_referrals,
        SUM(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 180 THEN referral_update_compliance END ) as last180days_total_referrals_in_compliance,
        COUNT(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 365 THEN lead_id END ) as last365days_total_referrals,
        SUM(CASE WHEN DATEDIFF('day', uc.created, CURRENT_DATE()) <= 365 THEN referral_update_compliance END ) as last365days_total_referrals_in_compliance
        
        {% else %}
	    COUNT(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 30 THEN lead_id END ) as last30days_total_referrals,
	    SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 30 THEN referral_update_compliance END ) as last30days_total_referrals_in_compliance,
	    COUNT(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 90 THEN lead_id END ) as last90days_total_referrals,
	    SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 90 THEN referral_update_compliance END ) as last90days_total_referrals_in_compliance,
	    COUNT(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 180 THEN lead_id END ) as last180days_total_referrals,
	    SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 180 THEN referral_update_compliance END ) as last180days_total_referrals_in_compliance,
	    COUNT(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 365 THEN lead_id END ) as last365days_total_referrals,
	    SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 365 THEN referral_update_compliance END ) as last365days_total_referrals_in_compliance
        {% endif %}
    
    FROM
            update_compliance uc
    GROUP BY
            agent_aggregate_id,bank_id
)
SELECT
	agent_aggregate_id,
	bank_id,
	coalesce(total_referrals,0) as total_referrals,
	coalesce(total_referrals_in_compliance,0) as total_referrals_in_compliance,
	case when coalesce(total_referrals,0) = 0 then 0
		else coalesce((total_referrals_in_compliance::decimal/total_referrals),0) 
	end as percentual_referrals_in_compliance,
     
	coalesce(last30days_total_referrals,0) as last30days_total_referrals,
	coalesce(last30days_total_referrals_in_compliance,0) as last30days_total_referrals_in_compliance,
	case when coalesce(last30days_total_referrals,0) = 0 then 0
		else coalesce((last30days_total_referrals_in_compliance::decimal/last30days_total_referrals),0) 
	end as last30days_percentual_referrals_in_compliance,

	coalesce(last90days_total_referrals,0) as last90days_total_referrals,
	coalesce(last90days_total_referrals_in_compliance,0) as last90days_total_referrals_in_compliance,
	case when coalesce(last90days_total_referrals,0) = 0 then 0
		else coalesce((last90days_total_referrals_in_compliance::decimal/last90days_total_referrals),0) 
	end as last90days_percentual_referrals_in_compliance,

	coalesce(last180days_total_referrals,0) as last180days_total_referrals,
	coalesce(last180days_total_referrals_in_compliance,0) as last180days_total_referrals_in_compliance,
	case when coalesce(last180days_total_referrals,0) = 0 then 0
		else coalesce((last180days_total_referrals_in_compliance::decimal/last180days_total_referrals),0) 
	end as last180days_percentual_referrals_in_compliance,

	coalesce(last365days_total_referrals,0) as last365days_total_referrals,
	coalesce(last365days_total_referrals_in_compliance,0) as last365days_total_referrals_in_compliance,
	case when coalesce(last365days_total_referrals,0) = 0 then 0
		else coalesce((last365days_total_referrals_in_compliance::decimal/last365days_total_referrals),0) 
	end as last365days_percentual_referrals_in_compliance
	
FROM
	referral_compliance
