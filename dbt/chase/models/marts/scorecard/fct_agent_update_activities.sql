{{ config(
    materialized = 'table'
) }}

WITH update_counts AS (
    SELECT
        ce.id AS lead_id,
        t2.bank_id,
        ca.profile_aggregate_id AS agent_aggregate_id,
        ca.created,
        CASE
            WHEN majorstatus = 'Inactive' THEN inactivedate
            WHEN majorstatus = 'Closed' THEN normalizedclosedate
            ELSE CURRENT_DATE
        END AS timer_stop_date,
        CASE
            WHEN COALESCE(lsuc.update_count,0) >= (EXTRACT(days FROM
                        (
                            CASE
                                WHEN majorstatus = 'Inactive' THEN inactivedate
                                WHEN majorstatus = 'Closed' THEN normalizedclosedate
                                WHEN hb_status = 'On Hold On Hold' THEN realestatestatustime
                                ELSE CURRENT_DATE
                            END - ca.created
                        )
                )/14 :: INT
            ) THEN 1
            ELSE 0
        END AS referral_update_compliance,
        EXTRACT(
            days
            FROM
                (
                    CASE
                        WHEN majorstatus = 'Inactive' THEN inactivedate
                        WHEN majorstatus = 'Closed' THEN normalizedclosedate
                        WHEN hb_status = 'On Hold On Hold' THEN realestatestatustime
                        ELSE CURRENT_DATE
                    END - ca.created
                )
        ) / 14 :: INT AS referral_update_weeks,
        COALESCE(
            lsuc.update_count,
            0
        )
    FROM
        {{ ref("chase_enrollments_test") }} ce
        JOIN {{ ref('current_assignments') }} ca
        ON ce.id = ca.lead_id
        LEFT OUTER JOIN (
            SELECT
                lead_id,
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
                OR status LIKE ('On Hold'))
            GROUP BY
                lead_id) lsuc
                ON ce.id = lsuc.lead_id
        left join {{ ref('leads') }} t2 
         ON ce.id = t2.id
            WHERE
                ca.role = 'AGENT'
        )
SELECT
     agent_aggregate_id,
     bank_id,
     SUM(referral_update_compliance) AS total_referrals_in_compliance,
     SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 30 THEN referral_update_compliance END ) as last30days_total_referrals_in_compliance,
     SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 90 THEN referral_update_compliance END ) as last90days_total_referrals_in_compliance,
     SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 180 THEN referral_update_compliance END ) as last180days_total_referrals_in_compliance,
     SUM(CASE WHEN EXTRACT(DAYS FROM (CURRENT_DATE - uc.created))::INT <= 365 THEN referral_update_compliance END ) as last365days_total_referrals_in_compliance
FROM
        update_counts uc
GROUP BY
        agent_aggregate_id,bank_id


