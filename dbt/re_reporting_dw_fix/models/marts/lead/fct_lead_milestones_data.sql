{{
  config(
    materialized = 'table'
    )
}}
WITH assigment_order_cte AS (
    SELECT
        ca.*
    FROM {{ ref('leads_data_v3') }} l 
    join {{ ref('current_assignments') }}  ca
    on l.id = ca.lead_id
)
, ao_history_cte AS (
    SELECT
        ua.lead_id,
        ua.profile_aggregate_id,
        ROW_NUMBER() over(
            PARTITION BY ua.lead_id
            ORDER BY
                ua.created ASC
        ) AS assigment_order
    FROM {{ ref('user_assignments') }} ua 
    WHERE ua.role='AGENT' and ua.profile_aggregate_id <>'00000000-0000-0000-0000-000000000000'
)
, current_assigment_cte AS (
    SELECT
        lead_id,
        MAX(assigment_order) max_assigment_order
    FROM
        ao_history_cte
    GROUP BY 1
)
, ao_history_most_recent_cte AS (
    SELECT
        aohc.*
    FROM
        ao_history_cte aohc 
    JOIN current_assigment_cte cacte on aohc.lead_id = cacte.lead_id and  aohc.assigment_order = cacte.max_assigment_order

)
, lead_certified_cte AS (
    SELECT
        DISTINCT lead_id,
        TRUE AS lead_certified
    FROM
        {{ ref('lead_status_updates') }}
        lsu
    WHERE
        lsu.category LIKE 'Property%'
        AND status NOT LIKE 'Rejected%'
        AND status NOT LIKE 'Routing%'
        AND status NOT LIKE 'New%'
        AND status NOT LIKE 'Closed%'
        AND status NOT IN (
            'Active Agent Assigned',
            'Inactive Out of Service Area'
        )
),
days_to_status_cte AS (
    SELECT
        lead_id,
        MIN(
            CASE
                WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
            END
        ) AS beginning_date,
        {% if target.type == 'snowflake' %}
         DATEDIFF(
            'day',
            MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            ),
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status NOT LIKE 'Rejected%'
                    AND status NOT LIKE 'Routing%'
                    AND status NOT LIKE 'New%'
                    AND status NOT IN (
                        'Active Agent Assigned',
                        'Inactive Out of Service Area'
                    )
                    AND status NOT LIKE 'Closed%' THEN created
                END
            )
        ) AS days_to_certified,
        DATEDIFF(
            'day',
            MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            ),
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Active%'
                    AND status NOT IN ('Active Agent Assigned') THEN created
                END
            )
        ) AS days_to_active,
        DATEDIFF(
            'day',
            MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            ),
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Inactive%'
                    AND status NOT IN ('Inactive Out of Service Area') THEN created
                END
            )
        ) AS days_to_inactive,
        DATEDIFF(
            'day',
            MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            ),
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Pending%' THEN created
                END
            )
        ) AS days_to_pending,
        DATEDIFF(
            'day',
            MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            ),
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Closed%' THEN created
                END
            )
        ) AS days_to_closed,
        DATEDIFF(
            'day',
            MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            ),
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status = 'Active Actively Searching' THEN created
                END
            )
        ) AS days_to_actively_searching,
        {% else %}
        DATE_PART(
            'day',
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status NOT LIKE 'Rejected%'
                    AND status NOT LIKE 'Routing%'
                    AND status NOT LIKE 'New%'
                    AND status NOT LIKE 'Unassigned%'
                    AND status NOT IN (
                        'Active Agent Assigned',
                        'Inactive Out of Service Area',
                        'Active Attempting to Contact'
                    )
                    AND status NOT LIKE 'Closed%' THEN created
                END
            ) - MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            )
        ) AS days_to_certified,
        DATE_PART(
            'day',
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Active%'
                    AND status NOT IN ('Active Agent Assigned') THEN created
                END
            ) - MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            )
        ) AS days_to_active,
        DATE_PART(
            'day',
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Inactive%'
                    AND status NOT IN ('Inactive Out of Service Area') THEN created
                END
            ) - MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            )
        ) AS days_to_inactive,
        DATE_PART(
            'day',
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Pending%' THEN created
                END
            ) - MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            )
        ) AS days_to_pending,
        DATE_PART(
            'day',
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status ILIKE 'Closed%' THEN created
                END
            ) - MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            )
        ) AS days_to_closed,
        DATE_PART(
            'day',
            MIN(
                CASE
                    WHEN lsu.category LIKE 'Property%'
                    AND status = 'Active Actively Searching' THEN created
                END
            ) - MIN(
                CASE
                    WHEN lsu.status ILIKE 'Active Agent Assigned' THEN created
                END
            )
        ) AS days_to_actively_searching,
        {% endif %}
        COUNT(
            CASE
                WHEN lsu.category LIKE 'Property%'
                AND lsu.status = 'Active Agent Assigned' THEN 1
            END
        ) AS assigment_count,
        MAX(
            CASE
                WHEN lsu.category LIKE 'Property%'
                AND lsu.status = 'Active Agent Assigned' THEN created
            END
        ) AS last_reasign_date
    FROM
        {{ ref('lead_status_updates') }}
        lsu
    GROUP BY
        1
),
raw_first_reassign_date_cte AS (
    SELECT
        lead_id,
        created,
        ROW_NUMBER() over (
            PARTITION BY lead_id
            ORDER BY
                created ASC
        ) AS row_num
    FROM
        {{ ref('lead_status_updates') }}
    WHERE
        category LIKE 'Property%'
        AND status = 'Active Agent Assigned'
),
first_reassign_date_cte AS (
    SELECT
        lead_id,
        created AS first_reassign_date
    FROM
        raw_first_reassign_date_cte
    WHERE
        row_num = 2
),
raw_reactive_cte AS (
    SELECT
        lead_id,
        created,CASE
            WHEN status ILIKE 'inactive%' THEN FALSE
            ELSE TRUE
        END AS status_active,CASE
            WHEN category IN (
                'PropertySell',
                'PropertySearch'
            ) THEN status
        END AS hb_status
    FROM
        {{ ref('lead_status_updates') }}
    WHERE
        status ILIKE 'inactive%'
        OR status ILIKE 'active%'
    ORDER BY
        lead_id,
        created
),
int_reactive_cte AS (
    SELECT
        *,
        LAG(
            status_active,
            1
        ) over (
            PARTITION BY lead_id
            ORDER BY
                created
        ) AS prev_status,
        LAG(
            hb_status,
            1
        ) over (
            PARTITION BY lead_id
            ORDER BY
                created
        ) AS prev_hb_status
    FROM
        raw_reactive_cte
),
int_cte_reactivate AS (
    SELECT
        *,
        CASE
            WHEN status_active = TRUE
            AND COALESCE(
                prev_status,
                TRUE
            ) = FALSE THEN TRUE
            ELSE FALSE
        END AS reactive,
        CASE
            WHEN status_active = TRUE
            AND COALESCE(
                prev_status,
                TRUE
            ) = FALSE THEN created
            ELSE NULL
        END reactive_date,
        CASE
            WHEN status_active = TRUE
            AND COALESCE(
                prev_status,
                TRUE
            ) = FALSE THEN prev_hb_status
            ELSE NULL
        END reactivation_reason
    FROM
        int_reactive_cte
),
final_reactivate AS (
    SELECT
        lead_id,
        MAX(reactive::INT)::BOOLEAN AS reactivate,
        MAX(reactive_date) AS reactivate_date,
        COUNT(
            DISTINCT reactive
        ) AS reactivate_count,
        MAX(reactivation_reason) AS reactivation_reason
    FROM
        int_cte_reactivate
    GROUP BY
        1
),
leads_info AS (
    SELECT
        id AS lead_id,
        reporting_enroll_date,
        agent_aggregate_id
    FROM
        {{ ref('leads_data_v3') }}
),
zendesk_info AS (
    SELECT
        lead_id::bigint as lead_id,
        COUNT(*) AS count_of_zendesk_tickets
    FROM
        {{ ref('fct_cust_com_event') }}
        fcce
    WHERE
        lead_id IS NOT NULL
        AND event_source = 'Zendesk'
        AND lead_id <> ''
        AND lead_id <> '125089125089125089125089125089125089'
    GROUP BY  1
),
source_cio_sms_sent_cte AS (
{% if target.type == 'snowflake' %}
  SELECT
    cpad.data:delivery_id::VARCHAR as delivery_id_data,
    cpad.data:agent.leadId::VARCHAR as lead_id,
    CONVERT_TIMEZONE('UTC', to_timestamp(cpad.timestamp)) as system_date,
    CONVERT_TIMEZONE('America/Chicago', to_timestamp(cpad.timestamp)) as reporting_date, * from {{ source('public', 'cio_prod_activities') }} cpad
  WHERE cpad.delivery_type ='webhook' and (cpad.name is null or cpad.name <> 'outreach')
{% else %}
  SELECT
    cpad.data->>'delivery_id' as delivery_id_data,
    COALESCE(cpad.data->'agent'->>'leadId',l.id::varchar) as lead_id,
    to_timestamp(cpad.timestamp) at time zone 'UTC' as system_date,
    to_timestamp(cpad.timestamp) at time zone 'CST' as reporting_date, cpad.*  
  FROM {{ source('public', 'cio_prod_activities') }} cpad
  LEFT JOIN {{ ref("leads_data_v3") }} l
  on cpad.customer_identifiers->>'email' = l.agent_email
  WHERE cpad.delivery_type ='webhook' and (cpad.name is null or cpad.name <> 'outreach')
{% endif %}
),
raw_sms_counts AS (
  SELECT *,MIN(system_date) over(
            PARTITION BY lead_id
        ) AS min_date  
  FROM source_cio_sms_sent_cte cte 
  JOIN {{ source('public', 'cio_prod_messages') }} ciopm 
  ON ciopm.id =  cte.delivery_id_data
  WHERE ciopm.campaign_id = 21
),
int_sms_counts AS (
    SELECT
        lead_id::bigint lead_int,
        {% if target.type == 'snowflake' %}
        CASE
            WHEN DATEDIFF(
                'hour',
                min_date,
                system_date
            ) <= 24
            AND DATEDIFF(
                'hour',
                min_date,
                system_date
            ) >= 0 THEN 24
            WHEN DATEDIFF(
                'hour',
                min_date,
                system_date
            ) <= 48
            AND DATEDIFF(
                'hour',
                min_date,
                system_date
            ) > 24 THEN 48
            WHEN DATEDIFF(
                'hour',
                min_date,
                system_date
            ) >= 192 THEN 192
        END hour_diff,*
        {% else %}
        CASE
            WHEN DATE_PART(
                'hour',
                system_date - min_date
            ) <= 24
            AND DATE_PART(
                'hour',
                system_date - min_date
            ) >= 0 THEN 24
            WHEN DATE_PART(
                'hour',
                system_date - min_date
            ) <= 48
            AND DATE_PART(
                'hour',
                system_date - min_date
            ) > 24 THEN 48
            WHEN DATE_PART(
                'hour',
                system_date - min_date
            ) >= 192 THEN 192
        END hour_diff,*
        {% endif %}
    FROM
        raw_sms_counts
),
final_sms_counts AS (
    SELECT
        lead_int AS lead_id,
        COUNT(
            CASE
                WHEN hour_diff = 24 and campaign_id = 21 THEN 1
            END
        ) AS "24_verified_connection_sms",
        COUNT(
            CASE
                WHEN hour_diff = 48 and campaign_id = 21 THEN 1
            END
        ) AS "48_verified_connection_sms",
        COUNT(
            CASE
                WHEN hour_diff = 192 and campaign_id = 18 THEN 1
            END
        ) AS "8day_text_sent"
    FROM
        int_sms_counts
    GROUP BY
        1
),
raw_sms_response_counts AS (
    SELECT
        *,
        MIN(system_date) over(
            PARTITION BY lead_id
        ) AS min_date
    FROM
        {{ ref('base_cio_campaign_sms_response') }}
    WHERE
        lead_id IS NOT NULL
        AND campaign_id in (21)
),
int_sms_response_counts AS (
    SELECT
        lead_id::bigint lead_int,
        {% if target.type == 'snowflake' %}
        CASE
            WHEN DATEDIFF(
                'hour',
                min_date,
                system_date
            ) <= 24
            AND DATEDIFF(
                'hour',
                min_date,
                system_date
            ) >= 0 THEN 24
            WHEN DATEDIFF(
                'hour',
                min_date,
                system_date
            ) <= 48
            AND DATEDIFF(
                'hour',
                min_date,
                system_date
            ) > 24 THEN 48
        END hour_diff,*
        {% else %}
        CASE
            WHEN DATE_PART(
                'hour',
                system_date - min_date
            ) <= 24
            AND DATE_PART(
                'hour',
                system_date - min_date
            ) >= 0 THEN 24
            WHEN DATE_PART(
                'hour',
                system_date - min_date
            ) <= 48
            AND DATE_PART(
                'hour',
                system_date - min_date
            ) > 24 THEN 48
        END hour_diff,*
        {% endif %}
    FROM
        raw_sms_response_counts
),
final_sms_response_counts AS (
    SELECT
        lead_int AS lead_id,
        COUNT(
            CASE
                WHEN hour_diff = 24 THEN 1
            END
        ) AS "post_24hrs_agent_reponse",
        COUNT(
            CASE
                WHEN hour_diff = 48 THEN 1
            END
        ) AS "post_48hrs_agent_reponse"
    FROM
        int_sms_response_counts
    GROUP BY
        1
),
final_cte AS (
    SELECT
        aoc.*,
        cte.reporting_enroll_date,
        cte.agent_aggregate_id,
        aohmrc.assigment_order,
        CASE
            WHEN aoc.profile_aggregate_id = aohmrc.profile_aggregate_id THEN TRUE
            ELSE FALSE
        END current_assigment,
        lcc.lead_certified,
        dts.days_to_certified,
        dts.days_to_active,
        dts.days_to_inactive,
        dts.days_to_pending,
        dts.days_to_closed,
        dts.days_to_actively_searching,
        frc.reactivate,
        CASE
            WHEN frc.reactivate = FALSE THEN NULL
            ELSE frc.reactivate_date
        END AS reactivate_date,
        CASE
            WHEN frc.reactivate = FALSE THEN NULL
            ELSE frc.reactivate_count
        END AS reactivate_count,
        CASE
            WHEN frc.reactivate = FALSE THEN NULL
            ELSE frc.reactivation_reason
        END AS reactivation_reason,
        dts.assigment_count,
        frdc.first_reassign_date,
        dts.last_reasign_date,
        zf.count_of_zendesk_tickets,
        fsms."24_verified_connection_sms",
        fsms."48_verified_connection_sms",
        fsms."8day_text_sent",
        fsmsr."post_24hrs_agent_reponse",
        fsmsr."post_48hrs_agent_reponse"
    FROM
        assigment_order_cte aoc
        LEFT JOIN lead_certified_cte lcc
        ON aoc.lead_id = lcc.lead_id
        LEFT JOIN days_to_status_cte dts
        ON aoc.lead_id = dts.lead_id
        LEFT JOIN final_reactivate frc
        ON aoc.lead_id = frc.lead_id
        LEFT JOIN first_reassign_date_cte frdc
        ON aoc.lead_id = frdc.lead_id
        LEFT JOIN ao_history_most_recent_cte aohmrc
        ON aoc.lead_id = aohmrc.lead_id
        LEFT JOIN current_assigment_cte cat
        ON aoc.lead_id = cat.lead_id
        LEFT JOIN leads_info cte
        ON aoc.lead_id = cte.lead_id
        LEFT JOIN zendesk_info zf
        ON aoc.lead_id = zf.lead_id
        LEFT JOIN final_sms_counts fsms
        ON aoc.lead_id = fsms.lead_id
        LEFT JOIN final_sms_response_counts fsmsr
        ON aoc.lead_id = fsmsr.lead_id
)
SELECT
    DISTINCT
    *
FROM
    final_cte
WHERE ROLE= 'AGENT'