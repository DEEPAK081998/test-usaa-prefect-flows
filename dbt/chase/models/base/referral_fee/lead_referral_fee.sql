WITH cte_qualified_programs (lead_id, associated_date, referral_fee_category_id, source_name) AS (
                (
                    SELECT  l.id                                         																				                                                          AS lead_id,
                            COALESCE((SELECT MAX(created) from {{ ref('current_assignments') }} where lead_id = l.id and role = 'AGENT' LIMIT 1), NOW())                                                                          AS associated_date,
                            COALESCE((SELECT referral_fee_category_id FROM {{ ref('referral_fee_categories') }} WHERE partner_id = l.bank_id ORDER BY created DESC LIMIT 1), '00000000-0000-0000-0000-000000000000'::uuid)     AS referral_fee_category_id,
                            COALESCE((SELECT name FROM {{ ref('referral_fee_categories') }} WHERE partner_id = l.bank_id ORDER BY created DESC LIMIT 1), 'default')                               				              AS source_name,
                            0   																														                                                          AS score
                    FROM   {{ ref('leads') }} l
                )
                UNION
                (
                    SELECT lead_id                                 AS lead_id,
                            created                                AS associated_date,
                            (data->>'referralFeeCategoryId')::uuid AS referral_fee_category_id,
                            'lead_override'                        AS source_name,
                            1									   AS score
                    FROM   {{ ref('current_lead_statuses') }}
                    WHERE  category='ReferralFee'
                    AND    status='Referral Fee Update'
                )
                UNION
                (
                    SELECT DISTINCT ON(ao.lead_id) * FROM (
                                    SELECT ca.lead_id                     AS lead_id,
                                            rfca.created                  AS associated_date,
                                            rfca.referral_fee_category_id AS referral_fee_category_id,
                                            'agent_override'              AS source_name,
                                            0							  AS score
                                    FROM   {{ ref('current_assignments') }} ca
                                    JOIN {{ ref('referral_fee_category_assignments') }} rfca
                                    ON ca.profile_aggregate_id = rfca.assignee_aggregate_id
                                    WHERE    ca.role='AGENT'
                                    AND    rfca.created <= (
                                        SELECT MAX(created) FROM {{ ref('current_assignments') }}
                                        WHERE profile_aggregate_id = rfca.assignee_aggregate_id
                                        AND lead_id = ca.lead_id
                                        AND role = 'AGENT'
                                    )
                    ) AS ao
                    ORDER BY ao.lead_id, ao.associated_date DESC
                )
                UNION
                (
                    SELECT DISTINCT ON (ao.lead_id) * FROM (
                                    SELECT ca.lead_id                     AS lead_id,
                                            rfca.created                  AS associated_date,
                                            rfca.referral_fee_category_id AS referral_fee_category_id,
                                            'brokerage_override'          AS source_name,
                                            0							  AS score
                                    FROM   {{ ref('current_assignments') }} ca
                                    JOIN referral_fee_category_assignments rfca
                                    ON ca.profile_aggregate_id = rfca.assignee_aggregate_id
                                    WHERE    ca.role='BROKERAGE'
                                    AND    rfca.created <= (
                                        SELECT MAX(CREATED) FROM {{ ref('current_assignments') }}
                                        WHERE profile_aggregate_id = ca.profile_aggregate_id
                                        AND role = 'BROKERAGE'
                                        AND lead_id = ca.lead_id
                                    )
                    ) AS ao
                    ORDER BY ao.lead_id, ao.associated_date DESC
                )
            )

            SELECT DISTINCT ON (by_source_name.lead_id) by_source_name.lead_id, by_source_name.percentage, by_source_name.source_name, by_source_name.referral_fee_category_id FROM
                (
                SELECT DISTINCT ON(cqp.lead_id, cqp.source_name) cqp.score, cqp.lead_id, cqp.source_name, rfcr.effective_date, rfcr.referral_fee_category_id, rfcr.percentage
                FROM cte_qualified_programs cqp
                JOIN (
                SELECT * FROM {{ ref('referral_fee_categories_rates') }} rfcr
                WHERE rfcr.effective_date <= NOW()
                ) AS rfcr
                ON cqp.referral_fee_category_id = rfcr.referral_fee_category_id
                WHERE cqp.associated_date > rfcr.effective_date
                ORDER BY cqp.lead_id, cqp.source_name, rfcr.effective_date DESC
                ) by_source_name
            ORDER BY by_source_name.lead_id, by_source_name.score DESC, by_source_name.percentage ASC