WITH
    lo_cte AS (
        SELECT
            ca.lead_id,
            ca.profile_aggregate_id AS mlo_aggregate_id,
            CONCAT(pup.first_name,' ',pup.last_name) AS inviting_lo,
            pup.first_name AS inviting_lo_firstname,
            pup.last_name AS inviting_lo_lastname,
            {% if target.type == 'snowflake' %}
            pup.phones[0].phoneNumber::varchar AS inviting_lo_phone,
            pup.email AS inviting_lo_email,
            pup.data:nmlsid::VARCHAR AS mlo_nmlsid,
            {% else %}
            pup.phones->0->>'phoneNumber' AS inviting_lo_phone,
            pup.email AS inviting_lo_email,
            pup.data->>'nmlsid' AS mlo_nmlsid,
            {% endif %}
            ROW_NUMBER() over (PARTITION BY ca.lead_id ORDER BY ca.created ASC) AS row_id
        FROM
            {{ ref('current_assignments') }} ca
            LEFT JOIN {{ ref('partner_user_profiles') }} pup
            ON pup.aggregate_id = ca.profile_aggregate_id
        WHERE
            ca.role = 'MLO'
    )
    , inviting_lo_cte AS (
        SELECT
            lsu.lead_id,
            loc.inviting_lo_email,
            loc.inviting_lo,
            loc.inviting_lo_firstname,
            loc.inviting_lo_lastname,
            loc.inviting_lo_phone,
            loc.mlo_aggregate_id AS inviting_aggregate_id,
            loc.mlo_nmlsid AS inviting_nmlsid
        FROM
            {{ ref('lead_status_updates') }} lsu
            LEFT JOIN lo_cte loc
            ON loc.lead_id = lsu.lead_id
        WHERE
            lsu.status LIKE '%Invited%' AND row_id = 1
        ORDER BY
            lead_id ASC
    )
    , pop_data AS (
        {% if target.type == 'snowflake' %}
            SELECT aggregate_id, fp.value:phoneType::VARCHAR as PhoneType, fp.value:phoneNumber::VARCHAR as phoneNumber
            FROM {{ ref('partner_user_profiles') }} pup,
            lateral flatten(input => pup.phones) fp
        {% else %}
            SELECT aggregate_id, json_array_elements(phones)->>'phoneType' as PhoneType, json_array_elements(phones)->>'phoneNumber' as phoneNumber
            FROM {{ ref('partner_user_profiles') }}
        {% endif %}
    )
    , aop_cte AS (
        SELECT aggregate_id, min(PhoneNumber) as OfficePhone
        FROM pop_data pop
        WHERE lower(pop.Phonetype) = 'office'
        GROUP BY aggregate_id
    )
    , amp_cte AS (
        SELECT aggregate_id, min(PhoneNumber) as MobilePhone
        FROM pop_data pop
        WHERE lower(pop.Phonetype) = 'mobilephone'
        GROUP BY aggregate_id
    )
    , mlo_hierarchy_cte AS (
        SELECT
            'Citizens' AS bank_name,
            CONCAT(pup.first_name,' ',pup.last_name) AS mlo_name,
            pup.email AS mlo_email,
            COALESCE(mop.OfficePhone,mmp.MobilePhone) AS mlo_phone,
            {% if target.type == 'snowflake' %}
            pup.data:nmlsid::VARCHAR AS mlo_nmlsid,
            pup.data:apsm.managerName::VARCHAR AS apsm_manager_name,
            pup.data:apsm.email::VARCHAR AS apsm_email,
            pup.data:psm.managerName::VARCHAR AS psm_manager_name,
            pup.data:psm.email::VARCHAR AS psm_email,
            pup.data:asm.name::VARCHAR AS asm_region,
            pup.data:asm.managerName::VARCHAR AS asm_manager_name,
            pup.data:asm.email::VARCHAR AS asm_email,
            pup.data:rsm.name::VARCHAR  AS rsm_region,
            pup.data:rsm.managerName::VARCHAR AS rsm_manager_name,
            pup.data:rsm.email::VARCHAR AS rsm_email,
            pup.data:rsm.managerName::VARCHAR AS dsm_manager_name,
            pup.data:rsm.name::VARCHAR AS dsm_division,
            pup.data:rsm.email::VARCHAR AS dsm_email,
            ROW_NUMBER() over (PARTITION BY mlo_nmlsid ORDER BY pup.updated DESC) AS row_id,
            {% else %}
            pup.data ->> 'nmlsid' AS mlo_nmlsid,
            pup.data -> 'apsm' ->> 'managerName' AS apsm_manager_name,
            pup.data -> 'apsm' ->> 'email' AS apsm_email,
            pup.data -> 'psm' ->> 'managerName' AS psm_manager_name,
            pup.data -> 'psm' ->> 'email' AS psm_email,
            pup.data -> 'asm' ->> 'name' AS asm_region,
            pup.data -> 'asm' ->> 'managerName' AS asm_manager_name,
            pup.data -> 'asm' ->> 'email' AS asm_email,
            pup.data -> 'rsm' ->> 'name' AS rsm_region,
            pup.data -> 'rsm' ->> 'managerName' AS rsm_manager_name,
            pup.data -> 'rsm' ->> 'email' AS rsm_email,
            pup.data -> 'dsm' ->> 'managerName' AS dsm_manager_name,
            pup.data -> 'dsm' -> 'name' AS dsm_division,
            pup.data -> 'dsm' -> 'email' AS dsm_email,
            ROW_NUMBER() over (PARTITION BY pup.data ->> 'nmlsid' ORDER BY pup.updated DESC) AS row_id,
            {% endif %}
            pup.aggregate_id,
            'true' AS enabled,
            pure.role,
            pup.updated
        FROM
            {{ ref('partner_user_profiles') }} pup
            LEFT JOIN {{ ref('partner_user_roles') }} pure
            ON pure.user_profile_id = pup.id
            LEFT OUTER JOIN aop_cte mop
            ON mop.aggregate_id = pup.aggregate_id
            LEFT OUTER JOIN amp_cte mmp
            ON mmp.aggregate_id = pup.aggregate_id
        WHERE
            email LIKE '%citizens%'
            AND pure.role = 'MLO'
    )
    , mlo_final AS (
        SELECT
            *
        FROM
            mlo_hierarchy_cte
        WHERE
            row_id = 1
    )
    , int_cte AS (
        SELECT
            ilc.*,
            ld.*,
            hc.apsm_manager_name AS inviting_aspm_manager_name,
            hc.apsm_email AS inviting_aspm_email,
            hc.psm_manager_name AS inviting_psm_manager_name,
            hc.psm_email AS inviting_psm_email,
            hc.asm_region AS inviting_asm_region,
            hc.asm_manager_name AS inviting_asm_manager_name,
            hc.asm_email AS inviting_asm_email,
            hc.rsm_region AS inviting_rsm_region,
            hc.rsm_manager_name AS inviting_rsm_manager_name,
            hc.rsm_email AS inviting_rsm_email,
            hc.dsm_manager_name AS inviting_dsm_manager_name,
            hc.dsm_email AS inviting_dsm_email,
            hc.dsm_division AS inviting_dsm_division,
            mhc.apsm_manager_name AS enrollment_aspm_manager_name,
            mhc.apsm_email AS enrollment_aspm_email,
            mhc.psm_manager_name AS enrollment_psm_manager_name,
            mhc.psm_email AS enrollment_psm_email,
            mhc.asm_region AS enrollment_asm_region,
            mhc.asm_manager_name AS enrollment_asm_manager_name,
            mhc.asm_email AS enrollment_asm_email,
            mhc.rsm_region AS enrollment_rsm_region,
            mhc.rsm_manager_name AS enrollment_rsm_manager_name,
            mhc.rsm_email AS enrollment_rsm_email,
            mhc.dsm_manager_name AS enrollment_dsm_manager_name,
            mhc.dsm_email AS enrollment_dsm_email,
            mhc.dsm_division AS enrollment_dsm_division
        FROM
            inviting_lo_cte ilc 
            FULL JOIN {{ ref('leads_data_v3') }} ld
            ON ld.id = ilc.lead_id
            LEFT JOIN mlo_final mhc
            ON mhc.mlo_nmlsid = ld.nmlsid
            LEFT JOIN mlo_final hc
            ON hc.mlo_nmlsid = ilc.inviting_nmlsid
        WHERE
            ld.bank_name = 'Citizens'
        ORDER BY
            ilc.lead_id ASC
    )
    , final_cte AS (
        SELECT
            'Citizens' AS bank_name,
            system_invite_date AS event_date,
            inviting_nmlsid AS mlo_nmlsid,
            source,
            mlosubmission,
            COUNT(system_invite_date) AS invite_count,
            0 AS enroll_count,
            0 AS close_date,
            0 AS warm_transfer_count
        FROM
            int_cte
        WHERE
            EXTRACT(YEAR FROM system_invite_date ) <> '1900'
        GROUP BY
            system_invite_date,
            mlo_nmlsid,
            source,
            mlosubmission

        UNION
        SELECT
            bank_name,
            system_enroll_date AS event_date,
            nmlsid,
            source,
            mlosubmission,
            0 AS invite_count,
            COUNT(
                CASE
                    WHEN system_enroll_date IS NOT NULL THEN id
                END
            ) AS enroll_count,
            0 AS close_date,
            0 AS warm_transfer_count
        FROM
            {{ ref('leads_data_v3') }}
        WHERE
            bank_name = 'Citizens'
        GROUP BY
            bank_name,
            system_enroll_date,
            nmlsid,
            source,
            mlosubmission
        UNION
        SELECT
            partner AS bank_name,
            DATE(DATE) AS event_date,
            nmlsid,
            '' AS utmcampaign,
            '1' AS mlo_submission,
            0 AS invite_count,
            0 AS enroll_count,
            0 AS close_date,
            COUNT(client_name) AS warm_transfer_count
        FROM
            {{ ref('warm_transferlog') }}
        WHERE
            partner = 'Citizens'
        GROUP BY
            DATE,
            partner,
            nmlsid
)
SELECT
    *
FROM
    final_cte
ORDER BY
    event_date DESC
