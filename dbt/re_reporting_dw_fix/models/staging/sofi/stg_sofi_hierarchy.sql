SELECT
	bank_name,
	level_4_manager_name AS executive_name,
	level_2_manager AS manager_name,
	mlo_name AS producer_name,
	lower(level_4_manager_email) AS executive_email,
	level_2_manager_email AS manager_email,
	mlo_email AS producer_email,
	{{ extract_email_prefix('level_4_manager_email') }} AS executive_prefix,
	{{ extract_email_prefix('level_2_manager_email') }} AS manager_prefix,
	{{ extract_email_prefix('mlo_email') }} AS producer_prefix
FROM
	{{ ref('all_partner_hierarchy') }}
WHERE
	bank_name ILIKE 'sofi'
ORDER BY
	bank_name,
	level_4_manager_name,
	level_2_manager,
	mlo_name