{{
  config(
    materialized = 'view',
	  tags=['sofi']
    )
}}
SELECT
	ga_date AS enrollment_date,
	ga_dimension5 AS user_id,
	ga_pagepath
FROM {{ source('public', 'sofi_user_id') }}
WHERE ga_dimension5 != 'Not Set' AND ga_pagepath = '/enrollment-confirmation'
ORDER BY ga_date DESC