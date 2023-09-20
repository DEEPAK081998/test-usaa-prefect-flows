{{
  config(
	materialized = 'table'
	)
}}
SELECT 
    *, 
    data->'cognitoUserName' as cognito_user_name 
FROM 
    {{ ref('partner_user_profiles') }} 