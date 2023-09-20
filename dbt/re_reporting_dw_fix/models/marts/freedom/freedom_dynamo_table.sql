{{
  config(
    materialized = 'view'
    ,tags=['mlo_invites']
    )
}}

SELECT * 

FROM {{ ref('dynamodb_invite_table') }}
  