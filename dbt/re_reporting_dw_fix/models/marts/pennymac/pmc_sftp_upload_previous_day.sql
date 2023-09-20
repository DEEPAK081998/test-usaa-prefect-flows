{{
  config(
    materialized = 'table',
  	tags=['pmc_sftp']
    )
}}

WITH

final_cte AS (
    SELECT
      pu."HomeStory ID"	,
      pu.created_datetime_cst	,
      pu."First Name"	,
      pu."Last Name"	,
      pu."Property City"	,
      pu."Property State"	,
      pu."Email"	,
      pu."Cell Phone"	,
      pu."Sales Group"	,
      pu."Marketing Source Code"	,
      pu."Received from HomeStory"	,
      pu."Campaign Name"	,
      pu."Estimated Payment Savings"	,
      pu."Property Address"	,
      pu."Property Zip Code"	,
      pu."Dialing Zip Code"	,
      pu."Property County"	,
      pu."Mailing Address"	,
      pu."Mailing City"	,
      pu."Mailing State"	,
      pu."Mailing Zip Code"	,
      pu."Current UPB"	,
      pu."Current Rate (%)"	,
      pu."Marketing Rate"	,
      pu."MI Amount"	,
      pu."Home Phone"	,
      pu."1st P&I"	,
      pu."Escrowed T&I"	,
      pu."First Name (Co-Borrower)"	,
      pu."Last Name (Co-Borrower)"	,
      pu."Age of Loan"	"Due Date"	,
      pu."Escrow Balance"	,
      pu."Original Loan Amount"	,
      pu."Note Date"	,
      pu."Maturity Date"	,
      pu."MLS Listing Price"	,
      pu."MLS Listing Date"	,
      pu."Bedroom Count"	,
      pu."Bathroom Count"	,
      pu."Year Built (Home Listed)"	,
      pu."Square Footage"	,
      pu."MLS Agent Name",
      pu."Lead Source",
      pu."Status"
    FROM {{ ref('pmc_upload') }} pu
    WHERE {{ filter_previous_day('created_datetime_cst', 'US/Central') }}
)
SELECT * FROM final_cte