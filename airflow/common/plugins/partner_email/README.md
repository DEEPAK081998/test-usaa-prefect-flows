REP-1307

REP-1152

**Process:**

1. Fetch data from DB (model a SQL query) and write it to a csv file in S3.
2. Presign this S3 object's URL and add it to a table (AWS lambda)
3. Send email with presigned URL on schedule

**Findings:**

- Email attachments are currently not supported by hightouch

+ Git Sync is a bi-directional integration between your Hightouch workspace and a Git repository.

- Presigned URL last at most 12h

**References**

[S3 Source](https://hightouch.com/docs/sources/s3)

[S3 Destination](https://hightouch.com/docs/destinations/s3)

[SMTP Email](https://hightouch.com/docs/destinations/email)

[Git Sync](https://hightouch.com/docs/extensions/git-sync) Code version control

[Sync with Airflow](https://hightouch.com/docs/extensions/airflow) "You can also trigger syncs automatically via Airflow or the Hightouch REST API."

[Presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html)

[HTTP Request](https://hightouch.com/docs/destinations/http-request)

[Airbyte S3 Destination](https://docs.airbyte.com/integrations/destinations/s3/)


Airbyte load csv file in S3 (always the same name, use bucket version control)



Parametrize dag with csv file path, bucket name, schedule (everyday 7AM CT), email to and other fields from yaml config, use json file

Add required packages (check if needed)
-[]Can Airbyte load a .csv file from a table created by dbt?


-[] Confirm Airflow is in CT


**Requirements:**

- Load RBC .csv in S3 with Airbyte fetching data from a dbt table [confirm with Daryl]
- Use S3 versioning control for RBS (closes and enrollments) csvs
- Use S3 Bucket `s3://mdp-prod-bucket` [confirm with Varun]
- Adapt Python code to reference cloud storage (S3) instead of local storage
- Schedule email sending with Airflow
- Use parameterized Airflow dag template (`main_etl_dag.py`)
- Create parameters .json file to feed dag template
- Send emails with Postmark API
- Retrieve Postmark api-key from AWS Secrets Manager or use Airflow variable?