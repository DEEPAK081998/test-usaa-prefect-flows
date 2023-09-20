# README #

This flow will run a task to trigger the airbyte connection

## Usage:

1. Register flow with command
   ```bash
   python scripts/prefect_build/build.py [--aws-profile <profile>] --labels <labels> --s3-bucket <S3 bucket name> --flows flows/test_project/airbyte_connection/flow.py
   ```
   **Note:** aws-profile is an optional argument.
2. The flow require 3 parameters configure them as need in prefect cloud
    1. connection-id: Connection id for particular airbyte connection it is the last part of url for particular
       connection in airbyte
       ex `http://localhost:8000/workspaces/d36c35c5-d72a-4691-ad1d-8b38aebae2b2/connections/032488b3-6818-4aa5-b357-f51fb7595a1c`
       where `032488b3-6818-4aa5-b357-f51fb7595a1c` is the connection-id
    2. host: The private ip address of airbyte ec2 instance
    3. port: The port on which airbyte is running default is `8000`
