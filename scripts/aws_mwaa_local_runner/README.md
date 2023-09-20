# About aws-mwaa-local-runner

This scripts provides a command line interface (CLI) utility that replicates an Amazon Managed Workflows for
Apache Airflow (MWAA) environment locally.

## About the CLI

The CLI builds a Docker container image locally that’s similar to a MWAA production image. This allows you to run a
local Apache Airflow environment to develop and test DAGs, custom plugins, and dependencies before deploying to MWAA.

## Prerequisites

- **macOS**: [Install Docker Desktop](https://docs.docker.com/desktop/).
- **Linux/Ubuntu**: [Install Docker Compose](https://docs.docker.com/compose/install/)
  and [Install Docker Engine](https://docs.docker.com/engine/install/).
- **Windows**: Windows Subsystem for Linux (WSL) to run the bash based command `mwaa-local-env`. Please
  follow [Windows Subsystem for Linux Installation (WSL)](https://docs.docker.com/docker-for-windows/wsl/)
  and [Using Docker in WSL 2](https://code.visualstudio.com/blogs/2020/03/02/docker-in-wsl2), to get started.

## Get started

### Step one: Creating env file

Run the following command from the repo root to create a new env file in `scripts/aws_mwaa_local_runner` directory:

```bash
echo "SOURCE_CODE_ROOT='$(pwd)'" >> scripts/aws_mwaa_local_runner/dev.env
```

After the file is created

* Copy the contents of [.env.default](.env.default) located in same directory to this new file.(You'll be able to access dynamic, standalone and common dags)
  
  **Notes:** 
  * Modify the value of key `AIRFLOW_PROJECT` to specify the project you want to use
  * Modify the value of key `LOCAL_DAG_DIR_PATH` to specify the location where you want to store the dag files locally.

### Step two: Building the Docker image

Build the Docker container image using the following command:

```bash
./mwaa-local-env build-image
```

**Note**: it takes several minutes to build the Docker image locally.

### Step three: Running Apache Airflow

#### Local runner

Runs a local Apache Airflow environment that is a close representation of MWAA by configuration.

```bash
./mwaa-local-env start
```

To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are
stopped.

### Step four: Accessing the Airflow UI

By default, the `bootstrap.sh` script creates a username and password for your local Airflow environment.

- Username: `admin`
- Password: `test`

#### Airflow UI

- Open the Apache Airlfow UI: <http://localhost:8080/>.

### Step five: Add DAGs and supporting files

#### DAGs

Add DAG code/parameter as needed to the folder specified by env `LOCAL_AIRFLOW_DAGS_PATH`/`AIRFLOW_PROJECT` accordingly.
on more info on how to add dags follow this [README](../../airflow/README.md)

#### Requirements.txt

1. Add Python dependencies to [airflow_requirements.txt](../../dags/airflow_requirements.txt) for standalone dags.
2. Or add Python dependencies to [common_requirements.txt](../../airflow/common/requirements/requirements.txt) for
   dynamic dags.
3. To test a requirements.txt without running Apache Airflow, use the following script:

```bash
./mwaa-local-env test-requirements
```

Let's say you add `aws-batch==0.6` to your `airflow_requirements.txt` file. You should see an output similar to:

```bash
Installing requirements.txt
Collecting aws-batch (from -r /usr/local/airflow/dags/requirements.txt (line 1))
  Downloading https://files.pythonhosted.org/packages/5d/11/3aedc6e150d2df6f3d422d7107ac9eba5b50261cf57ab813bb00d8299a34/aws_batch-0.6.tar.gz
Collecting awscli (from aws-batch->-r /usr/local/airflow/dags/requirements.txt (line 1))
  Downloading https://files.pythonhosted.org/packages/07/4a/d054884c2ef4eb3c237e1f4007d3ece5c46e286e4258288f0116724af009/awscli-1.19.21-py2.py3-none-any.whl (3.6MB)
    100% |████████████████████████████████| 3.6MB 365kB/s 
...
...
...
Installing collected packages: botocore, docutils, pyasn1, rsa, awscli, aws-batch
  Running setup.py install for aws-batch ... done
Successfully installed aws-batch-0.6 awscli-1.19.21 botocore-1.20.21 docutils-0.15.2 pyasn1-0.4.8 rsa-4.7.2
```

#### Custom plugins

Add your custom plugins in [plugins](../../dags/plugins) folder for standalone dags or
in [common_plugins](../../airflow/common/plugins) folder for dynamic dags

### Additional Configurations

#### Changing your local runner port

By default, airflow run on port 8080 on local but, you can change that by adding below env
var in your [dev.env](scripts/aws_mwaa_local_runner/dev.env) file

```dotenv
AIRFLOW_LOCAL_PORT=<port_no>
```

#### Using AWS creds with local runner

To use your AWS creds with local runner add below environment variable in
your [dev.env](scripts/aws_mwaa_local_runner/dev.env)

```dotenv
AWS_CREDS_FOLDER_PATH=<creds_folder_path>
# where <creds_folder_path> is the absolute path to your .aws folder 
```

if your aws creds file have named profiles then add below environment variable in
your [.env.localrunner](scripts/aws_mwaa_local_runner/docker/config/.env.localrunner) to specify profile to use

```dotenv
AWS_PROFILE=<profile_name>
```

**Note:** if you want to assume mwaa role on your local then instead of adding your main profile name in `AWS_PROFILE`
create new config by adding the below-mentioned text in your aws config file located at `${HOME}/.aws/config` and then
add the profile name created below in `AWS_PROFILE`

```text
[profile <profile_name>]
role_arn = <arn of mwaa execution role>
source_profile = <source profile name>
region = <region where mwaa is deployed>
```

#### Accessing remote airbyte server from local dags

To access remote server from local, create a connection on local airflow UI with following configurations.

```
connection_type=HTTP
host=<airbyte_remote_host>
extra={"cookie":"<cookie_value>"}
```

To get the cookie_value mentioned above, follow the steps mentioned below,

1. Login to the airbyte server from the chrome browser manually.
2. Run the following command
   ```bash
   python scripts/get_airbyte_cookie.py --domain <Domain name>
   ```
   This script will print the cookie name and value (if any) for various profiles you may have in the browser.
   You can use any set of cookies to access the airbyte server.

After connection is created from airflow UI, use that connectionId in AirbyteOperator to access the remote
airbyte server.

***(Not Recommended)*** If above methods doesn't work then to use your AWS creds with local runner add below three
environment variables in
your [.env.localrunner](scripts/aws_mwaa_local_runner/docker/config/.env.localrunner)

```dotenv
AWS_ACCESS_KEY_ID=<aws_access_key>
AWS_SECRET_ACCESS_KEY=<aws_secret>
AWS_SESSION_TOKEN=<aws_session_token> # optional 
```

#### Updating dynamic dags without restarting Airflow server

if you want to update your dags without restarting the airflow server run the following command from repo root

```shell
export $(xargs < scripts/aws_mwaa_local_runner/dev.env)
python "$SOURCE_CODE_ROOT"/scripts/airflow_build --local-dag-dir ${LOCAL_DAG_DIR_PATH} --update-dynamic-dags --dags-directory "$SOURCE_CODE_ROOT/$LOCAL_AIRFLOW_DAGS_PATH" --update-common-dags --update-standalone-dags --projects "$AIRFLOW_PROJECT" --requirements-file "$SOURCE_CODE_ROOT/$LOCAL_AIRFLOW_REQUIREMENTS_PATH" --plugins-directory "$SOURCE_CODE_ROOT/$LOCAL_AIRFLOW_PLUGINS_PATH" --test
```

**remember** this will only update _dags_ and **not** _plugins_ or _requirement_, to update those you need to restart
the server

#### Using local dbt project

if you want to use your local dbt project make sure your [dev.env](scripts/aws_mwaa_local_runner/dev.env) have this
below environment variable

```dotenv
LOCAL_DBT_PROJECT_DIR='dbt'
```

and then provide the below path to `project_dir` field in your DBT operator to specify your dbt project

```shell
/usr/local/dbt/<project_name>
# where <project_name> is the dbt project name like chase, dbt-test, re_reporting_dw_fix
```

#### Troubleshooting

follow this [guide](https://github.com/aws/aws-mwaa-local-runner#troubleshooting) for errors you may encounter when
using the local runner.