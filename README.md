# README #

This Repo is for managed data pipeline resources

## Local Setup

1. **Python Installation:**
    * **Option 1:**
        1. Update local repositories:
           ```bash
           sudo apt update
           ```
        2. Install the supporting software:
           ```bash
           sudo apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev
           ```
        3. Download Gzipped source tarball of python version 3.10.9
           from [here](https://www.python.org/downloads/release/python-3109/)
        4. Extract the file using tar:
           ```bash
           tar -xf Python-3.10.9.tgz
           ```
        5. Test System and Optimize Python:
           ```bash
           cd Python-3.10.9
           ./configure --enable-optimizations
           ```
           Note: This step can take up to 30 minutes to complete.
        6. Install the Python binaries by running the following command:
           ```bash
           sudo make altinstall
           ```
        5. Check python version:
           ```bash
           python3.10 --version
           ```
    * **Option 2:**
        1. Update local repositories:
            ```bash
            sudo apt update
            ```
        2. Install the supporting software:
           ```bash
           sudo apt install software-properties-common
           ```
        3. Add Deadsnakes PPA:
           ```bash
           sudo add-apt-repository ppa:deadsnakes/ppa
           
           sudo apt update
           ```
        4. Install Python:
           ```bash
           sudo apt install python3.10
           ```
           Note: This step can take up to 30 minutes to complete.
        5. Check python version:
           ```bash
           python3.10 --version
           ```

2. **Environment Setup:**  
   Use below set of commands to create virtual environment or use any other preferred method
    1. `sudo pip install virtualenv`
    2. `sudo pip install virtualenvwrapper`
    3. `export WORKON_HOME=~/Envs`
    4. `mkdir -p $WORKON_HOME`
    5. `source /usr/local/bin/virtualenvwrapper.sh`
    6. `mkvirtualenv -p python3.10 env1`
    7. `workon env1`
    8. `deactivate`

    * **Note:** Put line 3 and 5 in ~/.bashrc

3. **Project Setup:**
    1. Install requirements.txt :`pip install -r requirements.txt`

## Airbyte Connection

### Installation

[Octavia CLI](https://github.com/airbytehq/airbyte/tree/v0.40.4/octavia-cli) is a needed to manage Airbyte
configurations in YAML. please follow below command to install octavia on your local

```shell
curl -s -o- https://raw.githubusercontent.com/airbytehq/airbyte/v0.40.4/octavia-cli/install.sh | bash
```

### Directory structure:

* All airbyte connections are stored inside the airbyte directory and the directory structure it follows is

```text
.
├── readme.md
├── ...
├── ...
├── airbyte
│   ├── projects
│   │   ├── project_1
│   │   │   ├── connection_config
│   │   │   │   ├── api_http_headers.yaml
│   │   │   │   ├── sources
│   │   │   │   │   ├──source_1
│   │   │   │   │   │   └── configuration.yaml  
│   │   │   │   │   └── ...  
│   │   │   │   ├── destinations
│   │   │   │   │   ├──destination_1
│   │   │   │   │   │   └── configuration.yaml  
│   │   │   │   │   └── ...
│   │   │   │   └── connections
│   │   │   │       ├──connection_1
│   │   │   │       │   └── configuration.yaml  
│   │   │   │       └── ...
│   │   │   └── ... 
│   │   ├── project_2
│   │   │   └── ... 
│   │   └── ... 
│   └── ... 
└── ... 
```

### Project Setup

Follow below steps to set up a project

1. Create a project directory by using the following set if commands from repo root
    ```shell
    mkdir -p airbyte/projects/<project_name>/connection_config
   # where <project_name> is name of your project
    ```
2. To initialize a project Airbyte server is needed you can set up Airbyte on your local by following
   this [guide](https://airbyte.gitbook.io/airbyte/deploying-airbyte/local-deployment), or you can use sandbox or prod
   server, for using sandbox or prod server follow below steps
    1. copy [api_http_headers.yaml](scripts/octavia/api_http_headers.yaml) file by using the following command
       ```shell
       cp scripts/octavia/api_http_headers.yaml airbyte/projects/<project_name>/connection_config/local_api_http_headers.yaml
       # where <project_name> is name of your project
       ``` 
    2. on your browser login to the airbyte UI on sandbox/prod
    3. after successful login run the following command to get the cookie
       ```shell
       python scripts/get_airbyte_cookie.py --domain <domain_name>
       # where <domain_name> is the airbyte server domain for ex: airbyte-sandbox.re-sandbox.homestoryrewards.com or airbyte-prod.re-prod.homestoryrewards.com
       ```
    4. copy the cookie and paste the cookie in your *local_api_http_headers.yaml* file under key `Cookie`
3. Initialize your project by running the following command
   ```shell 
   1. cd airbyte/projects/<project_name>/connection_config
   # if using local airbyte server
   2. octavia init
   # if using sandbox/prod server
   2. octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> init
      # where <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
   ```

### Importing a connection

Follow below steps to import connection from airbyte server

1. change your directory to the *connection_config* folder of the project in which you want to create connection
   ex `cd airbyte/projects/project_1/connection_config`
2. To import connection you need to make sure that both source and destinations are already been tracked by octavia if
   not then
   ```shell
   # Use below command to import source 
   1. octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> import source <source_id>
   # Use below command to import destination 
   2. octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> import destination <destination_id>
   # Use below command to import connetion 
   3. octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> import connection <connection_id>
   
   # where <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
   # Note: if you are using local server then --airbyte-url and --api-http-headers-file-path params are not needed
   ```
3. update configuration files of all imported source/destination to replace secrets with placeholders(
   see [Using Placeholders for secrets](#using-placeholders-for-secrets) on how to use placeholders)
4. run the following command to apply your changes and upload your state files to s3
   ```shell
   python scripts/airbyte_build --octavia-deploy  --env-file-path ~/.octavia --airbyte-api-url <url> --zip-file-path s3://mdp-<environment>-bucket/airbyte/connectors/state/connector_states.zip --projects <project_name> --api-header-file <header_file_name>
   # where
   # <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
   # <environment> is aws enviroment like sandbox/prod
   # <project_name> is name of airbyte project
   # <header_file_name> is the name of header file which holds airbyte server cookie
   ```

### Creating new connection:

Follow below steps to create new connection

1. change your directory to the *connection_config* folder of the project in which you want to create connection
   ex `cd airbyte/projects/project_1/connection_config`
2. You can follow this [guide](https://airbyte.com/tutorials/version-control-airbyte-configurations) to create
   connections but in crux it is as follows
    1. ***creating source***:
        1. for creating source we need its connector id run the following command to find its id(in the given command we
           are finding the id of postgres)
           ```shell
           octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> list connectors sources | grep postgres
           # where <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
           # Note: if you are using local server then --airbyte-url and --api-http-headers-file-path params are not needed
           ```
        2. now use the id to create source
           ```shell
           octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> generate source <connector_id> <source_name>
           # where 
           # <connector_id> is the id fetched above
           # <source_name> is the name you want to give to source connection
           # <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
           # Note: if you are using local server then --airbyte-url and --api-http-headers-file-path params are not needed
           ```
        3. update configuration files to use placeholders for secrets(
           see [Using Placeholders for secrets](#using-placeholders-for-secrets) on how to use placeholders)
    2. ***creating destination***:
        1. for creating destination we need its connector id run the following command to find its id(in the given
           command we are finding the id of postgres)
           ```shell
           octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> list connectors destinations | grep postgres
           # where <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
           # Note: if you are using local server then --airbyte-url and --api-http-headers-file-path params are not needed
           ```
        2. now use the id to create source
           ```shell
           octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> generate destination <connector_id> <destination_name>
           # where 
           # <connector_id> is the id fetched above
           # <destination_name> is the name you want to give to destination connection
           # <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
           # Note: if you are using local server then --airbyte-url and --api-http-headers-file-path params are not needed
           ```
        3. update configuration files to use placeholders for secrets(
           see [Using Placeholders for secrets](#using-placeholders-for-secrets) on how to use placeholders)
    3. ***creating connection***:
        1. for creating connection we need source an destination configuration file, run the following command to create
           connection
           ```shell
           octavia --api-http-headers-file-path local_api_http_headers.yaml --airbyte-url <url> generate connection --source <source_path> --destination <destination_path> <connection_name>
           # where
           # <source_path> is the path to the configuration file of source connection created above
           # <destination_path> is the path to the configuration file of destination connection created above
           # <connection_name> is the name give to this connection
           # <url> is the server url like https://airbyte-sandbox.re-sandbox.homestoryrewards.com
           # Note: if you are using local server then --airbyte-url and --api-http-headers-file-path params are not needed
           ```

### Using Placeholders for secrets

Octavia relies on you to replace secrets with placeholders this solves 2 issues

1. Helps in not exposing user creds
2. And helps in re-usability of creds

To use placeholders do following:

1. Replace hardcoded creds with `${placeholder_name}`
2. Add `<placeholder_name>` in octavia config file(usually located in `~/.octavia`) Ex:  
   sample _config.yaml_ file
   ```yaml
     configuration:
      password: ${SECRET_POSTGRES_PASSWORD}
    ```
   sample _.octavia_ file
   ```dotenv
    SECRET_POSTGRES_PASSWORD=test
    ```
3. Also add this placeholder in same format mentioned in octavia file in your Airbyte secret manager(It
   is `AirbyteConnectionBuildSecret` resource in `mdp-secrets-<environment>-stack)

### Registering connection

* To register all connections use the below command. This will register all connections
  ```shell
  octavia apply
  ```
* To register a set of connections use the below command
  ```shell
  octavia apply -f [<connection_path1> <connection_path_2> ...] 
  ```
* For more help you can follow this [README](https://github.com/airbytehq/airbyte/blob/master/octavia-cli/README.md) or
  use
  ```shell
  octavia --help
  ```

### Access airbyte server from local

If you want to make calls to airbyte server from local, you need to add session cookies to HTTP request.

Please follow the steps below to generate session cookies,

1. Before running the file, login to the airbyte server from the chrome browser manually.
2. Run the following command
   ```bash
   python scripts/get_airbyte_cookie.py --domain <Domain name>
   ```
   This script will print the cookie name and value (if any) for various profiles you may have in the browser.
   You can use any set of cookies to make calls to the airbyte server.
3. If you want to get the session cookies info for a particular profile, run the following command
   ```bash
   python scripts/get_airbyte_cookie.py --cookie-file <cookie_file_path>
   ```
   where `cookie_file_path` is the absolute path where chrome has stored the cookies on local.
4. If you want to print cookies in a format that can be directly used in postman or while using curl,
   run the command mentioned above directly, otherwise you can use a pretty format by adding `--pretty-print` flag. Eg,
   ```bash
   python scripts/get_airbyte_cookie.py --domain <Domain name> --pretty-print
   ```
4. Add all cookies generated by the file above in the HTTP request to access the airbyte server.  
   If using pretty format, add the cookies as mentioned below,
    1. Postman: Add cookies in the header with key as 'cookie' and value '<cookie1_name>=<cookie1_value>; <
       cookie2_name>=<cookie2_value>'
    2. curl: Add a flag `--cookie` with value '<cookie1_name>=<cookie1_value>; <cookie2_name>=<cookie2_value>' to the
       curl command

   otherwise use the printed cookie string directly as shown below
    1. Postman: Add cookies in the header with key as 'cookie' and value '<printed_cookie_string>'
    2. curl: Add a flag `--cookie` with value '<printed_cookie_string>' to the curl command

## Airflow Dag

### Directory structure(MWAA):

* All airflow dags are stored inside the dags directory and the directory structure it follows is

```text
.
├── README.md
├── ...
├── ...
├── airflow
│   ├── common
│   │   ├── airflow_variables
│   │   │   ├── prod_variables.json
│   │   │   ├── sandbox_variables.json
│   │   │   └── ...
│   │   ├── dags
│   │   │   ├── dag_1.py
│   │   │   ├── dag_2.py
│   │   │   └── ...
│   │   ├── plugins
│   │   │   ├── plugin_1.py
│   │   │   ├── plugin_2
│   │   │   │   ├── plugin_2_a.py
│   │   │   │   └── ...
│   │   │   └── ...
│   │   ├── requirements
│   │   |   ├── requirements_version_1.txt
│   │   |   ├── requirements_version_2.txt
│   │   |   └── ...
│   │   └── ...   
│   ├── config
│   │   ├── dockerfile_config
│   │   └── ...
│   ├── dags_template
|   |   ├── generic_dag_1.py
|   |   ├── generic_dag_2.py
│   │   └── ...
│   ├── Dockerfile
│   ├── ecs_tasks
│   │   |   ├── task_config_1.json
│   │   |   ├── Dockerfile
│   │   |   └── script
│   │   |     └── entrypoint.sh
│   │   └── ...
│   ├── projects
│   │   ├── project_1
│   │   │   ├── parameters
│   │   │   │   ├── generic_dag_1
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
│   │   │   │   ├── generic_dag_2
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   ├── dags
│   │   │   │   ├── generic_dag_1.py
│   │   │   │   ├── generic_dag_2.py
│   │   │   │   └── ...
│   │   │   ├── plugins
│   │   │   │   ├── plugin_1.py
│   │   │   │   ├── plugin_2
│   │   │   │   │   ├── plugin_2_a.py
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   ├── requirements
│   │   │   │   ├── requirements.txt
│   │   │   │   └── ...
│   │   │   └── ...
│   │   ├── project_2
│   │   │   ├── parameters
│   │   │   │   ├── generic_dag_1
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
│   │   │   │   ├── generic_dag_2
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   ├── dags
│   │   │   │   ├── generic_dag_1.py
│   │   │   │   ├── generic_dag_2.py
│   │   │   │   └── ...
│   │   │   ├── plugins
│   │   │   │   ├── plugin_1.py
│   │   │   │   ├── plugin_2
│   │   │   │   │   ├── plugin_2_a.py
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   ├── requirements
│   │   │   │   ├── requirements.txt
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── ...
│   └── ...
└── ...
```

### Directory structure(ECS):

* All airflow dags are stored inside the dags directory and the directory structure it follows is

```text
.
├── readme.md
├── ...
├── ...
├── airflow
│   │
│   ├── config
│   │   ├── config.txt
│   │   └── ...
│   ├── Dockerfile
│   ├── projects
│   │   ├── project_1
│   │   │   ├── dags
│   │   │   │   ├── dag_1.py
│   │   │   │   ├── dag_2.py
│   │   │   │   └── ...
│   │   │   ├── plugins
│   │   │   │   ├── plugin_1
│   │   │   │   │   └── ...
│   │   │   │   ├── plugin_1
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   └── requirements.txt
│   │   ├── project_2
│   │   │   ├── dags
│   │   │   │   ├── dag_1.py
│   │   │   │   ├── dag_2.py
│   │   │   │   └── ...
│   │   │   ├── plugins
│   │   │   │   ├── plugin_1
│   │   │   │   │   └── ...
│   │   │   │   ├── plugin_1
│   │   │   │   │   └── ...
│   │   │   │   └── ...
│   │   │   └── requirements.txt
│   │   └── ...
│   └── ...
└── ...
```

### Creating new dags:

To create new dags for please refer to this [README](airflow/README.md)

### Testing dags(MWAA):

To test dags for MWAA environment please refer to this [README](scripts/aws_mwaa_local_runner/README.md)

### Testing dags(ECS):

To test dags for ECS environment please refer to this [README](scripts/airflow_ecs_local_runner/README.md)

### Registering dags:

For registering dags, upload dags in the S3 bucket that is configured in the MWAA resource.

* To upload all dags use the below command.
  ```shell
  python scripts/airflow_build [--aws-profile <aws profile>] --s3-bucket "<S3_BUCKET>"
  ```
* To upload a set of dags, use the below command
  ```shell
  python scripts/airflow_build [--aws-profile <aws profile>] --s3-bucket "<S3_BUCKET>" --dags [<dag_path1> <dag_path_2> ...]
  ```
* To upload a set of projects, use the below command
  ```shell
  python scripts/airflow_build [--aws-profile <aws profile>] --s3-bucket "<S3_BUCKET>" --projects [<project_1> <project_2> ...]
  ```
* For more help on how to use the script run the following command
  ```shell
  python scripts/airflow_build --help
  ```

## Great Expectations

### Creating a great expectations project

In order to create a new great expectations project, install great expectations on local by running the following command

```bash
pip install great-expectations
```

After installing great expectations on local, the next step is to initialize a great expectations project.
Great expectations projects will be kept under `great_expectations/`.

#### Initialize a great expectations project
Follow the steps mentioned below to initialize a great expectations project

```bash
cd great_expectations
```
```bash
great_expectations init
```
It will create the folder structure to organize your great expectations project.

#### Configuring a data source

If you already have a great expectations project or if you just created one following the steps mentioned above,
the next step is to configure a data source.  
Follow the steps mentioned below to configure a data source

```bash
great_expectations datasource new
```

This will open up a jupyter notebook on your local. Configure the connection details, among other things as mentioned.  
This will add a data source config in the great_expectations.yml present in your great_expectations project's root.  
For more details, please follow https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview

#### Creating an expectation suite

After you configure a data source, next step will be creating an expectation suite to add expectations.
Follow the steps below to create an expectation suite.

```bash
great_expectations suite new
```

This will prompt you to select the way you would like to create an expectation suite. Choose the appropriate option.  
For example, if you choose profiler to create a suite, a jupyter notebook will be launched.  
Follow the steps mentioned in the notebook.  
It will create an expectation suite in the expectations directory inside great expectations project.  
For more details, please follow https://docs.greatexpectations.io/docs/guides/expectations/create_expectations_overview

#### Creating a checkpoint config

After creating an expectation suite, the next step will be to create a checkpoint.  
The Checkpoint we create will run the Expectation Suite we previously configured against the data we provide.  
Follow the steps mentioned below for creating a checkpoint.

```bash
great_expectations checkpoint new `<checkpoint_name>`
```

This will open up a jupyter notebook where you can configure the checkpoint.  
After you create the checkpoint, it will be stored inside the checkpoints directory present inside the
great expectation project.  
For more details, please follow https://docs.greatexpectations.io/docs/tutorials/getting_started/tutorial_validate_data

**Note**: A sample test project `test_project` has been added in the `great_expectations` directory for reference.
