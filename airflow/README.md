# README

This readme will help setting up airflow on local and also writing basic dags

## Airflow Dag

### Directory structure:

* All airflow dags are divided in two parts first a common _**dag_template**_ that is stored inside [dags_template](dags_template) directory and its corresponding **_parameter file_** that is stored inside parameters folder in respective [projects](projects) and the directory structure it follows is

```text
.
├── README.md
├── ...
├── ...
├── airflow
│   ├── DOCKERFILE
│   ├── airflow_variables
│   │   ├── prod_variables.json
│   │   ├── sandbox_variables.json
│   │   └── ...
│   ├── config
│   │   ├── dockerfile_config
│   │   └── ...
│   ├── common
│   │   ├── plugins
│   │   │   ├── common_plugin_1.py
│   │   │   ├── common_plugin_2
│   │   │   │   ├── common_plugin_2_a.py
│   │   │   │   └── ...
│   │   │   └── ...
│   │   └── requirements
│   │       ├── requirements.txt
│   │       └── ...
│   ├── dags_template
│   │   ├── generic_dag_1.py
│   │   ├── generic_dag_2.py
│   │   └── ...
│   ├── projects
│   │   ├── project_1
│   │   │   ├── params
│   │   │   │   ├── generic_dag_1
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
│   │   │   │   ├── generic_dag_2
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
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
│   │   │   ├── params
│   │   │   │   ├── generic_dag_1
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
│   │   │   │   ├── generic_dag_2
│   │   │   │   │   ├── param_1.json
│   │   │   │   │   ├── param_2.json
│   │   │   │   │   └── ...
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

### Creating new dag template:

* New dags template can be created under [dags](dags_template) directory
* The template should be general(not specific to any project) and scalable
* If your dag has any package/library that has not been included before, add it in the requirements.txt file or plugins located inside common folder or projects folder as required 
* add a sample parameter file inside this directory `projects/test_project/parameters/<dag_template_name>/sample.json` with all the required and optional parameters that are needed and add supporting documentation in this README if needed

### Creating a new dag with a parameter file:

* New parameters file should be created in this path `projects/<project_name>/parameters/<dag_template_name>/<parameter_file_name>`
* The file contains should contain all the params that are needed for dag to run
* a sample parameter file can be obtained from this path `projects/test_project/parameters/<dag_template_name>/sample.json` and can be filled as needed



### Creating a new dag with an entry in dynamo db table:

* New entry should be created or a pre-existing entry must be updated in 'AirflowParamsDynamoDB' named dynamo-db resource in the 'airflow-application-stack' of the 'prefect-cloudformation' repository.
(`prefect-cloudformation/cloudformation/airflow/airflow-application-stack.yml`)
* The entry must contain a 'dag_name' and the 'template_file' which will be used for dag creation. A sample template file can be found under the name of [main_etl_dag.py](./dags_template/main_etl_dag.py).  Along with that the entry must also contain all the other required parameters.
* At the time of drafting, template_file should be airflow/dags_template/main_etl_dag.py
* A reference for creating parameters can be obtained from this path `projects/test_project/parameters/<dag_template_name>/sample.json` and can be filled as needed

### Testing dags:

To test dags please refer to this [README](../scripts/airflow_ecs_local_runner/README.md)

**Note:** If you want to override your parameter on server without updating file you can do any of the two methods
mentioned below

1. you need to set variable `<dag_name>_extra_params` on server with value as the provided in parameter file or
2. you can you dynamodb for setting parameters by
    1. adding a variable `dynamodb_config_table_name` as _key_ and dynamo db table name presented in the outputs of
       _**airflow stack**_ under `AirflowParamsDynamoTableName` which holds the config as _value_
    2. in the table itself to add value add the dag name in primary key and add all the config that need to be override

# Creating Connection object to airbyte on airflow

Follow the steps below to create a connection object using CLI:

```bash
airflow connections add '<connection_name>' --conn-type 'http' --conn-host '<airbyte_host>' --conn-port '<airbyte_port>'
```

Using airflow UI:

1. Under `Admin` menu, click on `Connections` option.
2. Click on `+` button.
3. Add appropriate connection details and save the connection.

# Creating Connection object to dbt on airflow

Follow the steps below to create a connection object using CLI:

```bash
airflow connections add <DB_NAME> --conn-uri  postgresql://<DB_USER>:<DB_PASS>@<DB_HOST>:<DB_PORT>/<DB_SCHEMA>
```

**Note:**: As shown in the command above, `DB_NAME` should be used as the connection name. So, if creating a
connection from UI, use `DB_NAME` as the name of the connection.

Using airflow UI:

1. Under `Admin` menu, click on `Connections` option.
2. Click on `+` button.
3. Add appropriate connection details and save the connection.

# Using Great expectations in airflow dags

For using great expectations in airflow dags, make sure you have great expectations installed on your
local. It comes with a CLI utility to create expectations, checkpoints etc.  
For installing great expectations and other dependencies on local, run the following command

```bash
pip install -r requirements.txt
```

After installing great expectations on local, the next step is to initialize a great expectations project.  
Great expectations projects will be kept under airflow/projects/{project_name}/plugins.  

Follow the steps mentioned below to initialize a great expectations project

```bash
cd airflow/projects/{project_name}/plugins
```
```bash
great_expectations init
```
It will create the folder structure to organize your great expectations project.

## Configuring a data source

If you already have a great expectations project or if you just created one following the steps mentioned above,
the next step is to configure a data source.  
Follow the steps mentioned below to configure a data source

```bash
great_expectations datasource new
```

This will open up a jupyter notebook on your local. Configure the connection details, among other things as mentioned.  
This will add a data source config in the great_expectations.yml present in your great_expectations project's root.  
For more details, please follow https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview

## Creating an expectation suite

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

## Creating a checkpoint config

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

## Using great expectations operator in airflow dag

After you create all the above resources, you can use the great expectations operator in your dag.  
The operator's documentation can be found at https://registry.astronomer.io/providers/great-expectations/modules/greatexpectationsoperator/  
The operator has several optional parameters, but it always requires either a data_context_root_dir or a data_context_config and either a checkpoint_name or checkpoint_config.

**Note**: An example great expectations project and dag can be found in the re_reporting_sandbox project.
It can be used for reference.

# Enable SNS notification for airflow task failure

To enable SNS notifications for task failures on Airflow, import default args from `airflow_plugins` and add default args in your dag configuration. 
After you add default args, error notification should start triggering on 
every task failure on Airflow.

# Change connection type

Use [this dag](#connection-type-change-dag) to change the connection type on airflow.

# Running Dags

# Project: test_project

## hello world dag

A sample hello-world flow

### Usage:

For triggering airflow dag from CLI, use the command below:

```bash
airflow dags trigger test_project_hello_world_dag
```

and from console, follow the steps mentioned below:

1. After logging into the airflow UI, choose the dag with ID `test_project_hello_world_dag` from the dags list.
2. Click on the pause/play button on the right side of the window.
3. Choose `Trigger Dag`. It will trigger the DAG and the status of the run will be present under `Grid` tab.
4. From there you can check if your task failed or succeeded and check the logs as well.

## airbyte connection dag

This flow will run a task to trigger the airbyte connection.

Before triggering this airflow dag, you would need to create a connection object to `airbyte` on airflow and
pass its ID as a parameter while running the flow.

**Note**: Use 'airbyte_connection' as the connection_name while creating connection below.

For airbyte connection, you can create a new one by following the
[steps to create airbyte connection object on airflow](#creating-connection-object-to-airbyte-on-airflow)

### Usage:

The flow require following parameters:

1. connection_id: Connection id for particular airbyte connection it is the last part of url for particular
   connection in airbyte
   ex `http://localhost:8000/workspaces/d36c35c5-d72a-4691-ad1d-8b38aebae2b2/connections/032488b3-6818-4aa5-b357-f51fb7595a1c`
   where `032488b3-6818-4aa5-b357-f51fb7595a1c` is the connection-id

For triggering the airflow dag, follow the steps below:

1. After logging into the airflow UI, choose the dag with ID `test_project_airbyte_connection_dag` from the dags list.
2. Click on the pause/play button on the right side of the window.
3. Choose `Trigger Dag w/ config`. Add parameter values and click on `Trigger`
4. It will trigger the DAG and the status of the run will be present under `Grid` tab.
5. From there you can check if your task failed or succeeded and check the logs as well.

## airbyte dbt dag

The dag will run a task to trigger the airbyte connection and then run the required dbt task

Before triggering this airflow dag, you would need to create a connection object to `airbyte` and `dbt` on
airflow and pass their ID's as parameters while running the flow.

**Note**: Use 'airbyte_connection' as the connection_name while creating connection below.

For airbyte connection, you can either use an existing airbyte connection object or create a new one by
following the [steps to create airbyte connection object on airflow](#creating-connection-object-to-airbyte-on-airflow)

For dbt connection, you can create a new connection by following the
[steps to create dbt connection object on airflow](#creating-connection-object-to-dbt-on-airflow)

Also, you would need to upload the dbt project directory as a zip file to the S3 and pass it as a parameter
while running the dag.

### Usage:

The flow require following parameters:

1. connection-id: Connection id for particular airbyte connection it is the last part of url for particular
   connection in airbyte
   ex `http://localhost:8000/workspaces/d36c35c5-d72a-4691-ad1d-8b38aebae2b2/connections/032488b3-6818-4aa5-b357-f51fb7595a1c`
   where `032488b3-6818-4aa5-b357-f51fb7595a1c` is the connection-id
2. dbt_conn_id: The name of the dbt connection object created on airflow.
3. dbt_project_path = The S3 URI of the zip file containing dbt project

For triggering the airflow dag, follow the steps below:

1. After logging into the airflow UI, choose the dag with ID `test_project_airbyte_dbt_dag` from the dags list.
2. Click on the pause/play button on the right side of the window.
3. Choose `Trigger Dag w/ config`. Add parameter values and click on `Trigger`
4. It will trigger the DAG and the status of the run will be present under `Grid` tab.
5. From there you can check if your task failed or succeeded and check the logs as well.

# Project: re_reporting

## connection type change dag

The dag changes the connection type of an airflow connection.

### Usage:

The flow require following parameters:

1. conn_id: Id of the connection to change the the type for.
2. target_conn_type: Target connection type.

For triggering the airflow dag, follow the steps below:

1. After logging into the airflow UI, choose the dag with ID `connection_type_change_dag` from the dags list.
2. Click on the pause/play button on the right side of the window.
3. Choose `Trigger Dag w/ config`. Add parameter values and click on `Trigger`
4. It will trigger the DAG and the status of the run will be present under `Grid` tab.
5. From there you can check if your task failed or succeeded and check the logs as well.

# Project: re_reporting_sandbox

## great_expectations_dag

This dag a sample dag and triggers expectations on a sandbox database.

Before triggering the dag, make sure you have all the project requirements installed. Also configure  
great_expectations.yml as your needs.

**Note**: If you want to include great_expectations in this or any other project, follow
[these steps](#using-great-expectations-in-airflow-dags)

### Usage:

For triggering the airflow dag, follow the steps below:

1. After logging into the airflow UI, choose the dag with ID `test_great_expectation_dag` from the dags list.
2. Click on the pause/play button on the right side of the window.
3. It will trigger the DAG and the status of the run will be present under `Grid` tab.
4. From there you can check if your task failed or succeeded and check the logs as well.
