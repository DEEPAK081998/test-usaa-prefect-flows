# README

This readme will help setting up airflow on local and also writing basic dags

# Setting up airflow on local
For setting up airflow on local, follow
(https://airflow.apache.org/docs/apache-airflow/stable/start/local.html#running-airflow-locally)

# Running airflow server
For running airflow server on local, run the command below
```bash
airflow standalone
```
The airflow server will be started at port 8080 by default. Visit (http://localhost:8080) to access the airflow UI.
Use the admin account details provided as an output of the command above to login.

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

# Creating a great expectations project

In order to create a new great expectations project, install great expectations and other 
dependencies on local by running the following command

```bash
pip install -r requirements.txt
```

After installing great expectations on local, the next step is to initialize a great expectations project.  
Great expectations projects will be kept under `dags/plugins`.  

Follow the steps mentioned below to initialize a great expectations project

```bash
cd dags/plugins
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

# Using Great expectations in airflow dags

A default great expectations project has been added in the plugins. Use that project to add expectations, checkpoints etc.
In order to add new expectations, data sources and checkpoints to the project, follow [these steps](#configuring-a-data-source).

## Using great expectations operator in airflow dag

Add following line at the top of your airflow dag in which you want to use great expectations operator

```bash
os.environ['MDP_S3_BUCKET'] = Variable.get('mdp-s3-bucket')
```

After you add this line, you will have to add a variable from airflow UI with name `mdp-s3-bucket` and value
will be the name of the s3 bucket in which you want to push expectation result. Add that bucket name whose 
access is given to airflow resource.

After you follow the steps mentioned above, you can use the operator in your dag.
The operator's documentation can be found at https://registry.astronomer.io/providers/great-expectations/modules/greatexpectationsoperator/.  
The operator has several optional parameters, but it always requires either a data_context_root_dir or a data_context_config and either a checkpoint_name or checkpoint_config.

**Note**: An example dag can be found under re_reporting_sandbox project which can be used for reference.

# Enable SNS notification for airflow task failure

To enable SNS notifications for task failures on Airflow, add a variable named `sns_task_failure_alert_topic_arn`
on Airflow. It's value should be the ARN of the SNS topic to publish the error notification on.
Also import default args from plugins/airflow_plugins and add default args in your dag configuration. 
After you setup the SNS topic ARN and add default args, error notification should start triggering on 
every task failure on Airflow.

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

## dag

The dag will fetch connection ids from DynamoDB, syncs airbyte connections for those connection ids and then triggers
dbt task

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
   1. table_name: Name of DynamoDB table where to fetch connection ids from.
   2. project_name: Name of the project associated with those connections.
   3. dbt_conn_id: The name of the dbt connection object created on airflow.
   4. dbt_project_path = The S3 URI of the zip file containing dbt project
   5. dbt_extra_kwargs = A dict of mappings to be passed to dbt run as arguments. It should have schema as follows,  
   `{ "select": list[str], "models": list[str], "exclude": list[str] }`

For triggering the airflow dag, follow the steps below:

**Note**: If you want dag to be triggered on regular intervals, store a variable on airflow with key
`re_reporting_dag_schedule` and value `<a cron expression>`

1. After logging into the airflow UI, choose the dag with ID `test_project_airbyte_dbt_dag` from the dags list.
2. Click on the pause/play button on the right side of the window.
3. Choose `Trigger Dag w/ config`. Add parameter values and click on `Trigger`
4. It will trigger the DAG and the status of the run will be present under `Grid` tab.
5. From there you can check if your task failed or succeeded and check the logs as well.

# Project: re_reporting_sandbox

## great_expectations_dag

This dag triggers expectations on a sandbox database.

Before triggering the dag, you will have to add a variable from airflow UI with name `mdp-s3-bucket`
and value will be the name of the s3 bucket in which you want to push expectation result. Add that 
bucket name whose access is given to airflow resource.

### Usage:

For triggering the airflow dag, follow the steps below:

1. After logging into the airflow UI, choose the dag with ID `test_great_expectation_dag` from the dags list.
2. Click on the pause/play button on the right side of the window.
3. It will trigger the DAG and the status of the run will be present under `Grid` tab.
4. From there you can check if your task failed or succeeded and check the logs as well.
