# About airflow-ecs-local-runner

This scripts provides a command line interface (CLI) utility that replicates the ecs Apache Airflow environment locally.

## Prerequisites

- Make sure you have all the
  [requirements](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin)
  for running airflow

## Get started

### Step one: Creating env file

* Run the following command from the repo root to create a new env file in `scripts/airflow_ecs_local_runner` directory:

  ```bash
  echo "SOURCE_CODE_ROOT='$(pwd)'" >> scripts/airflow_ecs_local_runner/dev.env
  ```

* After the file is created copy the contents of `.env.default` located in same directory to this new file
  Now
* Now if you have Linux run the below command, for other operating systems add `AIRFLOW_UID=50000` to [dev.env](dev.env)
  ```bash
  echo -e "AIRFLOW_UID=$(id -u)" >> scripts/airflow_ecs_local_runner/dev.env
  ```

**Note:** Modify the value of key `LOCAL_AIRFLOW_PROJECT_PATH` to point to the desired project path

### Step two: Initializing the database

Initialize the database by running the following command:

```bash
./airflow-ecs-local-env airflow-init
```

### Step three: Running Apache Airflow

#### Local runner

Runs a local Apache Airflow environment

```bash
./airflow-ecs-local-env start
```

To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are
stopped.

### Step four: Accessing the Airflow UI

By default, the script creates a username and password for your local Airflow environment.

- Username: `airflow`
- Password: `airflow`

If you want to set your uername and password add keys`_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` to
the [dev.env](dev.env) file with values as required

#### Airflow UI

- Open the Apache Airlfow UI: <http://localhost:8080/>.

for more info refer to
this [guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#)
