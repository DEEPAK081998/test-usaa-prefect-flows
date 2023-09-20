# Airflow Variables JSON files
- JSON files in this folder are used by Airflow to import variables.
- Each file correspond to an environment.

Run the following command from the folder containing `docker-compose.yaml`:
- `docker-compose run --rm airflow-webserver airflow variables import dags/variables/rep_292_sandbox.json`
- Replace `airflow-webserver` by the name of docker service (related to airflow webserver) specified in docker-compose.
- Replace `rep_292_sandbox.json` by the corresponding environment, such as `rep_292_beta.json` or `rep_292_prod.json`