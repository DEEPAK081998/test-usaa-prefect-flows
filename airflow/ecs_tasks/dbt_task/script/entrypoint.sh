#!/usr/bin/env bash

# Global defaults
WORK_DIR="${WORK_DIR:="usr/app/dbt"}"
DB_THREADS=${DB_THREADS:="2"}
DB_TYPE=${DB_TYPE:="postgres"}
DB_SCHEMA=${DB_SCHEMA:="public"}
DB_PORT=${DB_PORT:="5432"}
DBT_PROJECTS_DIR="${WORK_DIR}/projects/project"
DBT_PROFILES_DIR="${WORK_DIR}/profiles"
PROJECT_NAME="testrun"
AWS_PROFILE=${AWS_PROFILE:="default"}

create_dbt_profile() {
  # Creates dbt profile
  mkdir -p ${DBT_PROFILES_DIR}
  echo "
  $PROJECT_NAME:
    outputs:
      prod:
        dbname: ${DB_NAME}
        host: ${DB_HOST}
        pass: ${DB_PASS}
        port: ${DB_PORT}
        schema: ${DB_SCHEMA}
        threads: ${DB_THREADS}
        type: ${DB_TYPE}
        user: ${DB_USER}
    target: prod
  " >"$DBT_PROFILES_DIR/profiles.yml"
}

fetch_and_extract_s3_zip() {
  # Extract dbt project from s3 and unzip it
  echo "Extracting dbt project from S3"
  zip_name=$(basename "${DBT_PROJECT_PATH}")
  PROJECT_NAME=$(basename "${DBT_PROJECT_PATH}" ".zip")
  aws s3 cp "${DBT_PROJECT_PATH}" "/tmp/$zip_name" --profile "${AWS_PROFILE}"
  mkdir -p "${WORK_DIR}/projects/$PROJECT_NAME"
  unzip "/tmp/$zip_name" -d "${WORK_DIR}/projects/$PROJECT_NAME"
  DBT_PROJECTS_DIR="${WORK_DIR}/projects/$PROJECT_NAME"
}

# Check if dbt path is given and is s3 or not
if [[ -n "${DBT_PROJECT_PATH}" ]] && [[ "${DBT_PROJECT_PATH}" == *"s3://"* ]]; then
  fetch_and_extract_s3_zip
elif [[ -n "${DBT_PROJECT_PATH}" ]] && [[ ! "${DBT_PROJECT_PATH}" == *"s3://"* ]]; then
  echo "setting dbt project path to ${DBT_PROJECT_PATH}"
  DBT_PROJECTS_DIR=${DBT_PROJECT_PATH}
  PROJECT_NAME=$(basename "${DBT_PROJECT_PATH}" ".zip")
else
  echo "No project path provided using default path $DBT_PROJECTS_DIR"
fi

if [[ -z "${DB_NAME}" || -z "${DB_HOST}" || -z "${DB_PASS}" || -z "${DB_USER}" ]]; then
  echo "Error: dbt profile not set"
  exit 1
fi

create_dbt_profile

# Main dbt command
dbt "$@" --project-dir "${DBT_PROJECTS_DIR}" --profiles-dir "${DBT_PROFILES_DIR}"
