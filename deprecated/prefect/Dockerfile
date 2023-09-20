ARG PREFECT_BASE_IMAGE_URI=prefecthq/prefect:0.15.13-python3.7

FROM $PREFECT_BASE_IMAGE_URI

RUN pip install --upgrade pip

RUN pip install dbt-core

RUN pip install dbt-postgres

RUN pip install pandas

COPY dbt/ /
