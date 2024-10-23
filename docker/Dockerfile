FROM python:3.12-slim-bullseye

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir uv 


RUN uv pip install --no-cache --system -r requirements.txt

COPY ./docker/airflow-entrypoint.sh /entrypoint.sh

ENTRYPOINT bash entrypoint.sh