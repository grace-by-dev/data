FROM python:3.12-slim-bullseye

COPY ./requirements.txt requirements.txt

RUN pip install --no-cache-dir uv \
    && uv pip install --no-cache --system -r requirements.txt

COPY ./airflow-entrypoint.sh /entrypoint.sh

ENTRYPOINT bash entrypoint.sh