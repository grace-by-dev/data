#!/bin/bash
set -e

# Read the password from the environment variable
export PGPASSWORD=$POSTGRES_PASSWORD

# Run the init.sql script
psql -v airflow_password=$AIRFLOW_DB_PASSWORD -U "$POSTGRES_USER" -f /tmp/init.sql

