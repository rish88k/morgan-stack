#!/usr/bin/env bash
set -euo pipefail

echo "Running Airflow DB migrations..."
airflow db migrate

echo "Creating Airflow admin user..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME}" \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}"

echo "Airflow init completed successfully."
