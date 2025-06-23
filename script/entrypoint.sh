#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command -v pip) install --user -r requirements.txt
fi

if [ "$1" = "webserver" ]; then
  airflow db upgrade
  exec airflow webserver
elif [ "$1" = "scheduler" ]; then
  # Give the webserver time to start
  sleep 10
  airflow db init
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin || true
  exec airflow scheduler
fi