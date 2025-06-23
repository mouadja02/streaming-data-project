import os

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WORKERS = 4
SUPERSET_TIMEOUT = 60
SUPERSET_WEBSERVER_PORT = 8088
SUPERSET_WEBSERVER_TIMEOUT = 60

# Postgres database connection
SQLALCHEMY_DATABASE_URI = 'postgresql://airflow:airflow@postgres:5432/airflow'

# A secret key that will be used for securely signing the session cookie
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'this_is_a_default_secret_key')

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
} 