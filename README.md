# End-to-End Cloud Data Engineering Project

This project is a complete end-to-end data engineering pipeline designed to showcase modern, cloud-native data technologies and best practices. It's an ideal portfolio project for aspiring data engineers, cloud data architects, and anyone interested in building scalable data platforms.

The pipeline ingests data from a public API, processes it in real-time, stores it in a data lake, and transforms it into a data warehouse for analytics and visualization.

## Architecture

The following diagram illustrates the architecture of the data platform. If you have an `architecture.png` file, you can replace the diagram below by using the following markdown: [Architecture Diagram](architecture.png)


### Workflow

1.  **Ingest**: An Apache Airflow DAG runs on a schedule to fetch user data from the `randomuser.me` public API.
2.  **Stream**: The fetched data is published to an Apache Kafka topic.
3.  **Process & Land**: An Apache Spark Streaming job consumes the data from Kafka in real-time, processes it, and writes it to an AWS S3 data lake in Parquet format.
4.  **Catalog (Optional but Recommended)**: AWS Glue can be used to crawl the data in S3 and create a data catalog, making it easily queryable.
5.  **Load & Transform**: Another Airflow DAG orchestrates the process of loading data from S3 into an Amazon Redshift data warehouse. After loading, it triggers a `dbt` run to transform the raw data into clean, analysis-ready models.
6.  **Visualize**: Apache Superset is connected to the Redshift data warehouse to create interactive dashboards and visualizations.

## Tech Stack

-   **Orchestration**: Apache Airflow
-   **Real-time Messaging**: Apache Kafka
-   **Data Processing**: Apache Spark
-   **Data Lake**: AWS S3
-   **Data Warehouse**: Amazon Redshift
-   **Data Transformation**: dbt (Data Build Tool)
-   **Data Visualization**: Apache Superset
-   **Containerization**: Docker & Docker Compose
-   **Metadata Catalog**: AWS Glue (Optional)

## Getting Started

Follow these steps to set up and run the project.

### Prerequisites

-   [Docker](https://www.docker.com/products/docker-desktop) and Docker Compose
-   An [AWS Account](https://aws.amazon.com/free/)

### 1. AWS Setup

You'll need to create the following resources in your AWS account.

1.  **S3 Bucket**: Create a standard S3 bucket to serve as your data lake.
2.  **Redshift Cluster**: Provision a new Amazon Redshift cluster. Note down the cluster hostname, database name, username, and password.
3.  **IAM User**: Create a new IAM user with "Programmatic access". This will provide you with an `AWS_ACCESS_KEY_ID` and an `AWS_SECRET_ACCESS_KEY`. Attach a policy to this user that grants permissions to read/write to your S3 bucket and access your Redshift cluster.

### 2. Local Configuration

1.  **Clone the Repository**:
    ```bash
    git clone <your-repo-url>
    cd <repo-name>
    ```
2.  **Configure AWS Credentials**:
    - Open the `.env` file and replace the placeholder values with the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` of your IAM user.
    ```
    AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY
    ```
    **Note**: The `.env` file is included in `.gitignore` to prevent you from accidentally committing your secrets.

3.  **Configure S3 Bucket in Spark**:
    - Open `spark_stream.py`.
    - Find the line `option("path", "s3a://your-s3-bucket/users")` and replace `your-s3-bucket` with the name of your S3 bucket.

4.  **Configure dbt Connection**:
    - Open `dbt_profiles/profiles.yml`.
    - Replace the placeholder values with your Redshift cluster's connection details.

### 3. Running the Project

Once you have completed the setup and configuration, you can start all the services using Docker Compose:

```bash
docker-compose up -d --build
```

### 4. Accessing the Services

-   **Airflow Web UI**: `http://localhost:8080` (user: `airflow`, pass: `airflow`)
-   **Superset Web UI**: `http://localhost:8088` (you will need to run commands to create an admin user first)
-   **Adminer**: `http://localhost:8081` (for browsing the Postgres DB used by Airflow/Superset)
-   **Confluent Control Center**: `http://localhost:9021` (for managing Kafka)

To create a Superset admin user:
```bash
docker-compose exec superset superset fab create-admin
```
Follow the prompts to create your user. After that, you'll need to initialize the database:
```bash
docker-compose exec superset superset db upgrade
docker-compose exec superset superset init
``` 
