# Noora's WhatsApp Data Pipeline

This project sets up a data pipeline using Docker, Airflow, dbt, and Postgres to process WhatsApp communication data.

## Prerequisites

- Docker
- Docker Compose

## Setup

1.  **Clone the Repository**:
    ```bash
    git clone <repository-url>
    cd data-pipeline-noora
    ```

2.  **Environment Variables**:
    This project uses a `.env` file to manage environment variables for the Postgres database. Make sure you have a `.env` file in the root of the project with the following variables:
    ```env
    POSTGRES_USER=noora
    POSTGRES_PASSWORD=noora123
    POSTGRES_DB=whatsapp_communication
    ```
    These variables are used by the `postgres` service in the `docker-compose.yml` file.

## How to Run

1.  **Build and Start the Services**:
    To build the Docker images and start all the services in detached mode, run the following command:
    ```bash
    docker-compose up -d --build
    ```
    This will start the following services:
    -   `postgres`: The PostgreSQL database.
    -   `airflow`: The Airflow web server, scheduler, and worker.
    -   `pgadmin`: A web-based administration tool for Postgres.
    -   `jupyter`: A Jupyter Notebook server.

2.  **Create a New Airflow User (Optional)**:
    By default, you can log in to the Airflow UI with the username `admin` and password `admin`. To create a new user, run the following command from your terminal:
    ```bash
    docker-compose exec airflow bash -c "airflow users create \
        --username noora_user \
        --firstname Noor \
        --lastname Admin \
        --role Admin \
        --email noora@sushant.com \
        --password noora"
    ```

3.  **Accessing the Services**:
    -   **Airflow UI**: [http://localhost:8080](http://localhost:8080)
        -   **Username**: `admin` (or `noora_user` if created)
        -   **Password**: `admin` (or `noora` if created)
    -   **pgAdmin UI**: [http://localhost:8081](http://localhost:8081)
        -   **Username**: `admin@admin.com`
        -   **Password**: `admin`
    -   **Jupyter Notebook**: [http://localhost:8888](http://localhost:8888)

## Data Pipeline Workflow

The data pipeline is orchestrated by Airflow and is defined in the `airflow/dags/noora_whatsapp_pipeline.py` DAG. The pipeline is scheduled to run daily and performs the following steps:

1.  **Fetch Data**:
    -   The `fetch_data` task executes the `scripts/fetch_data.py` script.
    -   This script downloads two CSV files from Google Sheets: `whatsapp_messages` and `whatsapp_statuses`.
    -   The downloaded files are saved as TSV files in the `data/raw` directory, with the current date appended to the filename.

2.  **Load Data into Postgres**:
    -   **Load Messages**: The `load_messages` task executes a SQL script that creates a `messages` table in the Postgres database and loads the data from the latest `whatsapp_messages` TSV file.
    -   **Load Statuses**: The `load_statuses` task executes a SQL script that creates a `statuses` table in the Postgres database and loads the data from the latest `whatsapp_statuses` TSV file.

3.  **Run dbt for Transformation and Validation**:
    -   The `run_dbt` task executes the `dbt build` command, which runs all the dbt models.
    -   **Staging**: The `stg_messages` and `stg_statuses` models clean and prepare the raw data.
    -   **Intermediate Transformation**: The `int_messages_with_status_history` model joins the message and status data to create a comprehensive view of each message with its status history.
    -   **Data Marts**:
        -   The `dim_messages` model creates a dimension table for messages.
        -   The `fct_data_quality_audits` model runs data quality checks to ensure the integrity of the data.

4.  **Data Visualization**:
    -   After the pipeline has run successfully, the transformed data can be visualized using the `notebooks/data_visualization.ipynb` Jupyter notebook.

## Docker Workflow

The `docker-compose.yml` file orchestrates the different services required for the pipeline:

-   **`postgres`**: This service runs a PostgreSQL 15 database. It uses a volume to persist data across container restarts.
-   **`airflow`**: This service builds a custom Docker image using the provided `Dockerfile`. It runs the Airflow standalone command, which includes the webserver, scheduler, and worker. The local `dags`, `logs`, `plugins`, `data`, `scripts`, and `dbt_noora` directories are mounted into the container, allowing you to develop and test your DAGs locally.
-   **`pgadmin`**: This service runs the pgAdmin 4 web interface, which can be used to connect to the `postgres` service and inspect the database.
-   **`jupyter`**: This service runs a Jupyter Notebook server, which can be used to visualize the transformed data. The `notebooks` directory is mounted into the container, so you can access the `data_visualization.ipynb` file.

## How to Stop

To stop all the running services, use the following command:
```bash
docker-compose down
```

## Data Transformation and Validation

The data transformation and validation logic is managed by dbt. The dbt models are responsible for cleaning, transforming, and validating the raw data.

### Data Transformation

The core data transformation logic is handled by the `int_messages_with_status_history` model. This model joins the `stg_messages` and `stg_statuses` models to create a single view of each message with its complete status history. The key transformations are:

-   **Deduplication**: The `unique_messages` CTE ensures that only the most recent version of each message is used.
-   **Pivoting Statuses**: The `pivoted_statuses` CTE transforms the status data from a long format to a wide format, creating separate columns for `sent_at`, `delivered_at`, `read_at`, and `failed_at`.

### Data Validation

The `fct_data_quality_audits` model runs a series of data quality checks and logs any issues found. The following checks are performed:

-   **Duplicate Records**: Detects and flags duplicate messages based on identical content and similar `inserted_at` timestamps.
-   **Logical Consistency**:
    -   Checks if a message was marked as `read` before it was `sent`.
    -   Checks if a message was marked as `delivered` before it was `sent`.
-   **Missing Critical Info**: Ensures that outbound messages have an addressee and inbound messages have a sender.
-   **Invalid Routing Info**: Checks for empty addressee or sender information.

## Data Analysis

The `scripts` directory contains scripts to analyze the transformed data. To run the analysis scripts, you need to have Python and the required packages installed.

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the Analysis Scripts**:
    -   **Plot User Trends**:
        ```bash
        python scripts/plot_user_trends.py
        ```
    -   **Get Read Fraction**:
        ```bash
        python scripts/get_read_fraction.py
        ```
    -   **Plot Time to Read Distribution**:
        ```bash
        python scripts/plot_time_to_read_distribution.py
        ```
    -   **Plot Outbound Message Statuses**:
        ```bash
        python scripts/plot_outbound_message_statuses.py
        ```
