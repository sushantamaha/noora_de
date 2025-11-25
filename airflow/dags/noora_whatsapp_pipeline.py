# airflow/dags/noora_whatsapp_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

with DAG(
    dag_id="noora_whatsapp_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["noora", "whatsapp"],
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_data",
        bash_command="python /opt/airflow/scripts/fetch_data.py",
    )

    load_messages = BashOperator(
        task_id="load_messages",
        bash_command="""
set -euo pipefail

FILE=$(ls -t /opt/airflow/data/raw/whatsapp_messages_*.tsv | head -n1 || echo "")
if [[ -z "$FILE" ]]; then echo "No messages file found!"; exit 1; fi

echo "Loading messages from $FILE"

psql $AIRFLOW_CONN_POSTGRES_DEFAULT <<SQL
DROP TABLE IF EXISTS public.messages CASCADE;

CREATE TABLE IF NOT EXISTS public.messages (
    id                    BIGINT,
    message_type          TEXT,
    masked_addressees     TEXT,
    masked_author         TEXT,
    content               TEXT,
    author_type           TEXT,
    direction             TEXT,
    external_id           TEXT,
    external_timestamp    TEXT,        -- keep as TEXT first
    masked_from_addr      TEXT,
    is_deleted            BOOLEAN,
    last_status           TEXT,
    last_status_timestamp TEXT,
    rendered_content      TEXT,
    source_type           TEXT,
    uuid                  TEXT,
    inserted_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ DEFAULT NOW()
);


\\copy public.messages FROM '$FILE' WITH (FORMAT csv, DELIMITER E'\\t', HEADER true, NULL '')

-- Fix timestamp columns after load
ALTER TABLE public.messages 
ALTER COLUMN external_timestamp TYPE TIMESTAMPTZ USING external_timestamp::TIMESTAMPTZ,
ALTER COLUMN last_status_timestamp TYPE TIMESTAMPTZ USING NULLIF(last_status_timestamp, '')::TIMESTAMPTZ,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID;
SQL
""",
        env={"AIRFLOW_CONN_POSTGRES_DEFAULT": "postgresql://noora:noora123@postgres:5432/whatsapp_communication"},
    )

    load_statuses = BashOperator(
        task_id="load_statuses",
        bash_command="""
set -euo pipefail

FILE=$(ls -t /opt/airflow/data/raw/whatsapp_statuses_*.tsv | head -n1 || echo "")
if [[ -z "$FILE" ]]; then echo "No statuses file found!"; exit 1; fi

psql $AIRFLOW_CONN_POSTGRES_DEFAULT <<SQL
DROP TABLE IF EXISTS public.statuses CASCADE;

CREATE TABLE public.statuses (
    id BIGINT, 
    status TEXT, 
    timestamp TIMESTAMPTZ, 
    uuid UUID, 
    message_uuid UUID,
    message_id BIGINT, 
    number_id BIGINT, 
    inserted_at TIMESTAMPTZ DEFAULT NOW(), 
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

\\copy public.statuses FROM '$FILE' WITH (FORMAT csv, DELIMITER E'\\t', HEADER true, NULL '')
SQL
""",
        env={"AIRFLOW_CONN_POSTGRES_DEFAULT": "postgresql://noora:noora123@postgres:5432/whatsapp_communication"},
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="""
        set -euo pipefail
        cd /opt/airflow/dbt_noora
        dbt deps --profiles-dir . --project-dir .
        dbt build --profiles-dir . --project-dir .
        """,
    )
    fetch_data >> [load_messages, load_statuses] >> run_dbt
