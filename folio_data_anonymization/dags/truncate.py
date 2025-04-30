from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.git_plugins.truncate import truncate


default_args = {
    "owner": "libsys",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "truncate_tables",
    default_args=default_args,
    catchup=False,
    tags=["truncate"],
    params={},
) as dag:

    truncate_tables = PythonOperator(
        task_id="table_truncation",
        python_callable=truncate,
    )


(truncate_tables)
