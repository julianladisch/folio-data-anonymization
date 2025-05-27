"""Anonymize Tables in FOLIO based on Configuration File."""

import logging
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task


logger = logging.getLogger(__name__)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "anonymize_data",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["anonymize"],
) as dag:

    @task
    def setup(**kwargs) -> dict:
        """
        Setup task to prepare the environment for anonymization.
        select_tables DAG will pass:
        conf = {"tenant": "",
                "table_config":
                    {"table_name": "diku_mod_users.users",
                        "anonymize": {"jsonb": []}},
                        "set_to_empty": {"jsonb": []}},
                "data": (('id1', 'jsonb1'), ('id2', 'jsonb2'),...n)
                }
        """
        params = kwargs.get("params", {})
        table_config: dict = params.get("table_config", {})
        data: list = params.get("data", ())
        tenant = params.get("tenant", "diku")
        schema_table_name = f"{tenant}_{table_config.get("table_name")}"
        logger.info(f"Processing {len(data)} records from {schema_table_name}")
        return {"config": table_config, "data": data}

    @task
    def anonymize_row(**kwargs):
        """
        Anonymize the data
        """
        pass

    @task  # sql update the data
    def update_table():
        pass


payload = setup()
anonymize_row().partial(config=payload["config"]).expand(data=payload["data"]) # type: ignore
