"""Anonymize Tables in FOLIO based on Configuration File."""

import logging
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator


try:
    from plugins.git_plugins.utils import fake_jsonb, update_row
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.utils import fake_jsonb, update_row

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
    params={
        "tenant": Param(
            "Choose a tenant",
            type="string",
            description="Choose a tenant to anonymize.",
        ),
        "table_config": Param(
            {"key": "value"},
            type="object",
            description="Enter one row from JSON configuration file.",
        )
    }
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
    def get_tuples(**kwargs):
        return kwargs["payload"]["data"]

    @task_group(group_id="row_processing")
    def row_processing_group(**kwargs):
        config = kwargs["payload"]["config"]
        data = kwargs["data"]

        @task
        def anonymize_row(**kwargs) -> dict:
            """
            Anonymize the data
            """
            data: tuple = kwargs["data"]
            config: dict = kwargs["config"]

            return {
                "id": data[0],
                "jsonb": fake_jsonb(data[1], config),
            }

        @task
        def update_table(**kwargs):
            """
            Updates jsonb in the database with faked data
            """
            payload = kwargs["payload"]
            config = kwargs["config"]
            uuid = payload["id"]
            jsonb = payload["jsonb"]
            schema_table = config["table_name"]
            update_row(id=uuid, jsonb=jsonb, schema_table=schema_table)

        mod_data = anonymize_row(data=data, config=config)
        update_table(payload=mod_data, config=config)

    payload = setup()
    all_rows = get_tuples(payload=payload)
    row_processing_group.partial(payload=payload).expand(
        data=all_rows
    ) >> EmptyOperator(task_id="Finished")
