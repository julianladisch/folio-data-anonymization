"""Anonymize Tables in FOLIO based on Configuration File."""
import json
import logging
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
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
    render_template_as_native_obj=True,
    max_active_runs=1,
    max_active_tasks=10,
) as dag:

    @task
    def prepare_payload(**kwargs) -> dict:
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
        data: list = params.get("data", [])
        tenant = params.get("tenant", "diku")
        logger.info(f"Anonymizing data for tenant {tenant}")
        logger.info(
            f"Begin processing {len(data)} records from {table_config.get("table_name")}"  # noqa
        )
        return {"config": table_config, "data": data}

    @task
    def get_tuples(**kwargs):
        return kwargs["payload"]["data"]

    @task
    def anonymize_row_update_table(**kwargs):
        data: tuple = kwargs["payload"]["data"]
        config: dict = kwargs["payload"]["config"]
        logger.info(f"Anonymizing record {data[0]}")

        fake_json = fake_jsonb(data[1], config)
        update_row(
            id=data[0], jsonb=json.dumps(fake_json), schema_table=config["table_name"]
        )

    payload = prepare_payload()

    all_rows = get_tuples(payload=payload)

    anonymize_row_update_table.partial(payload=payload).expand(
        data=all_rows
    ) >> EmptyOperator(task_id="Finished")
