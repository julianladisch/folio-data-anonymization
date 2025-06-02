import json
import logging
import pathlib

from psycopg2.extensions import AsIs
from typing import Union

from faker import Faker
from jsonpath_ng import parse

from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    from plugins.git_plugins.providers import Organizations, Users
except (ImportError, ModuleNotFoundError):
    from folio_data_anonymization.plugins.providers import Organizations, Users

logger = logging.getLogger(__name__)

faker = Faker()
faker.add_provider(Organizations)
faker.add_provider(Users)


def fake_jsonb(jsonb: dict, config: dict) -> dict:
    """
    Fake the jsonb data based on the provided config.
    """
    for row in config["anonymize"]["jsonb"]:
        expr = parse(row[0])
        faker_function = getattr(faker, row[1])
        expr.update(jsonb, faker_function())
    for row in config.get("set_to_empty", {}).get("jsonb", []):
        expr = parse(row)
        expr.update(jsonb, "")
    return jsonb


def update_row(**kwargs) -> Union[bool, None]:
    row_uuid: str = kwargs['id']
    jsonb: dict = kwargs['jsonb']
    schema_table: str = kwargs['schema_table']
    update_sql: str = _get_sql_file("update_jsonb.sql")

    context = get_current_context()
    try:
        SQLExecuteQueryOperator(
            task_id="update_data_in_row",
            conn_id="postgres_folio",
            database="okapi",
            sql=update_sql,
            parameters={
                "schema_table": AsIs(schema_table),
                "jsonb": json.dumps(jsonb),
                "uuid": row_uuid,
            },
        ).execute(context)
        logger.info(f"Successfully updated {schema_table} uuid {row_uuid}")
        return True
    except Exception as e:
        raise AirflowFailException(
            f"Failed updating {schema_table} uuid {row_uuid} - {e}"
        )


def _get_sql_file(file_name: str) -> str:
    sql_path = pathlib.Path(__file__).parent / f"sql/{file_name}"
    return sql_path.read_text()
