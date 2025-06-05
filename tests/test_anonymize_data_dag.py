import json
import pathlib
import pydantic
import pytest

from psycopg2 import Error

from airflow.models import Connection

from folio_data_anonymization.plugins.utils import SQLPool
from folio_data_anonymization.plugins.sql_pool import SimpleConnectionPool

from folio_data_anonymization.dags.anonymize_data import (
    anonymize_row_update_table,
    prepare_payload,
    get_tuples,
)


class MockCursor(pydantic.BaseModel):
    def fetchall(self):
        return mock_result_set()

    def execute(self, sql_stmt, params):
        if str(params["table"]) == "diku_mod_organizations.foo":
            raise Error()
        self


class MockConnection(pydantic.BaseModel):
    def cursor(self):
        return MockCursor()


class MockPool(pydantic.BaseModel):
    def pool(self):
        return self

    def getconn(self):
        return MockConnection()

    def cursor(self):
        return MockCursor()


class MockAirflowConnection(pydantic.BaseModel):
    def get_connection_from_secrets(self):
        return Connection(  # noqa
            conn_id="postgres-folio",
            conn_type="postgres",
            host="example.com",
            password="pass",
            port=5432,
        )


def mock_result_set():
    return []


@pytest.fixture
def mock_data() -> list:
    with (pathlib.Path(__file__).parent / "fixtures/user.json").open() as fo:
        user = json.load(fo)

    return [(user["id"], json.dumps(user))]


@pytest.fixture
def mock_dag_run(mocker, configs, mock_data):
    user_config = configs["anonymize_users_tables"][0]
    schema_table_name = "".join(("diku_", user_config["table_name"]))
    user_config["table_name"] = schema_table_name

    dag_run = mocker.stub(name="dag_run")
    dag_run.run_id = "manual__2024-07-29T19:00:00:00:00"
    dag_run.dag = mocker.stub(name="dag")
    dag_run.dag.dag_id = "anonymize_data"
    dag_run.conf = {
        "tenant": "diku",
        "table_config": user_config,
        "data": mock_data,
    }

    return dag_run


def test_prepare_payload(mocker, mock_dag_run, caplog):
    payload = prepare_payload.function(params=mock_dag_run.conf)
    assert payload["config"]["table_name"] == "diku_mod_users.users"
    assert "Begin processing 1 records from diku_mod_users.users" in caplog.text
    data_tuples = get_tuples.function(payload=payload)
    assert data_tuples[0][0] == "925329d6-3caa-4ae0-bea8-705d70b7a51c"
    assert isinstance(json.loads(payload["data"][0][1]), dict)


def test_anonymize_row_update_table(mocker, mock_dag_run, mock_data, caplog):
    mocker.patch("folio_data_anonymization.plugins.utils.update_row", return_value=True)
    mocker.patch.object(SQLPool, "connection", MockAirflowConnection)
    mocker.patch.object(SQLPool, "pool", MockPool)
    mocker.patch.object(SimpleConnectionPool, "getconn", MockConnection)

    payload = prepare_payload.function(params=mock_dag_run.conf)

    anonymize_row_update_table.function(data=mock_data[0], payload=payload)
    assert "stanford" not in caplog.text
