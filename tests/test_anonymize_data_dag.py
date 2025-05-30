import json
import pathlib
import pytest

from folio_data_anonymization.dags.anonymize_data import (
    setup,
    get_tuples,
)


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


def test_setup(mocker, mock_dag_run, caplog):
    payload = setup.function(params=mock_dag_run.conf)
    assert payload["config"]["table_name"] == "diku_mod_users.users"
    assert "Begin processing 1 records from diku_mod_users.users" in caplog.text
    data_tuples = get_tuples.function(payload=payload)
    assert data_tuples[0][0] == "925329d6-3caa-4ae0-bea8-705d70b7a51c"
    assert isinstance(json.loads(payload["data"][0][1]), dict)
