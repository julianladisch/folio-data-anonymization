import pydantic
import pytest

from folio_data_anonymization.dags.select_tables import (
    calculate_table_ranges,
    combine_table_counts,
)

from folio_data_anonymization.plugins.select_tables import (
    do_anonymize,
    fetch_record_counts_per_table,
    schemas_tables,
)


class MockSQLExecuteQueryOperator(pydantic.BaseModel):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__dict__.update(kwargs)

    def execute(self, sql):
        kwargs = self.__dict__
        match kwargs["result_type"]:
            case "counts":
                return mock_result_set_count()
            case "selections":
                return mock_result_set_selections()
            case _:
                return []


def mock_result_set_count():
    return [
        (76, 'count'),
    ]


def mock_result_set_selections():
    return [(), (), ()]


@pytest.fixture
def mock_get_current_context(monkeypatch, mocker):
    def _context():
        context = mocker.stub(name="context")
        context.get = lambda *args: {}
        return context

    monkeypatch.setattr(
        'folio_data_anonymization.plugins.select_tables.get_current_context',
        _context,
    )


@pytest.fixture
def config():
    return {
        "anonymize_abc_tables": [
            {"table_name": "mod_table_a.one", "anonymize": {"jsonb": []}},
            {"table_name": "mod_table_b.two", "anonymize": {"jsonb": []}},
            {
                "table_name": "mod_table_c.three",
                "anonymize": {"jsonb": []},
                "set_to_empty": {"jsonb": []},
            },
        ]
    }


@pytest.fixture
def mock_dag_bag(mocker):
    def mock_get_dag(dag_id: str):
        return mocker.MagicMock()

    dag_bag = mocker.MagicMock()
    dag_bag.get_dag = mock_get_dag
    return dag_bag


def test_fetch_record_counts(mocker, mock_get_current_context, config):
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.SQLExecuteQueryOperator',
        return_value=MockSQLExecuteQueryOperator(result_type="counts"),
    )
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.sql_count_file',
        return_value='folio_data_anonymization/plugins/sql/counts.sql',
    )

    table = schemas_tables(config, "diku")[0]
    count = fetch_record_counts_per_table(schema=table)
    assert count == 76


def test_anonymize_selections(
    mocker, mock_get_current_context, mock_dag_bag, config, caplog
):
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.SQLExecuteQueryOperator',
        return_value=MockSQLExecuteQueryOperator(result_type="selections"),
    )
    mocker.patch(
        'folio_data_anonymization.plugins.select_tables.sql_selections_file',
        return_value='folio_data_anonymization/plugins/sql/selections.sql',
    )
    dag_bag = mocker.patch(
        "folio_data_anonymization.plugins.select_tables.DagBag",
        return_value=mock_dag_bag,
    )

    tables = schemas_tables(config, "diku")
    assert "diku_mod_table_c.three" in tables

    counts = combine_table_counts.function(
        schema_table=tables, record_counts_per_table=[38, 74, 76]
    )
    assert counts["diku_mod_table_a.one"] == 38

    ranges = calculate_table_ranges.function(
        batch_size=10, counts=counts, schemas_tables=tables
    )

    assert ranges[0]["table"] == "diku_mod_table_a.one"
    assert ranges[0]["ranges"][0] == (0, 10)
    assert ranges[0]["ranges"][3] == (30, 40)

    do_anonymize(ranges, config, "diku")
    assert ("Selecting records batch") in caplog.text
    assert dag_bag.called
    assert ("Anonymizing diku_mod_table_a.one with OFFSET: 0 LIMIT: 10") in caplog.text
    assert (
        "{'table_name': 'diku_mod_table_c.three', 'anonymize': {'jsonb': []}, 'set_to_empty': {'jsonb': []}}"  # noqa
    ) in caplog.text
