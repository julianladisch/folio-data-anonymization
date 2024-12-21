import pytest
import os
import psycopg

from pytest_postgresql import factories
from folio_data_anonymization.users import Users
from tests.mock_db import fixture_db

postgresql = factories.postgresql(process_fixture_name="fixture_db")

def test_anonymize_users(mocker, postgresql, caplog):
    mocker.patch(
        "folio_data_anonymization.users.Users",
        return_value=postgresql
    )
    users_task = Users()
    users_task.anonymize_users()
    assert "Anonymizing diku_mod_users.users" in caplog.text

