import pytest
import psycopg
import os
from pytest_postgresql import factories
from folio_data_anonymization.database import Database
from tests.mock_db import fixture_db


@pytest.fixture
def env_vars() -> dict:
    return {
        "PGHOST": os.getenv("PGHOST"),
        "PGPORT": int(os.getenv("PGPORT")),
        "PGUSER": os.getenv("PGUSER"),
        "PGPASSWORD": os.getenv("PGPASSWORD"),
        "PGDATABASE": os.getenv("PGDATABASE"),
        "TENANT": os.getenv("TENANT"),
    }


postgresql = factories.postgresql(process_fixture_name="fixture_db")

def test_load_env_vars_pytest_env(env_vars):
    assert env_vars["PGHOST"] == "localhost"
    assert env_vars["PGPORT"] == 5432
    assert env_vars["PGUSER"] == "test_user"
    assert env_vars["PGPASSWORD"] == "admin123"
    assert env_vars["PGDATABASE"] == "dbname"
    assert env_vars["TENANT"] == "diku"


def test_query_db(postgresql):
    cur = postgresql.execute("SELECT * FROM users WHERE id = %s;", ["1"], binary=True)
    (id, data) = cur.fetchone()
    assert data == 'John Doe'


def test_database_instance(env_vars, postgresql):
    db = Database()
    db_connection = db.connection
    assert db_connection.info.host == env_vars["PGHOST"]
    assert db_connection.info.port == env_vars["PGPORT"]
    assert db_connection.info.user == env_vars["PGUSER"]
    assert db_connection.info.password == env_vars["PGPASSWORD"]
    assert db_connection.info.dbname == env_vars["PGDATABASE"]


def test_schema_name(env_vars, postgresql):
    db = Database()
    name = db.schema_name('mod_users')
    assert name == 'diku_mod_users'


def test_table_name(env_vars, postgresql):
    db = Database()
    name = db.table_name('mod_users', 'users')
    assert name == 'diku_mod_users.users'

