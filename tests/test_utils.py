import copy
import json
import pathlib

import pydantic
import pytest

from psycopg2 import Error

from airflow.exceptions import AirflowFailException
from airflow.models import Connection

from folio_data_anonymization.plugins.utils import fake_jsonb, update_row, SQLPool
from folio_data_anonymization.plugins.sql_pool import SimpleConnectionPool


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


def test_org_fake_jsonb(configs):
    with (pathlib.Path(__file__).parent / "fixtures/organization.json").open() as fo:
        organization = json.load(fo)

    original_org = copy.deepcopy(organization)
    for row in configs.get("anonymize_organization_tables"):
        if row['table_name'].startswith("mod_organizations_storage.organizations"):
            org_config = row

    fake_jsonb(organization, org_config)

    assert organization["name"] != original_org["name"]
    assert organization["edi"]["libEdiType"] != original_org["edi"]["libEdiType"]
    assert (
        organization["addresses"][0]["addressLine1"]
        != original_org["addresses"][0]["addressLine1"]
    )
    assert organization["addresses"][0]["city"] != original_org["addresses"][0]["city"]
    assert organization["emails"][0]["value"] != original_org["emails"][0]["value"]
    assert (
        organization["accounts"][0]["contactInfo"]
        != original_org["accounts"][0]["contactInfo"]
    )
    assert len(organization["aliases"][0]["description"]) == 0
    assert len(organization["accounts"][0]["description"]) == 0


def test_user_fake_jsonb(configs):
    with (pathlib.Path(__file__).parent / "fixtures/user.json").open() as fo:
        user = json.load(fo)

    original_user = copy.deepcopy(user)
    for row in configs.get("anonymize_users_tables"):
        if row['table_name'].startswith("mod_users.users"):
            user_config = row

    fake_jsonb(user, user_config)

    assert user["username"] != original_user["username"]
    assert user["barcode"] != original_user["barcode"]
    assert user["externalSystemId"] != original_user["externalSystemId"]
    assert user["personal"]["lastName"] != original_user["personal"]["lastName"]
    assert (
        user["personal"]["addresses"][0]["addressLine1"]
        != original_user["personal"]["addresses"][0]["addressLine1"]
    )
    assert len(user.get('customFields')) == 0


def test_update_row(mocker):
    mocker.patch.object(SQLPool, "connection", MockAirflowConnection)
    mocker.patch.object(SQLPool, "pool", MockPool)
    mocker.patch.object(SimpleConnectionPool, "getconn", MockConnection)

    uuid = "f9d5a80e-3b51-11f0-a1d0-5a0f9a6cb774"
    jsonb = {"name": "George Fox"}
    schema_table = "diku_mod_users.users"
    assert update_row(id=uuid, jsonb=jsonb, schema_table=schema_table)


def test_failed_update_row(mocker):
    mocker.patch.object(SQLPool, "connection", MockAirflowConnection)
    mocker.patch.object(SQLPool, "pool", MockPool)
    mocker.patch.object(SimpleConnectionPool, "getconn", MockConnection)

    uuid = "c76983b2-3b52-11f0-a1d0-5a0f9a6cb774"
    jsonb = {"name": "The Giant Hen House"}
    schema_table = "diku_mod_organizations.foo"
    with pytest.raises(
        AirflowFailException,
        match="Failed updating diku_mod_organizations.foo uuid c76983b2-3b52-11f0-a1d0-5a0f9a6cb774",  # noqa
    ):
        update_row(id=uuid, jsonb=jsonb, schema_table=schema_table)
