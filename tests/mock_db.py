import pytest
import psycopg
import os
from pytest_postgresql import factories


def load_database(**kwargs):
    db_connection = psycopg.connect(**kwargs)
    with db_connection.cursor() as cur:
        cur.execute("CREATE TABLE users (id serial PRIMARY KEY, name varchar);")
        cur.execute(
            "INSERT INTO users (name) VALUES"
            "('John Doe'), ('Jane Doe'), ('Mary Smith');"
        )
        db_connection.commit()


fixture_db = factories.postgresql_proc(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    dbname=os.getenv("PGDATABASE"),
    load=[load_database],
)
