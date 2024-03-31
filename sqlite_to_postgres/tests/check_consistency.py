from typing import Iterator, List
import os
import sqlite3

import pytest
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(name="sqlite_conn")
def fixture_get_sqlite_connection() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(
        os.environ.get("SQLITE_PATH"),
        detect_types=sqlite3.PARSE_DECLTYPES
    )
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture(name="pg_conn")
def fixture_get_pg_connection() -> Iterator[_connection]:
    dsl = {
        "dbname": os.environ.get("PG_NAME"),
        "user": os.environ.get("PG_USER"),
        "password": os.environ.get("PG_PASSWORD"),
        "host": "127.0.0.1",
        "port": 5432,
    }
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    try:
        yield conn
    finally:
        conn.close()


@pytest.mark.parametrize("table_name", ["film_work", "person", "genre", "genre_film_work", "person_film_work"])
def test_sqlite_and_pg_tables_have_same_quantity_of_rows(table_name: str,
                                                         sqlite_conn: sqlite3.Connection,
                                                         pg_conn: _connection) -> None:
    cmd = f"SELECT COUNT(*) FROM {table_name}"
    sqlite_curs = sqlite_conn.cursor()
    sqlite_curs.execute(cmd)
    sqlite_amount = sqlite_curs.fetchone()[0]
    pg_curs = pg_conn.cursor()
    pg_curs.execute(cmd)
    pg_amount = pg_curs.fetchone()[0]
    assert sqlite_amount == pg_amount


@pytest.mark.parametrize(
    "table_name,fields",
    [
        ("film_work", ["id", "title", "description", "creation_date", "file_path", "rating", "type"]),
        ("person", ["id", "full_name"]),
        ("genre", ["id", "name", "description"]),
        ("genre_film_work", ["id", "genre_id", "film_work_id"]),
        ("person_film_work", ["id", "person_id", "film_work_id"]),
    ])
def test_sqlite_and_pg_tables_have_equal_data(table_name: str,
                                              fields: List[str],
                                              sqlite_conn: sqlite3.Connection,
                                              pg_conn: _connection) -> None:
    cmd = f"SELECT {', '.join(fields)} FROM {table_name} ORDER BY id"
    sqlite_curs = sqlite_conn.cursor()
    sqlite_curs.execute(cmd)
    sqlite_data = sqlite_curs.fetchall()
    pg_curs = pg_conn.cursor()
    pg_curs.execute(cmd)
    pg_data = [tuple(row) for row in pg_curs.fetchall()]
    assert len(sqlite_data) == len(pg_data)
    for sqlite_row, pg_row in zip(sqlite_data, pg_data):
        assert sqlite_row == pg_row
