import os
from typing import Iterator
from contextlib import contextmanager
import logging
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

from loader import models
from loader.db_executors import SQLiteExtractor, PostgresLoader, Table

load_dotenv()
psycopg2.extras.register_uuid()
logging.basicConfig(format="[%(asctime)s] [%(levelname)s] %(message)s", level=logging.INFO)


def load_from_sqlite(sqlite_conn: sqlite3.Connection, pg_conn: _connection) -> None:
    """Base method for loading data from SQLite to Postgres"""
    sqlite_extractor = SQLiteExtractor(sqlite_conn)
    postgres_loader = PostgresLoader(pg_conn)

    tables = (
        Table("film_work", models.Filmwork),
        Table("person", models.Person),
        Table("genre", models.Genre),
        Table("genre_film_work", models.GenreFilmwork),
        Table("person_film_work", models.PersonFilmwork),
    )
    for table in tables:
        logging.info(f"Starting loading data for table: {table.name}")
        postgres_loader.truncate_table(table)
        for data_chunk in sqlite_extractor.extract_from_table(table):
            postgres_loader.load_to_table(table, data_chunk)


@contextmanager
def _get_sqlite_conn(db_path: str) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


if __name__ == "__main__":
    dsl = {
        "dbname": os.environ.get("PG_NAME"),
        "user": os.environ.get("PG_USER"),
        "password": os.environ.get("PG_PASSWORD"),
        "host": "127.0.0.1",
        "port": 5432,
    }
    with _get_sqlite_conn(os.environ.get("SQLITE_PATH")) as sqlite_conn:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            logging.info("Starting loading data")
            try:
                load_from_sqlite(sqlite_conn, pg_conn)
            except (psycopg2.Error, sqlite3.Error) as e:
                logging.error(f"Error has occurred when loaded data: {e}")
