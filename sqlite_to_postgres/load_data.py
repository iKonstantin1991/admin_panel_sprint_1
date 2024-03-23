import os
from typing import Iterator, Optional, List, Union
from dataclasses import dataclass, astuple, fields
from contextlib import contextmanager
import logging
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(format="[%(asctime)s] [%(levelname)s] %(message)s", level=logging.INFO)

_CHUNK_SIZE = 100


@dataclass(frozen=True)
class Filmwork:
    id: str
    title: str
    description: Optional[str]
    creation_date: str
    file_path: Optional[str]
    rating: Optional[float]
    type: str
    created: Optional[str]
    modified: Optional[str]


@dataclass(frozen=True)
class Person:
    id: str
    full_name: str
    created: Optional[str]
    modified: Optional[str]


@dataclass(frozen=True)
class Genre:
    id: str
    name: str
    description: Optional[str]
    created: Optional[str]
    modified: Optional[str]


@dataclass(frozen=True)
class GenreFilmwork:
    id: str
    genre_id: str
    film_work_id: Optional[str]
    created: Optional[str]


@dataclass(frozen=True)
class PersonFilmwork:
    id: str
    person_id: str
    film_work_id: Optional[str]
    role: str
    created: Optional[str]


TableDataClass = Union[Filmwork, Person, Genre, GenreFilmwork, PersonFilmwork]
DataChunk = List[TableDataClass]


@dataclass(frozen=True)
class Table:
    name: str
    dataclass: TableDataClass


class SQLiteExtractor:
    def __init__(self, conn) -> None:
        self._curs = conn.cursor()

    def extract_from_table(self, table: Table) -> Iterator[DataChunk]:
        offset = 0
        while True:
            self._curs.execute(f"SELECT * FROM {table.name} ORDER BY id LIMIT {_CHUNK_SIZE} OFFSET {offset};")
            data = []
            for row in self._curs.fetchall():
                row = dict(row)
                row["created"] = row.pop("created_at", None)
                if "modified" in [field.name for field in fields(table.dataclass)]:
                    row["modified"] = row.pop("updated_at", None)
                data.append(table.dataclass(**row))
            if not data:
                break
            yield data
            offset += _CHUNK_SIZE


class PostgresLoader:
    def __init__(self, conn):
        self._curs = conn.cursor()

    def load_to_table(self, table: Table, data_chunk: DataChunk) -> None:
        column_names = [field.name for field in fields(table.dataclass)]
        col_count = ", ".join(["%s"] * len(column_names))
        args = ",".join(self._curs.mogrify(f"({col_count})", astuple(item)).decode() for item in data_chunk)
        self._curs.execute(f"""
            INSERT INTO content.{table.name} ({", ".join(column_names)})
            VALUES {args}
            ON CONFLICT DO NOTHING
        """)

    def truncate_table(self, table: Table) -> None:
        self._curs.execute(f"TRUNCATE content.{table.name} CASCADE")


def load_from_sqlite(sqlite_conn: sqlite3.Connection, pg_conn: _connection) -> None:
    """Base method for loading data from SQLite to Postgres"""
    sqlite_extractor = SQLiteExtractor(sqlite_conn)
    postgres_loader = PostgresLoader(pg_conn)

    tables = (
        Table("film_work", Filmwork),
        Table("person", Person),
        Table("genre", Genre),
        Table("genre_film_work", GenreFilmwork),
        Table("person_film_work", PersonFilmwork),
    )
    for table in tables:
        logging.info(f"Starting loading data for table: {table.name}")
        for data_chunk in sqlite_extractor.extract_from_table(table):
            postgres_loader.load_to_table(table, data_chunk)


@contextmanager
def _sqlite_conn(db_path: str):
    conn = sqlite3.connect(db_path)
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
    with _sqlite_conn(os.environ.get("SQLITE_PATH")) as sqlite_conn:
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            logging.info("Starting loading data")
            try:
                load_from_sqlite(sqlite_conn, pg_conn)
            except (psycopg2.Error, sqlite3.Error) as e:
                logging.error(f"Failed to load data: {e}")
