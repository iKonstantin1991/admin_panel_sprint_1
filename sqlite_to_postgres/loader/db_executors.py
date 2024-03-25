from dataclasses import fields, astuple
from typing import List, Iterator
from uuid import UUID
import psycopg2

from .models import TableDataClass, Table

DataChunk = List[TableDataClass]
_CHUNK_SIZE = 100


class SQLiteExtractor:
    def __init__(self, conn) -> None:
        self._curs = conn.cursor()

    def extract_from_table(self, table: Table) -> Iterator[DataChunk]:
        offset = 0
        while True:
            self._curs.execute(f"""
                SELECT {", ".join([field.name for field in fields(table.dataclass)])}
                FROM {table.name}
                ORDER BY id
                LIMIT {_CHUNK_SIZE}
                OFFSET {offset}
            """)
            data = []
            for row in self._curs.fetchall():
                row = dict(row)
                row["id"] = UUID(row["id"])
                data.append(table.dataclass(**row))
            if not data:
                break
            yield data
            offset += _CHUNK_SIZE


class PostgresLoader:
    def __init__(self, conn):
        self._curs = conn.cursor()

    def load_to_table(self, table: Table, data_chunk: DataChunk) -> None:
        column_names = ", ".join([field.name for field in fields(table.dataclass)])
        data = [astuple(item) for item in data_chunk]
        insert_query = f"""
            INSERT INTO content.{table.name} ({column_names})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        psycopg2.extras.execute_values(self._curs, insert_query, data, page_size=_CHUNK_SIZE)

    def truncate_table(self, table: Table) -> None:
        self._curs.execute(f"TRUNCATE content.{table.name} CASCADE")
