from dataclasses import dataclass, fields, astuple
from typing import List, Iterator
from uuid import UUID

from .models import TableDataClass

DataChunk = List[TableDataClass]
_CHUNK_SIZE = 100


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
