from uuid import UUID
from dataclasses import dataclass
from typing import Optional, Union
from datetime import date


@dataclass(frozen=True)
class UUIDMixin:
    id: UUID


@dataclass(frozen=True)
class Filmwork(UUIDMixin):
    title: str
    description: Optional[str]
    creation_date: date
    file_path: Optional[str]
    rating: Optional[float]
    type: str


@dataclass(frozen=True)
class Person(UUIDMixin):
    full_name: str


@dataclass(frozen=True)
class Genre(UUIDMixin):
    name: str
    description: Optional[str]


@dataclass(frozen=True)
class GenreFilmwork(UUIDMixin):
    genre_id: str
    film_work_id: Optional[str]


@dataclass(frozen=True)
class PersonFilmwork(UUIDMixin):
    person_id: str
    film_work_id: Optional[str]
    role: str


TableDataClass = Union[Filmwork, Person, Genre, GenreFilmwork, PersonFilmwork]


@dataclass(frozen=True)
class Table:
    name: str
    dataclass: TableDataClass
