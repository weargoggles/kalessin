import typing
import sqlalchemy
import sys
import databases
import os

from databases import Database

metadata = sqlalchemy.MetaData()

database = Database(os.getenv("DATABASE_URL", "postgresql://localhost/foo"))


class SetValidationError(Exception):
    pass


class UnsupportedTypeAnnotations(SetValidationError):
    pass


ALLOWED_COLUMN_TYPES = {int, str}


def validate_set(
    s: typing.Type[typing.NamedTuple]
) -> typing.Optional[SetValidationError]:
    bad_annotations = set(s.__annotations__.values()) - ALLOWED_COLUMN_TYPES
    if bad_annotations:
        return UnsupportedTypeAnnotations(
            "{} contains column types which are not supported: {}".format(
                s.__class__, bad_annotations
            )
        )


def lookup_column_type(t: typing.Type):
    if t == int:
        return sqlalchemy.Integer
    if t == str:
        return sqlalchemy.String
    if t == bool:
        return sqlalchemy.Boolean
    raise Exception


def table_from_tuple(s: typing.Type[typing.NamedTuple]):
    primary_key = getattr(s, "primary_key", [])
    columns = [
        sqlalchemy.Column(
            key, lookup_column_type(value), primary_key=(key in primary_key)
        )
        for key, value in s.__annotations__.items()
    ]
    return sqlalchemy.Table(s.__name__, metadata, *columns)


class SetMeta(typing.NamedTupleMeta):
    """
    The point of this is to have classes with the properties of typing.NamedTuple
    (that is, efficient access + storage in instances of the class) with the added 
    property that a sqlalchemy.Table is derived for the class when it is created.
    """

    def __new__(cls, typename, bases, ns):
        t = super().__new__(cls, typename, bases, ns)
        if ns.get("_root", False):
            return t

        setattr(t, "__table__", table_from_tuple(t))

        # namedtuples only have tuple as a base by default.
        # glue the rest of the bases back on.
        t.__bases__ = bases + t.__bases__

        return t


class Set(metaclass=SetMeta):
    _root = True

    @classmethod
    def get_table(cls):
        return cls.__table__

    @classmethod
    def get_headers(cls):
        return cls.get_table().columns

    @classmethod
    async def select(cls, *args, **kwargs):
        database.is_connected or await database.connect()
        records = await database.fetch_all(cls.get_table().select(*args, **kwargs))
        return [cls.instance_from_record(record) for record in records]

    @classmethod
    async def insert(cls, *args):
        database.is_connected or await database.connect()
        records = args
        query = cls.get_table().insert()
        await database.execute_many(query, records)

    @classmethod
    def instance_from_record(cls, record):
        return cls(**{column.key: record[column.key] for column in cls.get_headers()})

    @classmethod
    def create_table(cls):
        engine = sqlalchemy.create_engine(database._url._url)
        metadata.create_all(bind=engine, tables=[cls.get_table()])

    @classmethod
    def drop_table(cls):
        engine = sqlalchemy.create_engine(database._url._url)
        metadata.drop_all(bind=engine, tables=[cls.get_table()])

