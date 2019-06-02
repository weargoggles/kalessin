import asyncio
import functools
import os
import pytest
import sqlalchemy
import sys
import typing


import kalessin.sets
from databases import DatabaseURL

metadata = sqlalchemy.MetaData()

notes = sqlalchemy.Table(
    "notes", metadata, sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True)
)


class SetWithAllowedTypes(typing.NamedTuple):
    id: int
    foo: str


class SetWithDisallowedType(typing.NamedTuple):
    id: int
    foo: float


class NonNamedTupleWithAnnotations:
    foo: int


def test_non_named_tuples_have_annotations_special_member():
    assert len(NonNamedTupleWithAnnotations.__annotations__) == 1


def test_table_of_set():
    assert not kalessin.sets.validate_set(SetWithAllowedTypes)


def test_table_of_set_with_disallowed_types():
    err = kalessin.sets.validate_set(SetWithDisallowedType)
    assert err
    assert isinstance(err, kalessin.sets.UnsupportedTypeAnnotations)


def test_table_from_tuple():
    table = kalessin.sets.table_from_tuple(SetWithAllowedTypes)
    assert table.name == SetWithAllowedTypes.__name__
    assert len(table.columns) == 2


assert "TEST_DATABASE_URLS" in os.environ, "TEST_DATABASE_URLS is not set."

DATABASE_URLS = [url.strip() for url in os.environ["TEST_DATABASE_URLS"].split(",")]


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    # Create test databases
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.dialect == "mysql":
            url = str(database_url.replace(driver="pymysql"))
        engine = sqlalchemy.create_engine(url)
        metadata.create_all(engine)

    # Run the test suite
    yield

    # Drop test databases
    for url in DATABASE_URLS:
        database_url = DatabaseURL(url)
        if database_url.dialect == "mysql":
            url = str(database_url.replace(driver="pymysql"))
        engine = sqlalchemy.create_engine(url)
        metadata.drop_all(engine)


def async_adapter(wrapped_func):
    """
    Decorator used to run async test cases.
    """

    @functools.wraps(wrapped_func)
    def run_sync(*args, **kwargs):
        loop = asyncio.get_event_loop()
        task = wrapped_func(*args, **kwargs)
        return loop.run_until_complete(task)

    return run_sync


@async_adapter
async def test_new_set():
    class TestSet(kalessin.sets.Set):
        foo: int
        bar: str
        quux: int

        primary_key = ("foo", "quux")

    assert len(TestSet.get_table().columns) == 3
    assert TestSet.create_table() is None
    assert await TestSet.select() == []
    await TestSet.insert(
        TestSet(foo=1, bar="hi", quux=2), TestSet(foo=2, bar="hi", quux=3)
    )
    assert await TestSet.select() == [
        TestSet(foo=1, bar="hi", quux=2),
        TestSet(foo=2, bar="hi", quux=3),
    ]
    assert TestSet.drop_table() is None


def test_set_slots():
    class TestSlotSet(kalessin.sets.Set):
        foo: int
    
    assert TestSlotSet.__slots__ == ()
    a = TestSlotSet(foo=1)
    
    assert getattr(a, 'foo') == 1, "we can read a Set subclass instance's defined members"

    assert getattr(a, '__dict__', None) is None, "instances of Set subclasses should have no instance dictionary"

    with pytest.raises(AttributeError) as excinfo:
        setattr(a, 'bar', 2)

    assert 'bar' in str(excinfo.value), "should not be able to add attribute to Set subclass instance"
