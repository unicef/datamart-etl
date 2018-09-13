from copy import copy

from faker import Faker
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

from etl.ddl import add_tenant, reset_database, sync_all, sync_ddl, syncronyze_extensions
from etl.utils import create_database, drop_database

fake = Faker()


def test_create_database(engine):
    url = copy(engine.url)
    url.database = 'abcd'
    assert create_database(url) == 'abcd'
    drop_database(url)


def test_syncronyze_extensions(database, destination):
    assert syncronyze_extensions(database, destination)


def test_sync_ddl(database, destination):
    assert sync_ddl(database, destination)
    destMeta = MetaData(bind=database, reflect=True)
    sourceMeta = MetaData(bind=destination, reflect=True)
    assert [t.name for t in destMeta.sorted_tables] == [t.name for t in sourceMeta.sorted_tables]


def test_reset_database(database):
    reset_database(database)


def test_sync_all(database, destination):
    assert sync_all(database, destination)


def test_add_tenant(database: Engine, destination: Engine):
    metadata = add_tenant(database, destination, 'tenant')

    assert len(metadata.tables) == 1
    assert 'tenant_table' in metadata.tables
    table = metadata.tables['tenant_table']
    assert 'country_name' in table.columns.keys(), 'country_name columns ahs not been added'
