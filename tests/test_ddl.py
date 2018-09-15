from copy import copy

from faker import Faker
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

from etl.ddl import move_schema, reset_database, sync_all, sync_ddl, syncronyze_extensions
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


def test_make_single_tenant(database: Engine, destination: Engine):
    # syncronyze public
    # sync_ddl(database, destination)
    move_schema(database, destination, 'tenant1')

    public = MetaData()
    public.reflect(bind=database)
    tenant = MetaData()
    tenant.reflect(bind=database, schema='tenant1')
    # public_tables = set(public.tables)
    # tenant_tables = set(tenant.tables)
    total_tables = set(list(tenant.tables) + list(public.tables))

    check = MetaData()
    check.reflect(bind=destination)
    final_tables = set(check.tables)
    assert len(total_tables) == len(final_tables)
