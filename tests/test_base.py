from copy import copy

from faker import Faker

from etl.ddl import reset_database, sync_all, sync_ddl, syncronyze_extensions
from etl.utils import create_database, drop_database

fake = Faker()


def test_create_database(engine):
    url = copy(engine.url)
    url.database = 'abcd'
    assert create_database(url) == 'abcd'
    drop_database(url)


def test_syncronyze_extensions(database):
    destination = copy(database)
    destination.database = fake.password(length=20, special_chars=False, digits=False, upper_case=True,
                                         lower_case=False)

    assert syncronyze_extensions(database, destination)


def test_sync_ddl(database):
    destination = copy(database)
    destination.database = fake.password(length=20, special_chars=False, digits=False, upper_case=True,
                                         lower_case=False)

    assert sync_ddl(database, destination)


def test_reset_database(database):
    reset_database(database)


def test_sync_all(database):
    destination = copy(database)
    destination.database = fake.password(length=20, special_chars=False, digits=False, upper_case=True,
                                         lower_case=False)

    assert sync_all(database, destination)
