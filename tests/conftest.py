import os
from copy import copy

import pytest
from faker import Faker
from sqlalchemy import Column, create_engine, Integer, String
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from etl.utils import drop_database

fake = Faker()


def pytest_configure():
    pass


@pytest.fixture()
def engine():
    return create_engine(os.environ['TEST_DATABASE_URL'], echo=False)


@pytest.fixture()
def database():
    from etl.utils import create_database

    base_engine = create_engine(os.environ['TEST_DATABASE_URL'], echo=False)

    Session = sessionmaker(autoflush=False, autocommit=True)
    Session.configure(bind=base_engine)

    name = fake.password(length=20, special_chars=False, digits=False, upper_case=True, lower_case=False)
    tenant = fake.password(length=20, special_chars=False, digits=False, upper_case=True, lower_case=False)

    url = copy(make_url(base_engine.url))
    url.database = name
    create_database(url)

    db = create_engine(url)
    from sqlalchemy.schema import CreateSchema
    base_engine.execute(CreateSchema(tenant))

    Public = declarative_base()
    Tenant = declarative_base()

    class PublicTable(Public):
        __tablename__ = 'public_table'
        id = Column(Integer, primary_key=True)
        name = Column(String)

    class TenantTable(Tenant):
        __tablename__ = 'tenant_table'
        schema = tenant

        id = Column(Integer, primary_key=True)
        name = Column(String)

    Public.metadata.create_all(db)
    Tenant.metadata.create_all(db)
    yield db
    drop_database(db.url)
