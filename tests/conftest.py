import os
import warnings
from copy import copy

import pytest
from faker import Faker
from sqlalchemy import Column, create_engine, ForeignKey, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from etl.utils import drop_database

warnings.simplefilter('ignore', DeprecationWarning, 78)
warnings.simplefilter('ignore', DeprecationWarning, 91)

fake = Faker()


def pytest_configure():
    pass


@pytest.fixture()
def engine():
    return create_engine(os.environ['TEST_DATABASE_URL'], echo=False)


@pytest.fixture()
def dbname() -> str:
    return fake.password(length=20, special_chars=False, digits=False, upper_case=True, lower_case=False)


@pytest.fixture()
def destination(dbname) -> Engine:
    from etl.utils import create_database, drop_database
    base_engine = create_engine(os.environ['TEST_DATABASE_URL'], echo=False)
    url = copy(make_url(base_engine.url))
    url.database = f"destination_{dbname}"
    create_database(url)
    db = create_engine(url)
    yield db
    drop_database(url)


@pytest.fixture()
def database(dbname) -> Engine:
    from etl.utils import create_database

    base_engine = create_engine(os.environ['TEST_DATABASE_URL'], echo=False)

    # Session = sessionmaker(autoflush=False, autocommit=True)
    # Session.configure(bind=base_engine)

    # tenant = fake.password(length=20, special_chars=False, digits=False, upper_case=True, lower_case=False)
    tenant1 = 'tenant1'
    tenant2 = 'tenant2'
    url = copy(make_url(base_engine.url))
    url.database = f"source_{dbname}"
    create_database(url)
    db = create_engine(url)
    try:
        from sqlalchemy.schema import CreateSchema
        db.execute(CreateSchema(tenant1))
        db.execute(CreateSchema(tenant2))

        Base = declarative_base()

        class PublicNoFkTable(Base):
            __tablename__ = 'public_nofk_table'
            __table_args__ = ({"schema": "public"})
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String)

        class PublicTable(Base):
            __tablename__ = 'public_table'
            __table_args__ = ({"schema": "public"})
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String)

        class MasterMixin(object):
            __tablename__ = 'tenant_master_table'
            __table_args__ = ({"schema": tenant1},)

            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String)

            @declared_attr
            def public_id(cls):
                return Column(Integer, ForeignKey("public.public_table.id"), nullable=False)

        class ChildMixin(object):
            __tablename__ = 'tenant_detail_table'
            __table_args__ = ({"schema": tenant1})

            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String)

            @declared_attr
            def master_id(cls):
                schema = cls.__table_args__['schema']
                return Column(Integer, ForeignKey(f"{schema}.tenant_master_table.id",
                                                  ondelete="CASCADE"), nullable=False)

        class Tenant1MasterTable(MasterMixin, Base):
            __table_args__ = ({"schema": tenant1})

        class Tenant1DetailTable(ChildMixin, Base):
            __table_args__ = ({"schema": tenant1})

        class Tenant2MasterTable(MasterMixin, Base):
            __table_args__ = ({"schema": tenant2})

        class Tenant2DetailTable(ChildMixin, Base):
            __table_args__ = ({"schema": tenant2})

        Base.metadata.create_all(db)

    except Exception as e:
        raise e

    yield db
    drop_database(db.url)
