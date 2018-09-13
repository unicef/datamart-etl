import geoalchemy2  # noqa: F401
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import Column, MetaData, String
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from src.etl.utils import create_database, drop_database


def syncronyze_extensions(source: Engine, destination: Engine):
    connFrom = source.connect()
    cur = connFrom.execute("SELECT * from pg_extension;")
    extensions = [line[0] for line in cur.fetchall()]
    extensions += ['btree_gin']
    connFrom.close()

    connTo = destination.connect()
    connTo.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    for ext in extensions:
        try:
            connTo.execute("CREATE EXTENSION %s;" % ext)
            print("Creating extension %s" % ext)
        except ProgrammingError as e:  # pragma: no cover
            if 'already exists' not in str(e):
                raise
    connTo.close()
    return extensions


def sync_ddl(source: Engine, destination: Engine, from_schema="public", to_schema="public"):
    sourceMeta = MetaData()
    sourceMeta.reflect(source, schema=from_schema)
    destMeta = MetaData(destination, schema=to_schema)

    for name, table in sourceMeta.tables.items():
        table.tometadata(destMeta)
    destMeta.create_all()
    return destMeta


def add_tenant(source: Engine, destination: Engine, tenant="bolivia"):
    sourceMeta = MetaData()
    sourceMeta.reflect(bind=source, schema=tenant)

    for name, table in sourceMeta.tables.items():
        table.schema = "public"
        columns = list(table.columns)
        col = Column('country_name', String, default='public')
        columns.append(col)
        table.columns = columns

    sourceMeta.create_all(bind=destination)
    m = MetaData()
    m.reflect(bind=destination)
    return m


def reset_database(engine: Engine):
    drop_database(engine.url)
    create_database(engine.url)


def sync_all(source: Engine, destination: Engine):
    syncronyze_extensions(source, destination)
    return sync_ddl(source, destination)
