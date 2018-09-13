from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.automap import automap_base
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
    meta = MetaData(schema=from_schema)
    base = automap_base(metadata=meta)
    base.prepare(source, schema=from_schema, reflect=True)
    for name, table in base.metadata.tables.items():
        table.schema = to_schema
    base.metadata.create_all(destination)
    return base.metadata


def reset_database(engine: Engine):
    drop_database(engine.url)
    create_database(engine.url)


def sync_all(source: Engine, destination: Engine):
    syncronyze_extensions(source, destination)
    return sync_ddl(source, destination)
