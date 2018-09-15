import os

from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.sql.ddl import sort_tables_and_constraints

if __name__ == '__main__':  # pragma: no cover
    source = create_engine(os.environ['DATABASE_URL_ETOOLS'], echo=False)
    destination = create_engine(os.environ['DATABASE_URL_DATAMART'])

    # reset_database(destination)
    # syncronyze_extensions(source, destination)
    # sync_ddl(source, destination)
    # add_tenant(source, destination)
    # migrate(source, destination, 'public')
    metadata = MetaData()
    inspector = inspect(source)
    metadata.reflect(bind=source, schema='bolivia')

    tables = sort_tables_and_constraints(metadata.tables.values())
    for tablename, deps in tables:
        # TODO: remove me
        print(111, "main.py:23", tablename, deps)
