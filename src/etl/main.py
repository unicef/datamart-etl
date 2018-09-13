import os

from sqlalchemy import create_engine

from etl.ddl import add_tenant, reset_database, sync_ddl, syncronyze_extensions
from etl.sql import migrate

if __name__ == '__main__':  # pragma: no cover
    source = create_engine(os.environ['DATABASE_URL_ETOOLS'], echo=False)
    destination = create_engine(os.environ['DATABASE_URL_DATAMART'])

    reset_database(destination)
    syncronyze_extensions(source, destination)
    sync_ddl(source, destination)
    add_tenant(source, destination)
    migrate(source, destination, 'public')
