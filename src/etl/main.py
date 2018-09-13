import os

from sqlalchemy import create_engine

from etl.ddl import reset_database, sync_ddl, syncronyze_extensions

if __name__ == '__main__':  # pragma: no cover
    source = create_engine(os.environ['DATABASE_URL_ETOOLS'], echo=False)
    destination = create_engine(os.environ['DATABASE_URL_DATAMART'])

    reset_database(destination)
    syncronyze_extensions(source, destination)
    sync_ddl(source, destination, )
    sync_ddl(source, destination, from_schema="bolivia", to_schema="public")
