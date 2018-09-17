# coding=utf-8
import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from etl.ddl import adapt_schema, sync_public_schema, syncronyze_extensions
from etl.utils import create_database, drop_database


class Echo:
    @staticmethod
    def write(*args):
        sys.stdout.write("".join(args))
        sys.stdout.flush()


if __name__ == '__main__':
    source = create_engine(os.environ['DATABASE_URL_ETOOLS'], echo=False)
    destination = create_engine(os.environ['DATABASE_URL_DATAMART'], echo=False)

    DBSession = scoped_session(sessionmaker())
    DBSession.remove()
    DBSession.configure(bind=source, autoflush=False, expire_on_commit=False)
    DBSession.configure(bind=destination, autoflush=False, expire_on_commit=False)

    drop_database(destination.url)
    create_database(destination.url)

    syncronyze_extensions(source, destination)

    sync_public_schema(source, destination, echo=Echo)
    adapt_schema(source, destination, 'bolivia', echo=Echo)

    # migrate_public(source, destination, echo=Echo)
    # migrate_schema(source, destination, 'bolivia', echo=Echo)

    # DBSession.commit()

    # migrate(source, destination, ['bolivia', 'chad'], echo=Echo)
