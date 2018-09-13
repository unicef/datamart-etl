from copy import copy

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import ProgrammingError
from sqlalchemy_utils.functions import quote


def create_database(url, encoding='utf8', template='template1', ignore_exists=True):
    url = copy(make_url(url))

    database = url.database

    url.database = 'postgres'
    engine = sa.create_engine(url)

    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    engine.raw_connection().set_isolation_level(
        ISOLATION_LEVEL_AUTOCOMMIT
    )

    text = "CREATE DATABASE {0} ENCODING '{1}' TEMPLATE {2}".format(
        quote(engine, database),
        encoding,
        quote(engine, template)
    )
    try:
        result_proxy = engine.execute(text)
        result_proxy.close()
    except ProgrammingError as e:  # pragma: no cover
        if 'already exists' not in str(e) or not ignore_exists:
            raise
        elif ignore_exists:
            pass

    engine.dispose()
    return database


def drop_database(url):
    """Issue the appropriate DROP DATABASE statement.

    :param url: A SQLAlchemy engine URL.

    Works similar to the :ref:`create_database` method in that both url text
    and a constructed url are accepted. ::

        drop_database('postgres://postgres@localhost/name')
        drop_database(engine.url)

    """

    url = copy(make_url(url))

    database = url.database
    url.database = 'postgres'
    engine = sa.create_engine(url)

    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    connection = engine.connect()
    connection.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    # Disconnect all users from the database we are dropping.
    version = connection.dialect.server_version_info
    pid_column = (
        'pid' if (version >= (9, 2)) else 'procpid'
    )
    text = '''
    SELECT pg_terminate_backend(pg_stat_activity.%(pid_column)s)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = '%(database)s'
      AND %(pid_column)s <> pg_backend_pid();
    ''' % {'pid_column': pid_column, 'database': database}
    connection.execute(text)

    # Drop the database.
    text = 'DROP DATABASE IF EXISTS {0} '.format(quote(connection, database))
    connection.execute(text)
    # conn_resource = connection

    # if conn_resource is not None:
    connection.close()
    engine.dispose()
