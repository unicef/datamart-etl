from django_regex.utils import RegexList
from sqlalchemy import ForeignKeyConstraint, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

from etl.timeit import Timer
from etl.utils import get_all_tables, get_schema_fieldname

IGNORED_TABLES = RegexList([
    '.*\.auth_user_groups',
    '.*\.auth_user_user_permissions',
    '.*\.users_userprofile',
    '.*\.users_userprofile_countries_available',
    'public\.account_.*',
    'public\.authtoken_token',
    'public\.celery_.*',
    'public\.corsheaders_.*',
    'public\.django_admin_log',
    'public\.django_celery_.*',
    '.*\.django_migrations',
    'public\.django_session',
    'public\.djcelery_.*',
    'public\.environment_.*',
    'public\.notification_notification',
    'public\.post_office_.*',
    'public\.reversion_.*',
    'public\.socialaccount_.*',
    'public\.spatial_ref_sys',
    'public\.unicef_notification_.*',
    'public\.waffle_.*',
])


def migrate_public(source: Engine, destination: Engine, echo=None):
    DestinationSession = sessionmaker(destination)
    DestinationSession.configure(bind=destination, autoflush=False, expire_on_commit=False)

    destinationSession = DestinationSession()

    destMeta = MetaData()
    destMeta.reflect(bind=destination)
    conn = source.connect()
    processed = []
    tables = get_all_tables(source, tenant=None, public=True)
    with Timer() as t:
        for table, deps in tables:
            if table is None:
                continue
            fqn = f"{table.schema or 'public'}.{table.name}"
            if fqn in processed or fqn in IGNORED_TABLES:
                continue
            processed.append(fqn)
            echo.write(table.name)
            destTable = destMeta.tables[table.name]
            s = select([table])
            result = conn.execute(s)
            values = [dict(row) for row in result]
            ins = destTable.insert()
            if result.rowcount:
                destinationSession.execute(ins, values)

            echo.write("\r{table:40} {elapsed} {records:-10} records\n".format(
                table=table.name,
                elapsed=t.fmt(t.step),
                records=result.rowcount
            ))
        destinationSession.commit()
    if echo:
        echo.write(f'schema `public` data migration completed in {t.humanized}\n')
    return processed


def migrate_schema(source: Engine, destination: Engine, tenant: str, echo=None):
    destMeta = MetaData()
    destMeta.reflect(bind=destination)
    processed = []
    if processed is None:
        processed = []
    conn = source.connect()
    conn2 = destination.connect()
    tables = get_all_tables(source, tenant, public=False)
    if echo:
        write = echo.write
    else:
        write = lambda x: True

    write(f"Starting migration of schema {tenant}\n")
    with Timer("{table:40} {humanized} {records:-10} records") as t:
        for table, deps in tables:
            if table is None:
                continue
            if table.schema is None or table.schema == 'public':
                write("\r{table:40} skipped\n".format(table=table.name))
                continue
            fqn = f"{table.schema}.{table.name}"
            if fqn in processed or fqn in IGNORED_TABLES:
                write("\r{table:40} skipped\n".format(table=table.name))
                continue
            processed.append(fqn)
            write(fqn)
            destTable = destMeta.tables[table.name]
            fks = [fk for fk in table.constraints if isinstance(fk, ForeignKeyConstraint)]
            schema_info = {}
            for fk in fks:
                if fk.referred_table.schema:
                    # in django only one column FKs are supported
                    referring_col = fk.columns[fk.columns.keys()[0]]
                    if referring_col.table.schema:
                        schema_info = {get_schema_fieldname(referring_col): table.schema}
            s = select([table])
            result = conn.execute(s)
            for row in result:
                values = dict(row)
                values.update(schema_info)
                if table.schema:
                    values['country_name'] = table.schema
                ins = destTable.insert().values(values)
                try:
                    conn2.execute(ins, values)
                except Exception as e:
                    # TODO: remove me
                    print(111, "sql.py:126", values)
                    raise e
            write("\r{table:40} {elapsed} {records:-10} records\n".format(
                table=table.name,
                elapsed=t.fmt(t.step),
                records=result.rowcount
            ))
    if echo:
        echo.write(f'schema `{tenant}` data migration completed in {t.humanized}\n')

    return processed
