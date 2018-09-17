import geoalchemy2  # noqa: F401
from django_regex.utils import RegexList
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import BOOLEAN, Column, Integer, MetaData, PrimaryKeyConstraint, String
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import ColumnCollection
from sqlalchemy.sql.ddl import AddConstraint, CreateTable
from src.etl.utils import create_database, drop_database, get_all_tables, get_schema_fieldname

from etl.timeit import Timer

IGNORED_TABLES = RegexList([
    'public.spatial_ref_sys',
    '.*\.reversion_.*'
])


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


def adapt_table(table):
    pass


def sync_public_schema(source: Engine, destination: Engine, echo=None):
    tables = get_all_tables(source, tenant=None, public=True)
    destMeta = MetaData(destination, schema="public")
    processed = []

    if echo:
        echo.write("Copying public schema\n")
    with Timer("Public schema copied in {humanized}\n") as t:

        for table, deps in tables:
            if table is None:
                continue
            if table.name in processed:
                continue
            fqn = f"{table.schema or 'public'}.{table.name}"
            if fqn in processed or fqn in IGNORED_TABLES:
                continue
            try:
                table.tometadata(destMeta)
            except Exception as e:
                raise Exception(f"Error processing {table.name}") from e

        destMeta.create_all()
        if echo:
            echo.write(t.echo())
    return processed


def adapt_schema(source: Engine, destination: Engine, tenant="bolivia", echo=None):  # noqa: C901
    tables = get_all_tables(source, tenant, public=False)
    destMeta = MetaData(destination, schema="public")

    # pk = Column('id', Integer, primary_key=True)
    fks = []
    processed = []
    if echo:
        write = echo.write
    else:
        write = lambda x: True

    constraints = []

    write(f"Start adapting schema {tenant}\n")

    for table, deps in tables:
        if table is None:
            continue
        if table.name in processed:
            continue
        # if table.name not in ['audit_spotcheck', 'audit_engagement']:
        #     continue

        if table.schema and table.schema != 'public':
            write(".")
            table.schema = "public"
            new_columns = ColumnCollection()
            primary_key = {c for c in table.constraints if isinstance(c, PrimaryKeyConstraint)}
            table.constraints = {c for c in table.constraints if not isinstance(c, PrimaryKeyConstraint)}
            # remove self relation
            # to_remove = []
            # for constraint in table.constraints:
            #     if isinstance(constraint, ForeignKeyConstraint) and constraint.referred_table == table:
            #         self_relations.append({'columns': constraint.column_keys,
            #                                'table': table.name,
            #                                'constraint': constraint}
            #                               )
            #         to_remove.append(constraint)
            # table.constraints = table.constraints.difference(to_remove)
            # if table.name == 'audit_spotcheck':
            #     import pdb; pdb.set_trace()
            for col in table.columns:
                if col.primary_key:
                    _seq = Column('pk', Integer, autoincrement=True, unique=True)
                    pk = Column(col.name, Integer, primary_key=True)
                    country_name = Column('country_name', String, default='public', primary_key=True)

                    new_columns.add(_seq)
                    new_columns.add(pk)
                    new_columns.add(country_name)

                    table.primary_key.columns.add(country_name)
                    if col.foreign_keys:
                        fk = [c for c in col.foreign_keys][0]
                        table.constraints.remove(fk.constraint)
                        fks.append({'columns': (pk.name, country_name.name),
                                    'refcolumns': fk.constraint.referred_table.primary_key.columns.keys(),
                                    'source': table,
                                    'target': fk.constraint.referred_table}
                                   )

                elif col.foreign_keys:
                    fk = [c for c in col.foreign_keys][0]
                    if fk.constraint.referred_table.schema:
                        _fk = Column(col.name, Integer, nullable=col.nullable)
                        _country_name = Column(get_schema_fieldname(col),  # f'{target}_country_name',
                                               String,
                                               nullable=col.nullable)
                        table.constraints.remove(fk.constraint)
                        new_columns.add(_fk)
                        new_columns.add(_country_name)
                        fks.append({'columns': (_fk.name, _country_name.name),
                                    'refcolumns': fk.constraint.referred_table.primary_key.columns.keys(),
                                    'source': table,
                                    'target': fk.constraint.referred_table}
                                   )
                    else:
                        new_columns.add(col)
                else:
                    new_columns.add(col)
            table.columns = new_columns
            constraints.extend(table.constraints)
            table.constraints = primary_key
        else:
            pass
        processed.append(table.name)
        try:
            table.tometadata(destMeta)
        except Exception as e:
            raise Exception(f"Error processing {table.name}") from e

    destMeta.create_all()

    write("\nCreating foreign keys constraints\n")
    for entry in fks:
        clause = f"ALTER TABLE {entry['source'].name} \
ADD CONSTRAINT FK_{entry['target'].name} \
FOREIGN KEY({','.join(entry['columns'])}) \
REFERENCES {entry['target'].name}({','.join(entry['refcolumns'])})"
        try:
            destination.execute(str(clause))
        except Exception as e:
            print(str(e))
            print(CreateTable(entry['source']))
            print(clause)
            raise Exception(clause) from e

    write("Creating remaining constraints\n")
    for constraint in constraints:
        clause = AddConstraint(constraint)
        # if type(constraint.sqltext) is BinaryExpression:
        #     continue
        try:
            destination.execute(str(clause))
        except Exception as e:
            if isinstance(clause.element.columns.values()[0].type, BOOLEAN):
                pass
            else:
                print(e)
                print(CreateTable(constraint.table))
                print(clause)
                raise Exception(clause) from e

    # write("\nCreating self-relations constraint\n")
    # for self_relation in self_relations:
    #     constraint = self_relation['constraint']
    #     clause = f"ALTER TABLE {constraint.table.name} " \
    #              f"ADD CONSTRAINT self_relation_{constraint.table.name} " \
    #              f"FOREIGN KEY({constraint.column_keys[0]}) " \
    #              f"REFERENCES {constraint.table.name}(pk)"
    #     # TODO: remove me
    #     print(111, "ddl.py:167", clause)
    #     destination.execute(clause)
    #
    return tables


def reset_database(engine: Engine):
    drop_database(engine.url)
    create_database(engine.url)


def sync_all(source: Engine, destination: Engine):
    syncronyze_extensions(source, destination)
    return sync_ddl(source, destination)
