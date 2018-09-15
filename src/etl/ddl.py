import geoalchemy2  # noqa: F401
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import Column, Integer, MetaData, PrimaryKeyConstraint, String
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import ColumnCollection
from sqlalchemy.sql.ddl import sort_tables_and_constraints
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


def move_schema(source: Engine, destination: Engine, tenant="bolivia"):
    publicMeta = MetaData()
    publicMeta.reflect(bind=source)
    tenantMeta = MetaData()
    tenantMeta.reflect(bind=source, schema=tenant)

    destMeta = MetaData(destination, schema="public")

    tables = sort_tables_and_constraints(tenantMeta.tables.values())

    pk = Column('id', Integer, primary_key=True)
    country_name = Column('country_name', String, default='public', primary_key=True)
    fks = []

    for table, deps in tables:
        if table is None:
            continue

        if table.schema:
            table.schema = "public"
            new_columns = ColumnCollection(pk, country_name)
            table.constraints = {c for c in table.constraints if not isinstance(c, PrimaryKeyConstraint)}
            for col in table.columns:
                if col.primary_key:
                    pass
                elif col.foreign_keys:
                    # search constraint related this foreign_key
                    constraints = [c for c in table.constraints if c.columns.keys() == [col.name]]
                    assert len(constraints) == 1
                    constraint = constraints[0]
                    # only create multi-column key if fk to tenant table
                    if constraint.referred_table.schema:
                        target, __ = col.name.split('_')
                        # removes old constraint
                        table.constraints.remove(constraint)
                        # prepare new multi column foreign key
                        _fk = Column(f'{target}_id', Integer,
                                     )
                        _country_name = Column(f'{target}_country_name',
                                               String,
                                               nullable=False)

                        new_columns.add(_fk)
                        new_columns.add(_country_name)
                        fks.append({'columns': (_fk.name, _country_name.name),
                                    'refcolumns': ('id', 'country_name'),
                                    'source': table,
                                    'target': constraint.referred_table, }
                                   )
                    else:
                        new_columns.add(col)
                else:
                    new_columns.add(col)
            table.columns = new_columns
        else:
            pass
        table.tometadata(destMeta)

    destMeta.create_all()

    for entry in fks:
        clause = f"ALTER TABLE {entry['source'].name} \
    ADD CONSTRAINT FK_{entry['target'].name} \
    FOREIGN KEY({','.join(entry['columns'])}) \
    REFERENCES {entry['target'].name}({','.join(entry['refcolumns'])})"
        destination.execute(clause)


def reset_database(engine: Engine):
    drop_database(engine.url)
    create_database(engine.url)


def sync_all(source: Engine, destination: Engine):
    syncronyze_extensions(source, destination)
    return sync_ddl(source, destination)
