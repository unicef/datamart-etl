import pytest
from faker import Faker
from sqlalchemy import MetaData, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import join

from etl.ddl import adapt_schema
from etl.sql import migrate

fake = Faker()


@pytest.fixture()
def data(database: Engine, destination: Engine) -> (Engine, Engine):
    publicMeta = MetaData()
    publicMeta.reflect(bind=database)
    PublicTable = publicMeta.tables['public_table']

    conn = database.connect()
    ret = conn.execute(PublicTable.insert().values(name="PublicTable1"))
    pk1 = ret.inserted_primary_key[0]
    conn.execute(PublicTable.insert().values(name="PublicTable2"))

    for schema in ['tenant1', 'tenant2']:
        meta = MetaData()
        meta.reflect(bind=database, schema=schema)

        masterTable = meta.tables[f'{schema}.tenant_master_table']
        detailTable = meta.tables[f'{schema}.tenant_detail_table']
        ret = conn.execute(masterTable.insert().values(name=f"{schema}.MasterTable.1.1",
                                                       public_id=pk1))
        pk2 = ret.inserted_primary_key[0]
        conn.execute(masterTable.insert().values(name=f"{schema}.MasterTable.1.1",
                                                 public_id=pk1))

        conn.execute(detailTable.insert().values(name="{meta.schema}.DetailTable1.1.1",
                                                 master_id=pk2))
        conn.execute(detailTable.insert().values(name="{meta.schema}.DetailTable1.1.2",
                                                 master_id=pk2))
    return database, destination


def test_migrate(data):
    database, destination = data

    adapt_schema(database, destination, 'tenant1')

    migrate(database, destination, ['tenant1', 'tenant2'])

    meta = MetaData()
    meta.reflect(bind=destination)

    # PublicTable = meta.tables['public_table']
    TenantMasterTable = meta.tables['tenant_master_table']
    TenantDetailTable = meta.tables['tenant_detail_table']

    j1 = join(TenantDetailTable, TenantMasterTable)
    # j2 = join(TenantMasterTable, PublicTable)

    stm = select([TenantDetailTable.c.name.label('detail_name'),
                  TenantMasterTable.c.name.label('master_name'),
                  TenantDetailTable.c.country_name.label('detail_country_name'),
                  TenantMasterTable.c.country_name.label('master_country_name'),
                  TenantMasterTable.c.public_id.label('public_id'),
                  ]).select_from(j1)
    conn = destination.connect()
    result = conn.execute(stm)
    for row in result:
        # TODO: remove me
        print(111, "test_sql.py:65", dict(row))
