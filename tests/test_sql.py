import pytest
from faker import Faker
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

from etl.sql import migrate

fake = Faker()


@pytest.fixture()
def data(database: Engine, destination: Engine) -> (Engine, Engine):
    publicMeta = MetaData()
    publicMeta.reflect(bind=database)
    tenantMeta = MetaData()
    tenantMeta.reflect(bind=database, schema='tenant1')

    PublicTable = publicMeta.tables['public_table']
    TenantMasterTable = tenantMeta.tables['tenant1.tenant_master_table']
    TenantDetailTable = tenantMeta.tables['tenant1.tenant_detail_table']
    # ins = PublicTable.insert().values(name='aaaaaa')
    # ins.bind = database
    conn = database.connect()
    ret = conn.execute(PublicTable.insert().values(name="PublicTable1"))
    pk1 = ret.inserted_primary_key[0]
    conn.execute(PublicTable.insert().values(name="PublicTable2"))

    ret = conn.execute(TenantMasterTable.insert().values(name="TenantMasterTable1.1",
                                                         public_id=pk1))
    pk2 = ret.inserted_primary_key[0]
    conn.execute(TenantMasterTable.insert().values(name="TenantMasterTable1.2",
                                                   public_id=pk1))

    conn.execute(TenantDetailTable.insert().values(name="TenantDetailTable1.1.1",
                                                   master_id=pk2))
    conn.execute(TenantDetailTable.insert().values(name="TenantDetailTable1.1.2",
                                                   master_id=pk2))
    return database, destination


def test_migrate(data):
    database, destination = data
    migrate(database, destination, 'tenant1')
    pytest.fail()
