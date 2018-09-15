from sqlalchemy.engine import Engine


def migrate(source: Engine, destination: Engine, tenant="bolivia"):
    pass
    # SourceSession = sessionmaker(source)
    # sourceSession = SourceSession()
    #
    # DestSession = sessionmaker(destination)
    # destSession = DestSession()
    #
    # # sourceMeta= MetaData(bind=source, schema=tenant)
    # sourceMeta = MetaData()
    # sourceMeta.reflect(source, schema=tenant)
    # # sourceBase = automap_base(metadata=sourceMeta)
    # # sourceBase.prepare(source, schema=tenant, reflect=True)
    #
    # destMeta = MetaData(bind=destination)
    # destBase = automap_base(metadata=destMeta)
    # destBase.prepare(destination, schema="public", reflect=True)
    #
    # for name, table in sourceMeta.tables.items():
    #     if tenant in name:
    #         name = name.replace(f"{tenant}.", f"public.")
    #     else:
    #         name = f"public.{name}"
    #     # table.tometadata(destMeta, schema='public')
    #     destTable = destBase.metadata.tables[name]
    #     query = sourceSession.query(table)
    #     for row in query:
    #         clause = destTable.insert(row)
    #         # TODO: remove me
    #         print(111, "sql.py:32", clause)
    #         destSession.execute(clause)
    #         # destSession.merge(table.insert(row))
