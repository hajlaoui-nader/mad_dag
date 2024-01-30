import sys
from sqlalchemy import create_engine, Table, Column, String, MetaData, select
import pandas as pd


def clean_table(table_name, redshift_conn, file_path):
    ids = pd.read_csv(file_path)["id"].tolist()
    engine = create_engine(redshift_conn)

    if ids:
        # truncate table based on the ids
        with engine.connect() as conn:
            metadata = MetaData()

            # Define a temporary table
            temp_table = Table(
                "temp_ids", metadata, Column("id", String), prefixes=["TEMPORARY"]
            )

            # clean the temp table if exists
            conn.execute(temp_table.delete())
            conn.execute(temp_table.insert(), [{"id": id} for id in ids])
            events = Table(table_name, metadata, autoload_with=engine)
            # delete from events where id in (select id from temp_ids)
            conn.execute(events.delete().where(events.c.id.in_(temp_table.select())))
            conn.execute(temp_table.delete())


if __name__ == "__main__":
    table_name = sys.argv[1]
    redshift_conn = sys.argv[2]
    file_path = sys.argv[3]
    clean_table(table_name, redshift_conn, file_path)
