import sys
from sqlalchemy import create_engine, Table, MetaData
import pandas as pd


def insert(table_name, redshift_conn, file_path):
    df = pd.read_csv(file_path)

    engine = create_engine(redshift_conn)

    # truncate table based on the ids
    with engine.connect() as conn:
        metadata = MetaData()
        events = Table(table_name, metadata, autoload_with=engine)
        conn.execute(events.insert(), df.to_dict("records"))


if __name__ == "__main__":
    table_name = sys.argv[1]
    redshift_conn = sys.argv[2]
    file_path = sys.argv[3]
    insert(table_name, redshift_conn, file_path)
