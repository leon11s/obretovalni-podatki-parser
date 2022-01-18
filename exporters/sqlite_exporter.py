import pandas as pd
from sqlalchemy import create_engine


class SQLiteExporter:
    def __init__(self, database_path: str) -> None:
        self.database_path = database_path
        self.engine = create_engine(f"sqlite:///{self.database_path}")

    def add_df_to_table(self, data: pd.DataFrame, table_name: str) -> None:
        data.to_sql(name=table_name, con=self.engine, if_exists="append", index=False, chunksize=10_000)
