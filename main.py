import sqlite3
from loaders.meritve_csv_loader import LoadMeritveFromCsv
from exporters.sqlite_exporter import SQLiteExporter

sqlite_exporter = SQLiteExporter(database_path="data/obratovalni_podatki.db")

# list file
files = ["data/davr15m_1.csv", "data/davr15m_2.csv"]


for file in files:
    csv_loader = LoadMeritveFromCsv(file)
    print(f"Loading file {csv_loader.file_path}")
    data = csv_loader.load()
    sqlite_exporter.add_df_to_table(data, "meritve")
    print(f"Added {data.shape[0]} rows to DB.")
    print("-------------")
