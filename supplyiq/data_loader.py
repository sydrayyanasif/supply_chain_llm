"""data_loader.py


Load tables from SQLite into pandas DataFrames.
"""
import sqlite3
# Python ka sqlite3 module import hota hai,
# jisse tum SQLite database ke saath interact kar sakte ho
# (read/write/query).
import pandas as pd

class DataManager:
    def __init__(self, db_path: str = "supply_chain_data.db"):
# Jab bhi tum DataManager ka object banaoge,
# tumhe database ka path pass kar sakte ho (by default: "supply_chain_data.db")
        self.db_path = db_path

    def get_sheet(self, sheet_name: str) -> pd.DataFrame:
# Yeh function koi bhi sheet/table ka naam lega
# aur usko DataFrame me return karega.
        conn = sqlite3.connect(self.db_path)
        try:
            df = pd.read_sql_query(f'SELECT * FROM "{sheet_name}"', conn)
        finally:
            conn.close()
        return df
