"""
SQL Executor
Runs user-provided SQL queries after validation
"""
import sqlite3
from sql_validator import SQLValidator


def execute_query(sql_query: str, db_path: str = "supply_chain_data.db") -> dict:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
# cursor.description se har column ka metadata milta hai.
# [d[0] for d in cursor.description] ka matlab: har column ka naam uthana (first item of each description tuple).
        conn.close()
        return {'success': True, 'columns': cols, 'rows': rows}
    except Exception as e:
        return {'success': False, 'error': str(e)}
