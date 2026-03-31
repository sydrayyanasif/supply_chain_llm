"""
SQL Query Validator
Validates SQL queries for safety, syntax, and business logic
"""
import re
import sqlite3
from schema_manager import SchemaManager

class SQLValidator:
    def __init__(self, db_path: str = "supply_chain_data.db"):
        self.db_path = db_path
        self.schema_manager = SchemaManager()
        self.dangerous_patterns = [
            r'\bDROP\b',
            r'\bDELETE\b',
            r'\bINSERT\b',
            r'\bUPDATE\b',
            r'\bALTER\b',
            r'--',
            r'/\*.*?\*/',   # BUG FIX: Added re.DOTALL flag below so this matches multiline block comments
            r';\s*\w+'      # Multiple statements (semicolon followed by another word)
        ]

    def validate_query(self, sql_query: str) -> dict:
        result = {'is_valid': True, 'errors': [], 'warnings': []}
        q = sql_query.strip()

        # Check for dangerous patterns
        # BUG FIX: Added re.DOTALL so /\*.*?\*/ matches multiline block comments correctly
        for p in self.dangerous_patterns:
            if re.search(p, q, re.IGNORECASE | re.DOTALL):
                # Special case: the last pattern ;\s*\w+ would also match the final semicolon
                # followed by nothing — but since we strip, this only fires for multi-statement queries
                result['is_valid'] = False
                result['errors'].append(f"Dangerous pattern detected: {p}")

        # Query must start with SELECT
        if not re.match(r'^\s*SELECT\b', q, re.IGNORECASE):
            result['is_valid'] = False
            result['errors'].append("Query must start with SELECT")

        # Query must end with semicolon
        if not q.endswith(';'):
            result['is_valid'] = False
            result['errors'].append("Query must end with semicolon")

        # Validate syntax using SQLite EXPLAIN (dry run, no actual execution)
        if result['is_valid']:
            try:
                conn = sqlite3.connect(self.db_path)
                conn.execute(f"EXPLAIN {q}")
                conn.close()
            except sqlite3.Error as e:
                result['is_valid'] = False
                result['errors'].append(str(e))

        return result

    def format_validation_report(self, res: dict) -> str:
        lines = []
        if res['is_valid']:
            lines.append("✅ Query is valid.")
        else:
            lines.append("❌ Validation failed:")
            for err in res['errors']:
                lines.append(f"  • {err}")
        return '\n'.join(lines)
