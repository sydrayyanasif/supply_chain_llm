"""
Text-to-SQL Generator using Gemini LLM
Converts natural language queries to SQL with retry on failure
"""
import os
import re
from langchain_google_genai import ChatGoogleGenerativeAI
from schema_manager import SchemaManager
from dotenv import load_dotenv

load_dotenv()


class SQLGenerator:
    def __init__(self):
        self.schema_manager = SchemaManager()
        GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
        if not GOOGLE_API_KEY:
            raise EnvironmentError("GOOGLE_API_KEY environment variable not set.")
        self.llm = ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            api_key=GOOGLE_API_KEY,
            temperature=0.1,
        )

    def generate_sql_query(self, user_question: str, error_feedback: str = "") -> str:
        schema_context = self.schema_manager.get_schema_context()

        retry_block = ""
        if error_feedback:
            retry_block = f"""
PREVIOUS ATTEMPT FAILED WITH THIS ERROR:
{error_feedback}
Please fix the query based on the error above.
"""

        prompt = f"""
You are an expert SQLite SQL developer. Convert the user question to a valid SQLite query.

STRICT RULES:
1. Use ONLY table and column names from the schema below. Do NOT invent any.
2. Always wrap table/column names that contain spaces or special characters in double quotes.
   Examples: "Strategic Data", "Order Date", "Customer Shipment",
             "Time Period_Text", "Product Category", "Business Forecast"
3. For date/month filtering, ALWAYS use the _Text column (e.g., "Time Period_Text", 
   "ORDER_DATE_Text", "SHIPMENT_TIME_PERIOD_ID_Text", "TIME_PERIOD_ID_Text",
   "Lost_Month_Text", "PROMOTION_START_DT_Text") with 'Mon-YY' format like 'Jan-22'.
4. For year-only filters (e.g. "in 2023"), use LIKE '%23' on the _Text column.
5. For partial text matches, use UPPER(TRIM(col)) LIKE UPPER(TRIM('%value%')).
6. If question is ambiguous, write the most reasonable query; do NOT ask for clarification.
7. Default LIMIT 10 unless the user asks for all or a specific number.
8. Output ONLY the raw SQL query — no explanation, no markdown, no backticks.
9. End the query with a semicolon.
10. Never use DROP, DELETE, INSERT, UPDATE, or ALTER.

SQLITE-SPECIFIC NOTES:
- SQLite has no FULL OUTER JOIN; use LEFT JOIN + UNION if needed.
- Use strftime() for date arithmetic if needed.
- Column aliases with spaces must be in double quotes: SELECT SUM(x) AS "Total Sales"

{retry_block}

{schema_context}

USER QUESTION: "{user_question}"
"""

        response = self.llm.invoke(prompt)
        sql = response.content.strip()
        return self._clean_sql(sql)

    def _clean_sql(self, sql: str) -> str:
        # Strip markdown code fences
        sql = re.sub(r'```sql', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```', '', sql)
        sql = sql.strip()

        # Remove blank lines
        sql = "\n".join(line for line in sql.splitlines() if line.strip())

        # Collapse extra whitespace to single line
        sql = ' '.join(sql.split())

        # Ensure it starts with SELECT
        if not sql.lower().startswith("select"):
            idx = sql.lower().find("select")
            if idx != -1:
                sql = sql[idx:]

        # Ensure trailing semicolon
        if not sql.endswith(';'):
            sql += ';'

        return sql
