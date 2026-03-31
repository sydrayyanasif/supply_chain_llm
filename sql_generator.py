"""
Text-to-SQL Generator using Gemini LLM
Converts natural language queries to SQL
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
            model="gemini-1.5-flash",
            api_key=GOOGLE_API_KEY,
            temperature=0.1,
        )

    def generate_sql_query(self, user_question: str) -> str:
        schema_context = self.schema_manager.get_schema_context()
        prompt = f"""
        You are an expert SQL developer and data analyst. 
        Given the database schema and a user question below, generate a syntactically correct, efficient, and executable SQLite SQL query. 

        INSTRUCTIONS:
        - **DO NOT** invent any table or column names not present in the schema.
        - Use double quotes around column/table names with spaces or special characters.
        - When the user query contains a date/month/year in any text format (like 'Jan-22', '1/2/2022', 'March 2023'), always use the '_Text' version of the column (e.g. "Time Period_Text", "ORDER_DATE_Text", etc.) for filtering, if it exists. Never use the raw date column for such queries.
        - If both tables have month/year in _TEXT columns like ‘Jan-22’, always join on these columns for perfect alignment.
        - For text/partial matches, use UPPER(column) LIKE UPPER('%value%') syntax.
        - For all text column filters, always use UPPER(TRIM(column_name)) for matching, and always compare with   UPPER(TRIM('user_value')). This makes queries case-insensitive and space-insensitive.
        - Table names must match exactly as in the schema. For example, use **"customer shipment"** (with space), not customer_shipment.
        - If the question does not specify a LIMIT, default to LIMIT 10.
        - For ambi guous questions, respond with "Clarification needed:" and a question, do NOT assume anything.
        - Always use only the provided schema and sample values, and do not return explanations or comments.
        - Output only the SQL query, nothing else.

        DATABASE SCHEMA:
        {schema_context}

        USER QUESTION:
        \"{user_question}\"
        """

# Breakdown:
# ✅ \"{user_question}\" ka output:
# Agar user_question = "What is the revenue for Jan-22?" ho,
# toh yeh line banegi:

# USER QUESTION:
#         "What is the revenue for Jan-22?"
# ❓ Kyun use karte hain escaped quotes?
# LLM (Gemini) ko jab hum prompt bhejte hain, toh:
# User ke sawal ko clearly double quotes me wrap karna useful hota hai
# Isse LLM clearly samajhta hai ki: "Yeh hi user ka query hai"

        try:
            response = self.llm.invoke(prompt)
        except Exception as e:
            return f"Clarification needed: LLM service error — {e}"
        sql = response.content.strip()
        print("🔎 Generated SQL Query:\n", sql)
        return self._clean_sql_query(sql)


    def _clean_sql_query(self, sql_query: str) -> str:
        # Remove triple backticks and any leading/trailing whitespace
        sql = re.sub(r'```sql', '', sql_query, flags=re.IGNORECASE)
        sql = re.sub(r'```', '', sql)
        sql = sql.strip()  # Remove leading/trailing whitespace/newlines
        # Remove any lines that are empty (in case of newlines above/below)
        sql = "\n".join([line for line in sql.splitlines() if line.strip() != ""])
        # Now squash multiple spaces to one and ensure everything is in a single line
        sql = ' '.join(sql.split())
        if not sql.lower().startswith("select"):
            # Sometimes LLM may generate stuff like here is your query before SELECT. Try to remove anything before SELECT.
            idx = sql.lower().find("select")
# Python ka .find() function kisi string ke andar substring ka index (position) dhoondhta hai.
# Agar nahi milta → -1 return karta hai.
            if idx != -1:
                sql = sql[idx:]
        if not sql.endswith(';'):
            sql += ';'
        return sql
