"""
Defines the LangGraph flow: NL query → SQL generation → validation → execution → result.
"""
from langgraph.graph import StateGraph, START, END
from typing import TypedDict, List, Dict, Any
from sql_generator import SQLGenerator
from sql_validator import SQLValidator
from sql_executor import execute_query
from dotenv import load_dotenv
load_dotenv()

class State(TypedDict):
    messages: List[Dict[str, Any]]


sql_gen = SQLGenerator()
validator = SQLValidator()

def nl_to_sql_node(state: State) -> Dict[str, Any]:
# Isme messages naam ka ek list hota hai jisme user aur assistant ke messages dictionary ke form mein store hote hain.
    user_q = state['messages'][-1]['user'] # Last message (user ka question)
    sql = sql_gen.generate_sql_query(user_q) # Gemini se SQL bna
    return {'messages': [{'assistant': sql}]} # SQL ko assistant response mein daal

def validate_execute_node(state: State) -> Dict[str, Any]:
    sql = state['messages'][-1]['assistant'] # Last assistant message = SQL query
    vr = validator.validate_query(sql)
    if not vr['is_valid']:
        return {'messages': [{'assistant': validator.format_validation_report(vr)}]}
    res = execute_query(sql)
    if not res['success']:  # Agar execution mein error aayi toh error bhej do
        return {'messages': [{'assistant': f"Execution error: {res['error']}"}]}
    header = " | ".join(res['columns'])
    body = "\n".join([" | ".join(map(str,r)) for r in res['rows']])
    return {'messages': [{'assistant': header + "\n" + body}]}

# Product Category | Sales
# GROOMING         | 1200
# SNACKS           | 850
# WASHCARE         | 430


# Build graph
builder = StateGraph(State)
builder.add_node('nl_to_sql', nl_to_sql_node)
builder.add_edge(START, 'nl_to_sql')
builder.add_node('validate_execute', validate_execute_node)
builder.add_edge('nl_to_sql', 'validate_execute')
builder.add_edge('validate_execute', END)

graph = builder.compile()


# [
#   {"user": "Which product category had the highest shipment in Jan-22?"},
#   {"assistant": "SELECT \"Product Category\", SUM(SHIPMENT_QTY) FROM \"customer shipment\" WHERE SHIPMENT_TIME_PERIOD_ID_Text = 'Jan-22' GROUP BY \"Product Category\" ORDER BY SUM(SHIPMENT_QTY) DESC LIMIT 1;"},
#   {"assistant": "Product Category | SUM(SHIPMENT_QTY)\nGROOMING | 1290"}
# ]

