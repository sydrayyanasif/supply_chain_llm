"""
LangGraph flow: NL → SQL → validate → execute → (retry on failure) → result
"""
from langgraph.graph import StateGraph, START, END
from typing import TypedDict, List, Dict, Any
from sql_generator import SQLGenerator
from sql_validator import SQLValidator
from sql_executor import execute_query
from dotenv import load_dotenv

load_dotenv()

MAX_RETRIES = 2  # Retry up to 2 times if SQL fails


class State(TypedDict):
    messages: List[Dict[str, Any]]
    retry_count: int
    last_error: str


sql_gen = SQLGenerator()
validator = SQLValidator()


def nl_to_sql_node(state: State) -> Dict[str, Any]:
    """Convert user's natural language question to SQL, using error feedback on retries."""
    user_q = state['messages'][0]['user']  # always use original question
    error_feedback = state.get('last_error', '')
    sql = sql_gen.generate_sql_query(user_q, error_feedback=error_feedback)
    print(f"\n[SQL Generated]\n{sql}\n")
    return {
        'messages': state['messages'] + [{'assistant': sql}],
        'retry_count': state.get('retry_count', 0),
        'last_error': '',
    }


def validate_execute_node(state: State) -> Dict[str, Any]:
    """Validate the SQL, execute it, retry on failure."""
    sql = state['messages'][-1]['assistant']
    retry_count = state.get('retry_count', 0)

    # Validate
    vr = validator.validate_query(sql)
    if not vr['is_valid']:
        error_msg = validator.format_validation_report(vr)
        # Retry if under limit
        if retry_count < MAX_RETRIES:
            print(f"[Validation failed, retrying {retry_count+1}/{MAX_RETRIES}]")
            return {
                'messages': [state['messages'][0]],   # reset to original user message
                'retry_count': retry_count + 1,
                'last_error': error_msg,
            }
        return {
            'messages': state['messages'] + [{'assistant': f"❌ Could not generate a valid query after {MAX_RETRIES} attempts.\n{error_msg}"}],
            'retry_count': retry_count,
            'last_error': '',
        }

    # Execute
    res = execute_query(sql)
    if not res['success']:
        exec_error = f"Execution error: {res['error']}"
        if retry_count < MAX_RETRIES:
            print(f"[Execution failed, retrying {retry_count+1}/{MAX_RETRIES}]: {res['error']}")
            return {
                'messages': [state['messages'][0]],
                'retry_count': retry_count + 1,
                'last_error': exec_error,
            }
        return {
            'messages': state['messages'] + [{'assistant': f"❌ Query execution failed after {MAX_RETRIES} retries.\n{exec_error}"}],
            'retry_count': retry_count,
            'last_error': '',
        }

    # Format result
    if not res['rows']:
        answer = "No results found for your query."
    else:
        header = " | ".join(res['columns'])
        rows   = "\n".join(" | ".join(map(str, r)) for r in res['rows'])
        answer = header + "\n" + rows

    return {
        'messages': state['messages'] + [{'assistant': answer}],
        'retry_count': retry_count,
        'last_error': '',
    }


def should_retry(state: State) -> str:
    """Router: if last_error is set, loop back to SQL generation."""
    if state.get('last_error'):
        return 'nl_to_sql'
    return END


# ── Build Graph ───────────────────────────────────────────────────────────────
builder = StateGraph(State)
builder.add_node('nl_to_sql',        nl_to_sql_node)
builder.add_node('validate_execute', validate_execute_node)

builder.add_edge(START,        'nl_to_sql')
builder.add_edge('nl_to_sql',  'validate_execute')

# Conditional: retry or finish
builder.add_conditional_edges('validate_execute', should_retry, {
    'nl_to_sql': 'nl_to_sql',
    END:          END,
})

graph = builder.compile()
