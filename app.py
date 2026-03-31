import streamlit as st
from langgraph_flow import graph, State
import pandas as pd
from schema_manager import SchemaManager
from langchain_google_genai import ChatGoogleGenerativeAI
import io
import csv
import sqlite3
import os

from dotenv import load_dotenv
load_dotenv()

schema_manager = SchemaManager()
schema_context = schema_manager.get_schema_context()
llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash",
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.23,
)


def get_suggested_questions_dynamic(user_query):
    prompt = f"""
    You are an expert supply chain analytics assistant.
    Using the database schema and example user question below,
    generate 3 highly relevant follow-up questions a business user might ask next.
    The follow-ups should use available columns/tables/values as context, and should not repeat the original question.
    Return ONLY the list of 3 questions, one per line, and nothing else.

    === SCHEMA ===
    {schema_context}

    USER QUESTION:
    {user_query}
    """
    resp = llm.invoke(prompt)
    questions = []
    for line in resp.content.strip().splitlines():
        q = line.lstrip("-• 123.").strip()
        if q:
            questions.append(q)
    return questions[:3] if questions else []


def get_table_summary_nl(question, answer_table):
    if answer_table is None or answer_table.empty:
        return None
    sample_rows = answer_table.head(10).to_csv(index=False)
    prompt = f"""
    Below is a supply chain analytics question, and a table result (as CSV).
    Summarize the main insights in 2-3 bullet points, in clear business English. Do not repeat the question, focus on key data findings.

    USER QUESTION:
    {question}

    TABLE (CSV, top 10 rows):
    {sample_rows}

    SUMMARY:
    """
    resp = llm.invoke(prompt)
    bullets = [l.strip("-• \n") for l in resp.content.strip().splitlines() if l.strip()]
    return "\n".join(f"• {b}" for b in bullets if b)


# --------- FEEDBACK (thumbs-down only, minimal storage) --------------
FEEDBACK_DB = "feedback.db"

def ensure_feedback_table():
    with sqlite3.connect(FEEDBACK_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_negative_feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_question TEXT,
                answer TEXT,
                comment TEXT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
ensure_feedback_table()


def save_negative_feedback_to_db(user, answer, comment):
    with sqlite3.connect(FEEDBACK_DB) as conn:
        conn.execute(
            "INSERT INTO chat_negative_feedback (user_question, answer, comment) VALUES (?, ?, ?)",
            (user, answer, comment)
        )
    print(f"\nSaved Negative Feedback:\nQ: {user}\nA: {answer}\nComment: {comment}\n{'-'*40}")


# ---------- Streamlit UI Setup ----------
st.set_page_config(page_title="Supply Chain LLM", layout="centered", initial_sidebar_state="expanded")

# BUG FIX: Removed Python-style # comments from inside CSS string (invalid CSS).
# CSS comments use /* comment */ syntax.
st.markdown("""
    <style>
    [data-testid="stSidebar"] {
         background-color: #00a6ce !important;
     }
    .stButton > button {
        background: #e6f1f7 !important;
        color: #00468b !important;
        font-weight: 600 !important;
        border-radius: 0.7rem !important;
        font-size: 0.97rem !important;
        white-space: normal !important;
        word-break: break-word !important;
        line-height: 1.25 !important;
        padding: 0.52rem 0.7rem 0.52rem 0.7rem !important;
        margin-bottom: 0.12rem !important;
        text-align: left !important;
        box-shadow: none !important;
        border: none !important;
    }
    .stButton > button:hover {
        background: #c7e4fa !important;
        color: #002242 !important;
    }
    .badge-next {
        background: #e6f1f7 !important;
        color: #00468b !important;
        border-radius: 0.7rem;
        padding: 0.46rem 0.8rem 0.42rem 0.8rem;
        margin-right: 0.22rem;
        font-size: 0.97rem;
        font-weight: 600;
        display: inline-block;
        margin-bottom: 0.14rem;
        line-height: 1.25;
    }
    .feedback-row {
        margin-top: -0.45rem;
        margin-bottom: 0.5rem;
        padding-left: 0.08rem;
    }
    .thumb {
        font-size: 1.25rem;
        cursor: pointer;
        margin-right: 0.25rem;
    }
    .thumb.selected {
        font-weight: bold;
        text-shadow: 0 1px 3px #cce6f9;
        color: #00a6ce;
    }
    .stTextInput>div>div>input {
        background: #fff !important;
        color: #111 !important;
        font-size: 1rem;
        border-radius: 0.7rem;
        border: 1.5px solid #d3e2f8;
    }
    .ask-btn > button:hover {
        background: #00a6ce !important;
        color: #151c26 !important;
    }
    </style>
""", unsafe_allow_html=True)

FAQ_QUESTIONS = [
    "Show the business forecast and actual for Snacks category in January 2023.",
    "Which product category had the highest Monthly Revenue in 2023?",
    "Show me all orders for customer Hubert batter in Feb-22.",
    "List all months where inventory was lower than shipment quantity and lost sales were reported.",
    "For each month in 2022, compare the business forecast, actual sales, and the number of lost units for Namkeen.",
    "Show the total loss value by Customer in Jan-23."
]

if 'chat_history' not in st.session_state:
    st.session_state['chat_history'] = []
if 'selected_faq' not in st.session_state:
    st.session_state['selected_faq'] = None
if 'user_input' not in st.session_state:
    st.session_state['user_input'] = ""
if 'feedback' not in st.session_state:
    st.session_state['feedback'] = {}
if 'feedback_done' not in st.session_state:
    st.session_state['feedback_done'] = {}

# Sidebar: FAQ and Export
with st.sidebar:
    st.markdown("<h4 style='color:#e9edf5;margin-bottom:0.15rem;font-weight:800;'>Frequently Asked Questions</h4>", unsafe_allow_html=True)
    for q in FAQ_QUESTIONS:
        if st.button(q, key=q, use_container_width=True):
            st.session_state['selected_faq'] = q

    st.markdown("---")
    st.markdown("<div style='font-size:0.97rem;color:#e9edf5;margin-top:0.3rem;'>Recent Questions</div>", unsafe_allow_html=True)
    for chat in reversed(st.session_state['chat_history'][-5:]):
        st.markdown(f"<span style='color:#8dbaf3;font-size:0.98rem;'>• {chat['user']}</span>",
                    unsafe_allow_html=True)

    if st.session_state['chat_history']:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["User", "Bot Answer"])
        for ch in st.session_state['chat_history']:
            writer.writerow([ch["user"], ch["answer"]])
        st.download_button(
            label="⬇️ Download Chat (CSV)",
            data=output.getvalue(),
            file_name="supply_chain_chat.csv",
            mime="text/csv"
        )


st.markdown('<h2 style="color:#00a6ce;text-align:center;font-weight:800;margin-top:0.5rem;margin-bottom:0.0rem;">🤖 Supply Chain LLM Chatbot</h2>', unsafe_allow_html=True)


def process_user_query(question):
    state: State = {"messages": [{"user": question}]}
    result = graph.invoke(state)
    answer = result["messages"][-1]["assistant"]
    df, columns = None, None
    if "|" in answer:
        lines = answer.strip().splitlines()
        if len(lines) > 1 and "|" in lines[0]:
            header = [c.strip() for c in lines[0].split("|")]
            data_rows = []
            for line in lines[1:]:
                if "|" in line:
                    row = [c.strip() for c in line.split("|")]
                    data_rows.append(row)
            if data_rows:
                df = pd.DataFrame(data_rows, columns=header)
                columns = header
    return answer, df, columns


def ask_question_input():
    input_col, button_col = st.columns([7, 1])
    with input_col:
        user_input = st.text_input(
            "",
            value=st.session_state['user_input'],
            placeholder="Type a supply chain question or select from the left...",
            key="user_input_box"
        )
    with button_col:
        send_btn = st.button("Ask", use_container_width=True, key="ask_button")
    faq_q = st.session_state.get('selected_faq', None)
    question = None
    if faq_q:
        question = faq_q
        st.session_state['selected_faq'] = None
        st.session_state['user_input'] = ""
    elif send_btn and user_input:
        question = user_input
        st.session_state['user_input'] = ""
    else:
        st.session_state['user_input'] = user_input
    return question


question = ask_question_input()


if question:
    with st.spinner("Thinking..."):
        answer, df, columns = process_user_query(question)
    st.session_state['chat_history'].append({
        "user": question,
        "answer": answer,
        "df": df,
        "columns": columns,
        "summary": get_table_summary_nl(question, df) if df is not None else None,
    })


# ----------- Chat Display -----------
for i, chat in enumerate(reversed(st.session_state['chat_history'][-5:])):
    msg_id = len(st.session_state['chat_history']) - 1 - i
    st.markdown(f'<div style="font-weight:600;color:#00a6ce;">🧑 {chat["user"]}</div>', unsafe_allow_html=True)
    print(chat['answer'])
    st.markdown(
        f'<div style="font-weight:500;color:#111;background:#e6f1f7;border-radius:0.5rem;padding:0.48rem 1rem 0.48rem 1rem;margin-bottom:0.36rem;">🤖 {chat["answer"].replace("|", "&#124;").replace(chr(10), "<br>") if chat["df"] is None else ""}</div>',
        unsafe_allow_html=True
    )

    if chat['df'] is not None:
        num_rows = chat['df'].shape[0]
        base_row_height = 36
        max_visible_rows = 10
        height = min(num_rows, max_visible_rows) * base_row_height + 36
        st.dataframe(chat['df'], use_container_width=True, height=height)
        with st.expander("📊 Visualize as Chart", expanded=False):
            chart_type = st.selectbox("Chart Type", ["Bar", "Line", "Area"], key=f"chart_{i}")
            if len(chat['columns']) >= 2:
                x_col = st.selectbox("X-axis", chat['columns'], key=f"x_{i}")
                y_col = st.selectbox("Y-axis", chat['columns'], index=1, key=f"y_{i}")
                chart_df = chat['df'].copy()
                try:
                    chart_df[y_col] = chart_df[y_col].str.replace("%", "").astype(float)
                except Exception:
                    try:
                        chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors="coerce")
                    except Exception:
                        pass
                if chart_type == "Bar":
                    st.bar_chart(chart_df.set_index(x_col)[y_col])
                elif chart_type == "Line":
                    st.line_chart(chart_df.set_index(x_col)[y_col])
                elif chart_type == "Area":
                    st.area_chart(chart_df.set_index(x_col)[y_col])
        if chat.get("summary"):
            st.markdown(
                f"<div style='background:#f4fbfd;border-radius:0.5rem;padding:0.45rem 0.85rem 0.45rem 0.85rem;margin-bottom:0.15rem;margin-top:0.12rem;color:#225681;font-size:0.97rem;'>"
                f"<b>Summary:</b><br>{chat['summary'].replace(chr(10), '<br>')}</div>", unsafe_allow_html=True
            )

    # --- Feedback: thumbs down only with thank you and auto-close ---
    st.markdown('<div class="feedback-row">', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1, 10])
    thumbs_up = "👍"
    thumbs_down = "👎"
    feedback_state = st.session_state['feedback'].get(msg_id, (None, ""))
    with c1:
        if st.button(thumbs_up, key=f"thumbs_up_{msg_id}"):
            st.session_state['feedback'][msg_id] = ("up", "")
            st.session_state['feedback_done'][msg_id] = False
    with c2:
        if st.button(thumbs_down, key=f"thumbs_down_{msg_id}"):
            st.session_state['feedback'][msg_id] = ("down", "")
            st.session_state['feedback_done'][msg_id] = False

    with c3:
        show_box = (
            st.session_state['feedback'].get(msg_id, (None, ""))[0] == "down"
            and not st.session_state['feedback_done'].get(msg_id, False)
        )
        if show_box:
            comment = st.text_input(
                "Feedback (How can we improve this answer?)",
                value=feedback_state[1],
                key=f"fb_{msg_id}"
            )
            if comment.strip():
                save_negative_feedback_to_db(chat["user"], chat["answer"], comment.strip())
                st.session_state['feedback_done'][msg_id] = True
                st.success("Thank you for your feedback!", icon="✅")
        elif st.session_state['feedback_done'].get(msg_id, False):
            st.success("Thank you for your feedback!", icon="✅")
    st.markdown('</div>', unsafe_allow_html=True)

    # Suggested questions
    st.markdown("<span style='color:#00a6ce;font-size:0.95rem;'>Try next:</span>", unsafe_allow_html=True)
    suggested = get_suggested_questions_dynamic(chat['user'])
    if not suggested:
        suggested = ["Show overall monthly trend.", "List top customers for this segment."]
    st.markdown(" ".join([f"<span class='badge-next'>{s}</span>" for s in suggested]), unsafe_allow_html=True)
