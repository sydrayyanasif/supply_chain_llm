import streamlit as st
from langgraph_flow import graph, State
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from schema_manager import SchemaManager
from langchain_google_genai import ChatGoogleGenerativeAI
import io
import csv
import sqlite3
import os
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─── LLM & Schema ────────────────────────────────────────────────────────────
schema_manager = SchemaManager()
schema_context = schema_manager.get_schema_context()
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash",
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.2,
)

# ─── Page Config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SupplyIQ · AI Analytics",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── CSS ─────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&family=DM+Sans:wght@300;400;500&display=swap');

/* ── Root Variables ── */
:root {
    --bg-primary:    #0a0e1a;
    --bg-secondary:  #0f1525;
    --bg-card:       #131929;
    --bg-hover:      #1a2236;
    --border:        #1e2d47;
    --border-light:  #243450;
    --amber:         #f59e0b;
    --amber-dim:     #92610a;
    --cyan:          #06b6d4;
    --cyan-dim:      #0e4f5c;
    --green:         #10b981;
    --red:           #ef4444;
    --text-primary:  #e8edf5;
    --text-secondary:#8899b4;
    --text-dim:      #4a5f7a;
    --font-display:  'Syne', sans-serif;
    --font-mono:     'Space Mono', monospace;
    --font-body:     'DM Sans', sans-serif;
}

/* ── Global Reset ── */
html, body, [class*="css"] {
    font-family: var(--font-body) !important;
    background-color: var(--bg-primary) !important;
    color: var(--text-primary) !important;
}

.stApp { background-color: var(--bg-primary) !important; }

/* ── Sidebar ── */
[data-testid="stSidebar"] {
    background: var(--bg-secondary) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] > div:first-child { padding-top: 1rem; }

/* ── Hide default Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--bg-primary); }
::-webkit-scrollbar-thumb { background: var(--border-light); border-radius: 2px; }

/* ── Main Content Padding ── */
.main .block-container {
    padding: 1.5rem 2rem 4rem 2rem !important;
    max-width: 1100px !important;
}

/* ── Hero Header ── */
.hero-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1.5rem 0 1rem 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.5rem;
}
.hero-logo {
    font-family: var(--font-display);
    font-size: 1.9rem;
    font-weight: 800;
    letter-spacing: -0.03em;
    color: var(--text-primary);
    line-height: 1;
}
.hero-logo span { color: var(--amber); }
.hero-tagline {
    font-family: var(--font-mono);
    font-size: 0.72rem;
    color: var(--text-dim);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-top: 0.2rem;
}
.hero-badge {
    margin-left: auto;
    background: linear-gradient(135deg, var(--cyan-dim), #0a3a47);
    border: 1px solid var(--cyan);
    color: var(--cyan);
    font-family: var(--font-mono);
    font-size: 0.68rem;
    padding: 0.3rem 0.7rem;
    border-radius: 0.25rem;
    letter-spacing: 0.08em;
}

/* ── KPI Cards ── */
.kpi-row {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 0.75rem;
    margin-bottom: 1.25rem;
}
.kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 0.5rem;
    padding: 0.9rem 1rem;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, var(--amber), transparent);
}
.kpi-card.cyan::before { background: linear-gradient(90deg, var(--cyan), transparent); }
.kpi-card.green::before { background: linear-gradient(90deg, var(--green), transparent); }
.kpi-label {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-dim);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-bottom: 0.35rem;
}
.kpi-value {
    font-family: var(--font-display);
    font-size: 1.6rem;
    font-weight: 700;
    color: var(--text-primary);
    line-height: 1;
}
.kpi-value.amber { color: var(--amber); }
.kpi-value.cyan  { color: var(--cyan); }
.kpi-value.green { color: var(--green); }
.kpi-sub {
    font-size: 0.72rem;
    color: var(--text-dim);
    margin-top: 0.25rem;
}

/* ── Input area ── */
.stTextInput > div > div > input {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-light) !important;
    border-radius: 0.5rem !important;
    color: var(--text-primary) !important;
    font-family: var(--font-body) !important;
    font-size: 0.95rem !important;
    padding: 0.65rem 1rem !important;
    transition: border-color 0.2s;
}
.stTextInput > div > div > input:focus {
    border-color: var(--amber) !important;
    box-shadow: 0 0 0 3px rgba(245,158,11,0.1) !important;
}
.stTextInput > div > div > input::placeholder { color: var(--text-dim) !important; }

/* ── Buttons ── */
.stButton > button {
    background: var(--bg-card) !important;
    color: var(--text-secondary) !important;
    border: 1px solid var(--border-light) !important;
    border-radius: 0.4rem !important;
    font-family: var(--font-body) !important;
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    padding: 0.5rem 0.85rem !important;
    transition: all 0.18s ease !important;
    white-space: normal !important;
    word-break: break-word !important;
    text-align: left !important;
}
.stButton > button:hover {
    background: var(--bg-hover) !important;
    color: var(--amber) !important;
    border-color: var(--amber-dim) !important;
}

/* ── Ask button special ── */
div[data-testid="column"]:last-child .stButton > button {
    background: var(--amber) !important;
    color: #0a0e1a !important;
    border-color: var(--amber) !important;
    font-weight: 700 !important;
    font-family: var(--font-display) !important;
    letter-spacing: 0.04em;
}
div[data-testid="column"]:last-child .stButton > button:hover {
    background: #d97706 !important;
    color: #0a0e1a !important;
    border-color: #d97706 !important;
}

/* ── Chat bubbles ── */
.msg-user {
    display: flex;
    align-items: flex-start;
    gap: 0.75rem;
    margin-bottom: 1rem;
    animation: fadeSlideUp 0.3s ease;
}
.msg-avatar {
    width: 32px; height: 32px;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.85rem;
    flex-shrink: 0;
    margin-top: 2px;
}
.msg-avatar.user-av {
    background: linear-gradient(135deg, var(--amber), #d97706);
    color: #0a0e1a;
    font-weight: 700;
}
.msg-avatar.bot-av {
    background: linear-gradient(135deg, var(--cyan-dim), #063d4f);
    border: 1px solid var(--cyan);
    color: var(--cyan);
}
.msg-bubble-user {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-left: 3px solid var(--amber);
    border-radius: 0 0.6rem 0.6rem 0.6rem;
    padding: 0.7rem 1rem;
    font-size: 0.93rem;
    color: var(--text-primary);
    line-height: 1.5;
    max-width: 85%;
}
.msg-bubble-bot {
    background: var(--bg-secondary);
    border: 1px solid var(--border);
    border-left: 3px solid var(--cyan);
    border-radius: 0 0.6rem 0.6rem 0.6rem;
    padding: 0.7rem 1rem;
    font-size: 0.93rem;
    color: var(--text-secondary);
    line-height: 1.6;
    max-width: 85%;
    font-family: var(--font-mono);
    font-size: 0.82rem;
    white-space: pre-wrap;
}

/* ── SQL Preview box ── */
.sql-block {
    background: #070b14;
    border: 1px solid var(--border);
    border-radius: 0.4rem;
    padding: 0.75rem 1rem;
    font-family: var(--font-mono);
    font-size: 0.78rem;
    color: var(--cyan);
    margin: 0.5rem 0;
    overflow-x: auto;
    white-space: pre-wrap;
    word-break: break-word;
}

/* ── Data table ── */
[data-testid="stDataFrame"] {
    border: 1px solid var(--border) !important;
    border-radius: 0.5rem !important;
    overflow: hidden;
}
[data-testid="stDataFrame"] > div { background: var(--bg-card) !important; }

/* ── Summary card ── */
.summary-card {
    background: linear-gradient(135deg, #0d1f30, #091624);
    border: 1px solid var(--cyan-dim);
    border-left: 3px solid var(--cyan);
    border-radius: 0.5rem;
    padding: 0.85rem 1.1rem;
    margin-top: 0.6rem;
    font-size: 0.88rem;
    color: var(--text-secondary);
    line-height: 1.7;
}
.summary-label {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--cyan);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 0.4rem;
}

/* ── Suggested questions ── */
.suggest-label {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-dim);
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin: 0.9rem 0 0.4rem 0;
}
.suggest-chip {
    display: inline-block;
    background: var(--bg-card);
    border: 1px solid var(--border-light);
    border-radius: 2rem;
    padding: 0.3rem 0.75rem;
    font-size: 0.8rem;
    color: var(--text-secondary);
    margin: 0.2rem 0.2rem 0.2rem 0;
    line-height: 1.4;
    cursor: default;
    transition: border-color 0.15s;
}
.suggest-chip:hover { border-color: var(--amber); color: var(--amber); }

/* ── Feedback ── */
.fb-row { display: flex; align-items: center; gap: 0.4rem; margin: 0.5rem 0; }
.fb-divider {
    border: none;
    border-top: 1px solid var(--border);
    margin: 1.25rem 0 0.75rem 0;
}

/* ── Sidebar Sections ── */
.sidebar-section-label {
    font-family: var(--font-mono);
    font-size: 0.62rem;
    color: var(--text-dim);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 0.7rem 0 0.35rem 0;
}
.sidebar-faq-q {
    font-size: 0.82rem;
    color: var(--text-secondary);
    padding: 0.35rem 0.5rem;
    border-left: 2px solid var(--amber-dim);
    margin-bottom: 0.3rem;
    border-radius: 0 0.3rem 0.3rem 0;
    cursor: pointer;
    transition: all 0.15s;
    line-height: 1.4;
    background: transparent;
}
.sidebar-faq-q:hover { background: var(--bg-hover); color: var(--amber); }
.recent-q {
    font-size: 0.8rem;
    color: var(--text-dim);
    padding: 0.25rem 0;
    border-bottom: 1px solid var(--border);
    line-height: 1.4;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.recent-q::before { content: '›  '; color: var(--amber); }

/* ── Expander ── */
details > summary {
    font-family: var(--font-mono);
    font-size: 0.78rem;
    color: var(--text-dim);
    cursor: pointer;
    padding: 0.4rem 0;
    letter-spacing: 0.05em;
}
details > summary:hover { color: var(--amber); }
[data-testid="stExpander"] {
    border: 1px solid var(--border) !important;
    border-radius: 0.5rem !important;
    background: var(--bg-card) !important;
}

/* ── Selectbox ── */
[data-testid="stSelectbox"] > div > div {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-light) !important;
    border-radius: 0.4rem !important;
    color: var(--text-primary) !important;
}

/* ── Spinner ── */
[data-testid="stSpinner"] p { color: var(--amber) !important; font-family: var(--font-mono); font-size: 0.82rem; }

/* ── Success ── */
[data-testid="stAlert"] {
    background: #0a1f17 !important;
    border: 1px solid var(--green) !important;
    border-radius: 0.4rem !important;
    color: var(--green) !important;
}

/* ── Download button ── */
[data-testid="stDownloadButton"] > button {
    background: transparent !important;
    border: 1px solid var(--border-light) !important;
    color: var(--text-secondary) !important;
    font-size: 0.82rem !important;
    width: 100% !important;
}
[data-testid="stDownloadButton"] > button:hover {
    border-color: var(--cyan) !important;
    color: var(--cyan) !important;
}

/* ── Animation ── */
@keyframes fadeSlideUp {
    from { opacity: 0; transform: translateY(10px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes pulse {
    0%,100% { opacity: 1; } 50% { opacity: 0.4; }
}
.thinking-dot {
    display: inline-block;
    width: 6px; height: 6px;
    background: var(--amber);
    border-radius: 50%;
    animation: pulse 1.2s ease-in-out infinite;
    margin: 0 2px;
}
.thinking-dot:nth-child(2) { animation-delay: 0.2s; }
.thinking-dot:nth-child(3) { animation-delay: 0.4s; }

/* ── Section divider ── */
.section-divider {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin: 1.5rem 0 1rem 0;
}
.section-divider-line { flex: 1; height: 1px; background: var(--border); }
.section-divider-text {
    font-family: var(--font-mono);
    font-size: 0.65rem;
    color: var(--text-dim);
    letter-spacing: 0.12em;
    text-transform: uppercase;
    white-space: nowrap;
}
</style>
""", unsafe_allow_html=True)


# ─── Helper Functions ─────────────────────────────────────────────────────────

def get_db_stats():
    """Pull live KPI numbers from the database."""
    try:
        conn = sqlite3.connect("supply_chain_data.db")
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM "Strategic Data"')
        rows = c.fetchone()[0]
        c.execute('SELECT SUM("Actual") FROM "Strategic Data"')
        total_actual = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM "Lost Sale"')
        lost = c.fetchone()[0]
        conn.close()
        return rows, int(total_actual), lost
    except:
        return 120, 2_450_000, 100


def get_suggested_questions(user_query: str):
    prompt = f"""
You are a supply chain analytics expert.
Given the user question below and the database schema, generate exactly 3 short follow-up questions a business analyst might ask next.
Each question must be under 12 words.
Return ONLY the 3 questions, one per line. No numbering, no bullets, no extra text.

SCHEMA SUMMARY:
Tables: Strategic Data, Order Date, Customer Shipment, Material Forecast, Promotion, Facility Material Inventory, Lost Sale
Categories: GROOMING, SNACKS, WASHCARE, DESSERT, NAMKEEN
Date format: Jan-22, Feb-23 etc.

USER QUESTION: {user_query}
"""
    try:
        resp = llm.invoke(prompt)
        qs = [l.lstrip("-•123. ").strip() for l in resp.content.strip().splitlines() if l.strip()]
        return qs[:3]
    except:
        return []


def get_nl_summary(question: str, df: pd.DataFrame):
    if df is None or df.empty:
        return None
    try:
        sample = df.head(10).to_csv(index=False)
        prompt = f"""
You are a supply chain analyst. Summarize the key business insight from this query result in 2-3 concise bullet points.
Be specific with numbers. Use ↑ for increase, ↓ for decrease. No fluff.

QUESTION: {question}
DATA (CSV):
{sample}

Return ONLY the bullet points, one per line, starting with •
"""
        resp = llm.invoke(prompt)
        bullets = [l.strip() for l in resp.content.strip().splitlines() if l.strip()]
        return "\n".join(bullets)
    except:
        return None


def process_query(question: str):
    state: State = {"messages": [{"user": question}]}
    result = graph.invoke(state)
    raw = result["messages"][-1]["assistant"]
    df, columns, sql_used = None, None, ""

    # Extract SQL if echoed
    for msg in result["messages"]:
        if "assistant" in msg and msg["assistant"].strip().upper().startswith("SELECT"):
            sql_used = msg["assistant"]
            break

    if "|" in raw:
        lines = raw.strip().splitlines()
        if len(lines) > 1 and "|" in lines[0]:
            header = [c.strip() for c in lines[0].split("|")]
            data_rows = []
            for line in lines[1:]:
                if "|" in line:
                    row = [c.strip() for c in line.split("|")]
                    if len(row) == len(header):
                        data_rows.append(row)
            if data_rows:
                df = pd.DataFrame(data_rows, columns=header)
                columns = header
    return raw, df, columns, sql_used


# ─── Feedback DB ──────────────────────────────────────────────────────────────
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


def save_feedback(user, answer, comment):
    with sqlite3.connect(FEEDBACK_DB) as conn:
        conn.execute(
            "INSERT INTO chat_negative_feedback (user_question, answer, comment) VALUES (?, ?, ?)",
            (user, answer, comment)
        )


# ─── Session State ────────────────────────────────────────────────────────────
defaults = {
    'chat_history': [],
    'selected_faq': None,
    'user_input': "",
    'feedback': {},
    'feedback_done': {},
    'query_count': 0,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ─── Sidebar ──────────────────────────────────────────────────────────────────
FAQ_QUESTIONS = [
    "Which category had the highest actual sales in 2023?",
    "Show monthly business forecast vs actual for GROOMING.",
    "Top 3 customers by shipment quantity in Jan-23?",
    "Which facility has the lowest closing inventory?",
    "Show lost sales breakdown by reason in 2022.",
    "Compare consensus forecast vs actual shipments for SNACKS.",
    "Which promotions had the highest sales uplift in 2023?",
    "Total inventory value by product category.",
]

with st.sidebar:
    # Logo in sidebar
    st.markdown("""
    <div style='padding:0.5rem 0 1rem 0;border-bottom:1px solid #1e2d47;margin-bottom:0.5rem;'>
        <div style='font-family:"Syne",sans-serif;font-size:1.2rem;font-weight:800;color:#e8edf5;'>
            ⬡ Supply<span style='color:#f59e0b;'>IQ</span>
        </div>
        <div style='font-family:"Space Mono",monospace;font-size:0.6rem;color:#4a5f7a;letter-spacing:0.1em;margin-top:0.15rem;'>
            ANALYTICS · POWERED BY GEMINI
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div class='sidebar-section-label'>Preset Queries</div>", unsafe_allow_html=True)
    for q in FAQ_QUESTIONS:
        if st.button(q, key=f"faq_{q}", use_container_width=True):
            st.session_state['selected_faq'] = q

    st.markdown("<div style='border-top:1px solid #1e2d47;margin:0.75rem 0;'></div>", unsafe_allow_html=True)

    # Recent questions
    if st.session_state['chat_history']:
        st.markdown("<div class='sidebar-section-label'>Recent</div>", unsafe_allow_html=True)
        for chat in reversed(st.session_state['chat_history'][-5:]):
            q_short = chat['user'][:52] + "…" if len(chat['user']) > 52 else chat['user']
            st.markdown(f"<div class='recent-q'>{q_short}</div>", unsafe_allow_html=True)

        st.markdown("<div style='margin-top:0.75rem;'></div>", unsafe_allow_html=True)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Timestamp", "User Question", "Bot Answer"])
        for ch in st.session_state['chat_history']:
            writer.writerow([ch.get("ts", ""), ch["user"], ch["answer"]])
        st.download_button(
            label="⬇  Export Chat as CSV",
            data=output.getvalue(),
            file_name=f"supplyiq_chat_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True
        )

    # Clear chat
    st.markdown("<div style='margin-top:0.5rem;'></div>", unsafe_allow_html=True)
    if st.button("✕  Clear Conversation", use_container_width=True, key="clear_btn"):
        st.session_state['chat_history'] = []
        st.session_state['query_count'] = 0
        st.rerun()


# ─── Main Header ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-header">
    <div>
        <div class="hero-logo">Supply<span>IQ</span></div>
        <div class="hero-tagline">Natural Language → SQL · Supply Chain Intelligence</div>
    </div>
    <div class="hero-badge">⬡ GEMINI 1.5 FLASH</div>
</div>
""", unsafe_allow_html=True)

# ─── KPI Cards ───────────────────────────────────────────────────────────────
db_rows, total_actual, lost_rows = get_db_stats()
queries_run = st.session_state['query_count']

st.markdown(f"""
<div class="kpi-row">
    <div class="kpi-card">
        <div class="kpi-label">Data Records</div>
        <div class="kpi-value amber">{db_rows:,}</div>
        <div class="kpi-sub">across 7 supply chain tables</div>
    </div>
    <div class="kpi-card cyan">
        <div class="kpi-label">Total Actual Sales</div>
        <div class="kpi-value cyan">{total_actual:,}</div>
        <div class="kpi-sub">units · 2022–2023</div>
    </div>
    <div class="kpi-card green">
        <div class="kpi-label">Queries Run</div>
        <div class="kpi-value green">{queries_run}</div>
        <div class="kpi-sub">this session</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ─── Input Area ──────────────────────────────────────────────────────────────
def get_question():
    col_in, col_btn = st.columns([8, 1])
    with col_in:
        user_input = st.text_input(
            "",
            value=st.session_state['user_input'],
            placeholder="Ask anything about your supply chain data…  e.g. 'Which category had the highest shipment in Jan-23?'",
            key="main_input",
            label_visibility="collapsed"
        )
    with col_btn:
        send = st.button("Ask →", key="ask_btn", use_container_width=True)

    faq = st.session_state.get('selected_faq')
    question = None
    if faq:
        question = faq
        st.session_state['selected_faq'] = None
        st.session_state['user_input'] = ""
    elif send and user_input.strip():
        question = user_input.strip()
        st.session_state['user_input'] = ""
    else:
        st.session_state['user_input'] = user_input
    return question


question = get_question()

if question:
    with st.spinner("⬡ Generating SQL and querying database…"):
        answer, df, columns, sql_used = process_query(question)
        summary = get_nl_summary(question, df)
        suggested = get_suggested_questions(question)

    st.session_state['query_count'] += 1
    st.session_state['chat_history'].append({
        "user": question,
        "answer": answer,
        "df": df,
        "columns": columns,
        "sql": sql_used,
        "summary": summary,
        "suggested": suggested,
        "ts": datetime.now().strftime("%H:%M"),
    })
    st.rerun()


# ─── Chat History ─────────────────────────────────────────────────────────────
if not st.session_state['chat_history']:
    st.markdown("""
    <div style='text-align:center;padding:3rem 0;'>
        <div style='font-size:2.5rem;margin-bottom:0.75rem;'>⬡</div>
        <div style='font-family:"Syne",sans-serif;font-size:1.1rem;color:#4a5f7a;font-weight:600;'>
            Ask your first question above
        </div>
        <div style='font-size:0.85rem;color:#2e3f57;margin-top:0.4rem;'>
            Try a preset query from the sidebar, or type anything naturally
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    for i, chat in enumerate(reversed(st.session_state['chat_history'][-6:])):
        msg_id = len(st.session_state['chat_history']) - 1 - i

        # ── User message ──
        st.markdown(f"""
        <div class="msg-user">
            <div class="msg-avatar user-av">U</div>
            <div style="flex:1;">
                <div class="msg-bubble-user">{chat['user']}</div>
                <div style="font-size:0.68rem;color:#2e3f57;margin-top:0.2rem;font-family:'Space Mono',monospace;">
                    {chat.get('ts','')}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Bot message ──
        st.markdown(f"""
        <div class="msg-user" style="margin-bottom:0.5rem;">
            <div class="msg-avatar bot-av">⬡</div>
            <div style="flex:1;">
        """, unsafe_allow_html=True)

        if chat['df'] is not None:
            # Table result
            n = chat['df'].shape[0]
            height = min(n, 10) * 35 + 40
            st.dataframe(
                chat['df'],
                use_container_width=True,
                height=height,
                hide_index=True
            )

            # ── Chart ──
            with st.expander("📊 Visualize", expanded=False):
                if len(chat['columns']) >= 2:
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        chart_type = st.selectbox("Chart", ["Bar","Line","Area","Pie","Scatter"], key=f"ct_{i}")
                    with c2:
                        x_col = st.selectbox("X-axis", chat['columns'], key=f"xc_{i}")
                    with c3:
                        y_col = st.selectbox("Y-axis", chat['columns'], index=min(1, len(chat['columns'])-1), key=f"yc_{i}")

                    chart_df = chat['df'].copy()
                    try:
                        chart_df[y_col] = chart_df[y_col].astype(str).str.replace("%","").str.replace(",","")
                        chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors="coerce")
                    except:
                        pass

                    clr = {"color_discrete_sequence": ["#f59e0b","#06b6d4","#10b981","#ef4444","#8b5cf6"]}
                    plotly_theme = dict(
                        template="plotly_dark",
                        paper_bgcolor="#0f1525",
                        plot_bgcolor="#0a0e1a",
                        font=dict(color="#8899b4", family="DM Sans"),
                        margin=dict(l=10, r=10, t=30, b=10),
                    )

                    if chart_type == "Bar":
                        fig = px.bar(chart_df, x=x_col, y=y_col, **clr)
                    elif chart_type == "Line":
                        fig = px.line(chart_df, x=x_col, y=y_col, **clr)
                    elif chart_type == "Area":
                        fig = px.area(chart_df, x=x_col, y=y_col, **clr)
                    elif chart_type == "Pie":
                        fig = px.pie(chart_df, names=x_col, values=y_col, **clr)
                    else:
                        fig = px.scatter(chart_df, x=x_col, y=y_col, **clr)

                    fig.update_layout(**plotly_theme)
                    st.plotly_chart(fig, use_container_width=True)

        else:
            # Text result
            clean_ans = chat['answer'].replace("|", "&#124;").replace("\n", "<br>")
            st.markdown(f"""
            <div class="msg-bubble-bot">{clean_ans}</div>
            """, unsafe_allow_html=True)

        st.markdown("</div></div>", unsafe_allow_html=True)

        # ── AI Summary ──
        if chat.get('summary'):
            bullets_html = chat['summary'].replace("\n", "<br>")
            st.markdown(f"""
            <div class="summary-card">
                <div class="summary-label">⬡ AI Analysis</div>
                {bullets_html}
            </div>
            """, unsafe_allow_html=True)

        # ── Feedback row ──
        st.markdown("<div class='fb-divider'></div>", unsafe_allow_html=True)
        fb_col1, fb_col2, fb_col3 = st.columns([1, 1, 10])
        with fb_col1:
            if st.button("👍", key=f"up_{msg_id}"):
                st.session_state['feedback'][msg_id] = ("up", "")
                st.session_state['feedback_done'][msg_id] = True
        with fb_col2:
            if st.button("👎", key=f"dn_{msg_id}"):
                st.session_state['feedback'][msg_id] = ("down", "")
                st.session_state['feedback_done'][msg_id] = False
        with fb_col3:
            fb_thumb = st.session_state['feedback'].get(msg_id, (None,""))[0]
            fb_done  = st.session_state['feedback_done'].get(msg_id, False)
            if fb_thumb == "up" and fb_done:
                st.markdown("<span style='color:#10b981;font-size:0.8rem;font-family:\"Space Mono\",monospace;'>✓ Thanks for the feedback</span>", unsafe_allow_html=True)
            elif fb_thumb == "down" and not fb_done:
                comment = st.text_input("What went wrong?", key=f"fb_txt_{msg_id}", label_visibility="collapsed",
                                        placeholder="What went wrong? How can we improve?")
                if comment.strip():
                    save_feedback(chat["user"], chat["answer"], comment.strip())
                    st.session_state['feedback_done'][msg_id] = True
                    st.rerun()
            elif fb_thumb == "down" and fb_done:
                st.markdown("<span style='color:#10b981;font-size:0.8rem;font-family:\"Space Mono\",monospace;'>✓ Feedback saved — we'll improve</span>", unsafe_allow_html=True)

        # ── Suggested follow-ups ──
        sugg = chat.get('suggested', [])
        if sugg:
            st.markdown("<div class='suggest-label'>Try next</div>", unsafe_allow_html=True)
            chips = " ".join(f"<span class='suggest-chip'>{s}</span>" for s in sugg)
            st.markdown(chips, unsafe_allow_html=True)

        # ── Divider between messages ──
        if i < len(st.session_state['chat_history']) - 1:
            st.markdown("""
            <div class="section-divider">
                <div class="section-divider-line"></div>
                <div class="section-divider-text">earlier</div>
                <div class="section-divider-line"></div>
            </div>
            """, unsafe_allow_html=True)
