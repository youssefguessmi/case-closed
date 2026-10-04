"""Blackwood Investigation Assistant - serving layer UI.

    streamlit run app.py
"""
import html

import pandas as pd
import pymysql
import streamlit as st

from serving.agent import InvestigatorAgent, QueryResult
from serving.llm import GemmaClient

st.set_page_config(page_title="Blackwood Case File", page_icon="🔎", layout="centered",
                   initial_sidebar_state="collapsed")

CSS = """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;800&family=Chakra+Petch:wght@400;600&family=Share+Tech+Mono&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #05060D; --panel: rgba(11,16,36,0.86); --line: #1B2A4A; --ink: #D8F6FF; --muted: #7C8DB5;
  --cyan: #00F0FF; --mag: #FF2BD6; --ok: #39FF88; --bad: #FF4D6D; --warn: #FFB547;
}
html, body, .stApp, [data-testid="stMarkdownContainer"], textarea, input, button {
  font-family: "Chakra Petch", "Segoe UI", sans-serif !important;
}
.stApp {
  background-color: var(--bg);
  background-image:
    radial-gradient(ellipse 60% 40% at 15% 0%, rgba(0,240,255,0.10), transparent 70%),
    radial-gradient(ellipse 50% 35% at 90% 5%, rgba(255,43,214,0.08), transparent 70%),
    linear-gradient(rgba(0,240,255,0.05) 1px, transparent 1px),
    linear-gradient(90deg, rgba(0,240,255,0.05) 1px, transparent 1px);
  background-size: auto, auto, 40px 40px, 40px 40px;
}
/* retro CRT scanlines */
.stApp::before { content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 1000;
  background: repeating-linear-gradient(0deg, rgba(255,255,255,0.022) 0 1px, transparent 1px 3px); }
header[data-testid="stHeader"] { background: transparent; }
[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] { display: none; }
.block-container { padding-top: 2.2rem; padding-bottom: 7rem; max-width: 54rem; }

/* header */
.casefile { display: flex; justify-content: space-between; align-items: flex-end; gap: 1rem; flex-wrap: wrap; }
.casefile .eyebrow { font: 400 0.8rem/1 "Share Tech Mono", monospace; letter-spacing: 0.14em;
  text-transform: uppercase; color: var(--cyan); margin-bottom: 0.7rem; opacity: 0.9; }
.casefile h1 { font-family: "Orbitron", sans-serif !important; font-weight: 800; font-size: 2.3rem; line-height: 1.05;
  letter-spacing: 0.06em; text-transform: uppercase; margin: 0; padding: 0; color: var(--ink);
  text-shadow: 0 0 10px rgba(0,240,255,0.55), 0 0 28px rgba(0,240,255,0.25); }
.casefile .sub { color: var(--muted); font-size: 0.95rem; margin-top: 0.6rem; }
.stamp { font: 400 0.85rem/1 "Share Tech Mono", monospace; letter-spacing: 0.22em; text-transform: uppercase;
  color: var(--mag); border: 1.5px solid var(--mag); padding: 0.45rem 0.7rem; border-radius: 3px;
  transform: rotate(-4deg); white-space: nowrap; text-shadow: 0 0 8px rgba(255,43,214,0.7);
  box-shadow: 0 0 12px rgba(255,43,214,0.35), inset 0 0 8px rgba(255,43,214,0.2); }
.neon-rule { height: 2px; margin: 1.1rem 0 1.2rem; background: linear-gradient(90deg, var(--cyan), var(--mag));
  box-shadow: 0 0 10px rgba(0,240,255,0.6); }

/* model status panel */
.hud { position: relative; background: var(--panel); border: 1px solid var(--line); border-radius: 4px;
  padding: 0.85rem 1rem 0.9rem; margin-bottom: 1.4rem; }
.hud::before, .hud::after { content: ""; position: absolute; width: 14px; height: 14px; border-color: var(--cyan); border-style: solid; }
.hud::before { top: -1px; left: -1px; border-width: 2px 0 0 2px; }
.hud::after { bottom: -1px; right: -1px; border-width: 0 2px 2px 0; }
.hud-title { display: flex; align-items: center; gap: 0.5rem; font: 400 0.72rem/1 "Share Tech Mono", monospace;
  letter-spacing: 0.18em; text-transform: uppercase; color: var(--muted); margin-bottom: 0.7rem; }
.dot { width: 8px; height: 8px; border-radius: 50%; background: var(--muted); }
.dot.live { background: var(--ok); box-shadow: 0 0 8px var(--ok); animation: pulse 1.8s ease-in-out infinite; }
@keyframes pulse { 50% { opacity: 0.35; } }
@media (prefers-reduced-motion: reduce) { .dot.live { animation: none; } }
.hud-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(8.5rem, 1fr)); gap: 0.7rem 1rem; }
.hud-cell .k { font: 400 0.66rem/1.2 "Share Tech Mono", monospace; letter-spacing: 0.12em; text-transform: uppercase; color: var(--muted); }
.hud-cell .v { font: 400 0.98rem/1.3 "Share Tech Mono", monospace; color: var(--cyan); font-variant-numeric: tabular-nums;
  text-shadow: 0 0 6px rgba(0,240,255,0.35); }
.hud-warn { margin-top: 0.75rem; font: 400 0.78rem/1.4 "Share Tech Mono", monospace; color: var(--warn); }

/* chat */
[data-testid="stChatMessage"] { background: var(--panel); border: 1px solid rgba(0,240,255,0.22); border-radius: 4px;
  padding: 1rem 1.1rem; margin-bottom: 0.9rem; box-shadow: 0 0 18px rgba(0,240,255,0.06); }
[data-testid="stChatMessage"]:has([data-testid="stChatMessageAvatarUser"]) { border-color: rgba(255,43,214,0.35);
  box-shadow: 0 0 18px rgba(255,43,214,0.06); }
[data-testid="stChatMessageAvatarUser"] { background: var(--bg); border: 1px solid var(--mag); color: var(--mag); }
[data-testid="stChatMessageAvatarAssistant"] { background: var(--bg); border: 1px solid var(--cyan); color: var(--cyan); }
[data-testid="stChatInput"] { border: 1px solid rgba(0,240,255,0.4); border-radius: 4px; background: var(--panel);
  box-shadow: 0 0 14px rgba(0,240,255,0.12); }
[data-testid="stBottomBlockContainer"] { background: var(--bg); }

/* verification stamp, evidence, query */
.verdict { display: inline-flex; gap: 0.5rem; align-items: center; margin: 0.6rem 0 0.2rem;
  font: 400 0.8rem/1 "Share Tech Mono", monospace; letter-spacing: 0.12em; text-transform: uppercase;
  padding: 0.45rem 0.65rem; border-radius: 3px; border: 1px solid currentColor; }
.verdict.ok { color: var(--ok); text-shadow: 0 0 8px rgba(57,255,136,0.6); box-shadow: 0 0 10px rgba(57,255,136,0.2); }
.verdict.bad { color: var(--bad); text-shadow: 0 0 8px rgba(255,77,109,0.6); box-shadow: 0 0 10px rgba(255,77,109,0.25); }
.verdict.na { color: var(--muted); }
.meta { font: 400 0.74rem/1.4 "Share Tech Mono", monospace; color: var(--muted); margin: 0.35rem 0 0.3rem; }
[data-testid="stExpander"] details { border-color: var(--line); background: rgba(5,6,13,0.6); border-radius: 3px; }
[data-testid="stExpander"] summary p { font: 400 0.82rem "Share Tech Mono", monospace !important;
  letter-spacing: 0.08em; text-transform: uppercase; color: var(--muted); }
code, pre { font-family: "Share Tech Mono", monospace !important; }

/* suggested questions */
.suggest-label { font: 400 0.74rem/1 "Share Tech Mono", monospace; letter-spacing: 0.16em; text-transform: uppercase;
  color: var(--muted); margin: 0.2rem 0 0.3rem; }
[data-testid="stButtonGroup"] button { background: var(--panel); border: 1px solid rgba(0,240,255,0.3); color: var(--ink);
  border-radius: 3px; }
[data-testid="stButtonGroup"] button:hover { border-color: var(--cyan); color: var(--cyan); box-shadow: 0 0 10px rgba(0,240,255,0.35); }
</style>
"""

HEADER = """
<div class="casefile">
  <div>
    <div class="eyebrow">// Case file 2026-0814 · Blackwood Technologies</div>
    <h1>Investigation Assistant</h1>
    <div class="sub">Ask about the evidence in plain English. Times are UTC.</div>
  </div>
  <div class="stamp">Classified</div>
</div>
<div class="neon-rule"></div>
"""

AVATAR = {"user": ":material/person:", "assistant": ":material/manage_search:"}

# Shown before the first question; each was answered correctly in testing.
SUGGESTIONS = [
    "Reconstruct the timeline for EMP-0047, EMP-0031 and EMP-0092 between 10 PM and midnight",
    "Does EMP-0031's alibi hold up?",
    "Was anyone in the server room after 11 PM?",
    "Who plugged in a USB device that night?",
    "Who was in the building between 10 PM and midnight?",
    "Did anyone email an external address that night?",
]


@st.cache_resource
def get_agent() -> InvestigatorAgent:
    return InvestigatorAgent(GemmaClient())


def hud_html(info: dict) -> str:
    """Model status panel: which model, its size, and where it is running."""
    def cell(k, v):
        return f'<div class="hud-cell"><div class="k">{k}</div><div class="v">{html.escape(str(v))}</div></div>'

    loaded = info.get("loaded_gb")
    cells = [cell("Model", info["model"])]
    if info["backend"] == "google":
        cells.append(cell("Runs on", "Google AI Studio"))
    else:
        family = (info.get("family") or "").replace("gemma", "Gemma ").strip()
        cells += [
            cell("Family", family or "-"),
            cell("Parameters", info.get("parameters") or "-"),
            cell("Quantization", info.get("quantization") or "-"),
            cell("Weights on disk", f"{info['disk_gb']:.2f} GB" if info.get("disk_gb") else "-"),
            cell("Memory in use", f"{loaded:.2f} GB" if loaded else "idle"),
            cell("On GPU", f"{info['gpu_share']:.0%}" if loaded else "-"),
            cell("Context", f"{info['context']:,} tokens" if info.get("context") else "-"),
        ]
    if info.get("nvidia_name"):
        gpu = info["nvidia_name"].replace("NVIDIA GeForce ", "").replace(" Laptop GPU", " Laptop")
        cells.append(cell(gpu, f"{info['nvidia_used_gb']:.1f} / {info['nvidia_total_gb']:.1f} GB"))

    warn = ""
    if loaded and info.get("gpu_share", 0) > 0.5 and info.get("nvidia_used_gb") is not None \
            and info["nvidia_used_gb"] < loaded * 0.5:
        warn = ('<div class="hud-warn">⚠ The model is on another GPU (likely integrated graphics), '
                'not the NVIDIA card. Turn off Vulkan in Ollama settings for faster answers.</div>')
    status = "Loaded" if loaded else "Idle · loads on first question"
    return (f'<div class="hud"><div class="hud-title"><span class="dot{" live" if loaded else ""}"></span>'
            f'Model status · {status}</div><div class="hud-grid">{"".join(cells)}</div>{warn}</div>')


def render_evidence(res: QueryResult):
    """Verification stamp, evidence table and the SQL used, beneath an answer."""
    if res.ok:
        c = res.custody
        if c.checked and c.ok:
            stamp = f'<span class="verdict ok">✓ Verified · {c.verified}/{c.checked} records match source</span>'
        elif c.failures:
            stamp = (f'<span class="verdict bad">✗ Tampering detected · {len(c.failures)}/{c.checked} '
                     f'records do not match source</span>')
        else:
            stamp = '<span class="verdict na">Summary result · no per-record check</span>'
        st.markdown(stamp, unsafe_allow_html=True)
        if res.rows:
            with st.expander(f"Evidence · {len(res.rows)} record{'s' if len(res.rows) != 1 else ''}"):
                st.dataframe(pd.DataFrame(res.rows, columns=res.columns), hide_index=True, width="stretch")
        if c.failures:
            with st.expander("Failed records", expanded=True):
                st.json(c.failures)
    with st.expander("Query used", expanded=not res.ok):
        st.code(res.sql or "-", language="sql")
        if len(res.attempts) > 1:
            st.caption(f"Gemma needed {len(res.attempts)} attempts; earlier ones were rejected.")
    meta = f"Audit log entry #{res.audit_id}" if res.audit_id else ""
    if res.audit_error:
        meta = f"Audit log failed: {html.escape(res.audit_error)}"
    if meta:
        st.markdown(f'<div class="meta">{meta}</div>', unsafe_allow_html=True)


def handle(question: str):
    agent = get_agent()
    st.session_state.turns.append({"role": "user", "content": question})
    with st.chat_message("user", avatar=AVATAR["user"]):
        st.markdown(question)
    with st.chat_message("assistant", avatar=AVATAR["assistant"]):
        history = [{"question": t["result"].question, "sql": t["result"].sql}
                   for t in st.session_state.turns if t["role"] == "assistant" and t["result"].ok]
        with st.spinner("Searching the records..."):
            res = agent.run(question, history)
        answer = st.write_stream(agent.explain(res))
        render_evidence(res)
    st.session_state.turns.append({"role": "assistant", "result": res, "answer": answer})


# ---------------- page ----------------
# Blank lines would end Markdown's HTML block and print the CSS as text.
st.markdown("\n".join(line for line in CSS.splitlines() if line.strip()), unsafe_allow_html=True)
st.markdown(HEADER, unsafe_allow_html=True)
hud_slot = st.empty()  # filled at the end of the run, so it shows the model after any answer loads it

if "turns" not in st.session_state:
    st.session_state.turns = []

ok, msg = get_agent().llm.health()
if not ok:
    st.error(f"Gemma is not available: {msg}")

for t in st.session_state.turns:
    with st.chat_message(t["role"], avatar=AVATAR[t["role"]]):
        if t["role"] == "user":
            st.markdown(t["content"])
        else:
            st.markdown(t["answer"] or "")
            render_evidence(t["result"])

picked = None
suggestion_slot = st.empty()
if not st.session_state.turns:
    with suggestion_slot.container():
        st.markdown('<div class="suggest-label">Suggested questions</div>', unsafe_allow_html=True)
        picked = st.pills("Suggested questions", SUGGESTIONS, label_visibility="collapsed", key="suggestion")

question = st.chat_input("Ask about the night of August 14...") or picked
if question:
    suggestion_slot.empty()
    try:
        handle(question)
    except pymysql.MySQLError as e:
        st.error(f"Database connection failed: {e}. Is the MySQL92 service running?")

hud_slot.markdown(hud_html(get_agent().llm.model_info()), unsafe_allow_html=True)
