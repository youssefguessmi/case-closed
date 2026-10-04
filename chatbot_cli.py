"""Terminal version of the investigation chatbot.

    python chatbot_cli.py
"""
from serving.agent import InvestigatorAgent


def main():
    agent = InvestigatorAgent()
    ok, msg = agent.llm.health()
    print(f"LLM: {msg}")
    if not ok:
        return
    history = []
    print("Ask about the Blackwood evidence (times are UTC). Type 'quit' to exit.\n")
    while True:
        try:
            q = input("investigator> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if q.lower() in {"quit", "exit"}:
            break
        if not q:
            continue
        res = agent.run(q, history)
        print(f"\n-- SQL ({len(res.attempts)} attempt(s)) --\n{res.sql}\n")
        if res.ok:
            history.append({"question": q, "sql": res.sql})
            print(f"-- {len(res.rows)} rows · {res.custody_note()} --")
        for chunk in agent.explain(res):
            print(chunk, end="", flush=True)
        print(f"\n\n[audit entry #{res.audit_id}]\n" if res.audit_id else "\n")


if __name__ == "__main__":
    main()
