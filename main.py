"""Entry point: terminal investigation chatbot.

    python main.py              # chat in the terminal
    streamlit run app.py        # web UI
    python etl.py               # team ETL (evidence archiving + PostgreSQL polling)
"""
from chatbot_cli import main

if __name__ == "__main__":
    main()
