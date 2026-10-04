"""Entry point for the investigation chatbot.

    streamlit run main.py       # web UI (same as: streamlit run app.py)
    python main.py              # chat in the terminal
    python etl.py               # team ETL (evidence archiving + PostgreSQL polling)
"""
import runpy
from pathlib import Path


def _running_under_streamlit() -> bool:
    try:
        from streamlit import runtime
        return runtime.exists()
    except ImportError:
        return False


if _running_under_streamlit():
    # Streamlit re-executes this file on every interaction, so run the page each time.
    runpy.run_path(str(Path(__file__).with_name("app.py")), run_name="__main__")
elif __name__ == "__main__":
    from chatbot_cli import main
    main()
