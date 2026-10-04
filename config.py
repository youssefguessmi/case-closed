"""Central configuration, read from environment / .env."""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))

# Admin account: only used by the pipeline loader (creates schemas, users, writes data).
MYSQL_ADMIN_USER = os.getenv("MYSQL_ADMIN_USER", "root")
MYSQL_ADMIN_PASSWORD = os.getenv("MYSQL_ADMIN_PASSWORD", "")

# Read-only account: the ONLY account the chatbot uses to run generated SQL.
MYSQL_RO_USER = os.getenv("MYSQL_RO_USER", "investigator_ro")
MYSQL_RO_PASSWORD = os.getenv("MYSQL_RO_PASSWORD", "change-me-ro")

# Audit writer: may only INSERT into audit.query_log.
MYSQL_AUDIT_USER = os.getenv("MYSQL_AUDIT_USER", "audit_writer")
MYSQL_AUDIT_PASSWORD = os.getenv("MYSQL_AUDIT_PASSWORD", "change-me-audit")

# LLM backend: "ollama" (local Gemma) or "google" (Gemma via Google AI Studio API).
LLM_BACKEND = os.getenv("LLM_BACKEND", "ollama")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:4b")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
GOOGLE_MODEL = os.getenv("GOOGLE_MODEL", "gemma-3-27b-it")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

MAX_ROWS = int(os.getenv("MAX_ROWS", "500"))
QUERY_TIMEOUT_MS = int(os.getenv("QUERY_TIMEOUT_MS", "5000"))
MAX_SQL_RETRIES = int(os.getenv("MAX_SQL_RETRIES", "2"))

DATA_FILES = {
    "badge": ROOT / "badge_access.csv",
    "device": ROOT / "device_logs.log",
    "transaction": ROOT / "building_transactions.sql",
}
