# utils/db.py
import sqlite3
from pathlib import Path
from typing import Optional, List, Tuple

DB_PATH = Path("audits.db")

def _ensure_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(audits)")
    cols = {row[1] for row in cur.fetchall()}  # set de nombres de columna

    # columnas nuevas si faltan
    if "run_url" not in cols:
        cur.execute("ALTER TABLE audits ADD COLUMN run_url TEXT")
    if "llm_summary" not in cols:
        cur.execute("ALTER TABLE audits ADD COLUMN llm_summary TEXT")
    conn.commit()

def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS audits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                summary_json TEXT
            )
        """)
        conn.commit()
        _ensure_columns(conn)

def insert_audit(
    filename: str,
    summary_json: str,
    run_url: Optional[str] = None,
    llm_summary: Optional[str] = None,
) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO audits (filename, summary_json, run_url, llm_summary)
            VALUES (?, ?, ?, ?)
        """, (filename, summary_json, run_url, llm_summary))
        conn.commit()

def get_last_audits(limit: int = 5) -> List[Tuple]:
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, filename, created_at, summary_json, run_url, llm_summary
            FROM audits
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        return c.fetchall()
