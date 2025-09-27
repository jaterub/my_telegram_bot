
# db.py

import sqlite3
from pathlib import Path

DB_PATH = Path("audits.db")


def init_db():
    """Crea la tabla audits si no existe."""
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


def insert_audit(filename: str, summary_json: str):
    """Inserta una nueva auditoría en la base de datos."""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO audits (filename, summary_json)
            VALUES (?, ?)
        """, (filename, summary_json))
        conn.commit()


def get_last_audits(limit: int = 5):
    """Devuelve las últimas auditorías realizadas."""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT id, filename, created_at, summary_json
            FROM audits
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,))
        rows = c.fetchall()
    return rows
