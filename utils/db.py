# utils/db.py
import os
import sqlite3
import time
from pathlib import Path
from typing import List, Optional, Tuple
# ============================================================
# Base configuration
# ============================================================
# Permite sobrescribir la ruta con variable de entorno SQLITE_PATH
DB_PATH = Path(os.getenv("SQLITE_PATH", "audits.db"))

# SQL del esquema limpio
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    created_at REAL DEFAULT (strftime('%s','now')),
    summary_json TEXT,
    run_url TEXT,
    llm_summary TEXT
);
"""

INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_audits_created_at ON audits(created_at DESC);"


# ============================================================
# 🔹 Inicialización y verificación de estructura
# ============================================================
def init_db() -> None:
    """Inicializa la base de datos asegurando el esquema esperado."""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.executescript(SCHEMA_SQL)
        c.execute(INDEX_SQL)
        conn.commit()
    print("[DB] Inicializada correctamente (estructura limpia).")


def init_db_upgrade() -> None:
    """Idempotente: garantiza que el esquema está actualizado."""
    init_db()
    print("[DB] Upgrade completado (estructura revisada).")


# ============================================================
# 🔹 Inserta una nueva auditoría
# ============================================================
def insert_audit(
    filename: str,
    summary_json: str,
    run_url: Optional[str] = None,
    llm_summary: Optional[str] = None,
) -> None:
    """Inserta o actualiza la auditoría asociada al run_url indicado."""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        now = time.time()

        updated_existing = False
        if run_url:
            c.execute(
                "SELECT id FROM audits WHERE run_url = ? ORDER BY id DESC LIMIT 1",
                (run_url,),
            )
            row = c.fetchone()
            if row:
                c.execute(
                    """
                    UPDATE audits
                    SET filename = ?, summary_json = ?, run_url = ?, llm_summary = ?, created_at = ?
                    WHERE id = ?
                    """,
                    (filename, summary_json, run_url, llm_summary, now, row[0]),
                )
                updated_existing = True

        if not updated_existing:
            c.execute(
                """
                INSERT INTO audits (filename, summary_json, run_url, llm_summary, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (filename, summary_json, run_url, llm_summary, now),
            )

        conn.commit()
    print(f"[DB] Auditoría registrada/actualizada: {filename}")


# ============================================================
# 🔹 Recupera las últimas auditorías
# ============================================================
def get_last_audits(limit: int = 5) -> List[Tuple]:
    """Devuelve las últimas auditorías registradas."""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute(
            """
            SELECT
              id,
              filename,
              datetime(created_at, 'unixepoch') AS created_at,
              summary_json,
              run_url,
              llm_summary
            FROM audits
            WHERE id IN (
              SELECT MAX(id) FROM audits WHERE run_url IS NOT NULL GROUP BY run_url
            )
            OR run_url IS NULL
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return c.fetchall()
