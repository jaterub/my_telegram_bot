# db/sqlite_store.py
# Persistencia mínima en SQLite para logs de auditoría.
# - Archivo: data/bot.db
# - Tabla: audits

import json, os, sqlite3, time
from pathlib import Path
from typing import List, Dict, Any, Optional

DB_DIR = Path("data")
DB_PATH = DB_DIR / "bot.db"

DDL = """
CREATE TABLE IF NOT EXISTS audits (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_id INTEGER NOT NULL,
  file_name TEXT NOT NULL,
  run_id INTEGER,
  run_url TEXT,
  summary_json TEXT NOT NULL,
  created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audits_chat_created ON audits(chat_id, created_at DESC);
"""

def _connect() -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")     # mejor concurrencia básica
    conn.execute("PRAGMA synchronous=NORMAL;")   # rendimiento razonable
    return conn

def init() -> None:
    """Crea tablas si no existen."""
    with _connect() as con:
        con.executescript(DDL)

def save_audit(
    chat_id: int,
    file_name: str,
    summary: dict | str,
    run_id: Optional[int],
    run_url: Optional[str],
) -> int:
    """Inserta una auditoría. summary puede ser dict (se serializa) o str JSON."""
    if isinstance(summary, dict):
        summary_json = json.dumps(summary, ensure_ascii=False)
    else:
        summary_json = summary

    with _connect() as con:
        cur = con.execute(
            "INSERT INTO audits(chat_id, file_name, run_id, run_url, summary_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (chat_id, file_name, run_id, run_url, summary_json, time.time()),
        )
        return cur.lastrowid

def list_audits(chat_id: int, limit: int = 10) -> List[Dict[str, Any]]:
    """Devuelve últimas auditorías de un chat."""
    with _connect() as con:
        rows = con.execute(
            "SELECT id, file_name, run_id, run_url, summary_json, created_at "
            "FROM audits WHERE chat_id=? ORDER BY created_at DESC LIMIT ?",
            (chat_id, limit),
        ).fetchall()
    out = []
    for (id_, fname, rid, rurl, sj, ts) in rows:
        # intenta parsear JSON
        try:
            summary = json.loads(sj)
        except Exception:
            summary = sj
        out.append({
            "id": id_, "file_name": fname, "run_id": rid, "run_url": rurl,
            "summary": summary, "created_at": ts
        })
    return out
