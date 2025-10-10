# handlers/history.py
import json
import datetime as dt
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from utils.db import get_last_audits

def _fmt_ts(ts_str: str) -> str:
    # ts de SQLite (YYYY-MM-DD HH:MM:SS)
    try:
        dt_obj = dt.datetime.fromisoformat(ts_str)
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts_str

async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_last_audits(limit=5)
    if not rows:
        await update.message.reply_text("No hay auditorías registradas todavía.")
        return

    parts = []
    for (aid, filename, created_at, summary_json, run_url, llm_summary) in rows:
        title = f"#{aid} · {filename} · { _fmt_ts(created_at) }"
        if llm_summary:
            parts.append(f"{title}\n{llm_summary}\n{run_url or ''}\n")
        else:
            parts.append(f"{title}\n{run_url or ''}\n")

    await update.message.reply_text("\n".join(parts).strip())

def register_handlers(app):
    app.add_handler(CommandHandler("history", history_cmd))
