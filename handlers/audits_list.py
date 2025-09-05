# handlers/audits_list.py
import datetime as dt
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from db import sqlite_store as store

def _fmt_ts(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

async def audits_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    store.init()
    rows = store.list_audits(update.effective_chat.id, limit=5)
    if not rows:
        return await update.message.reply_text("No hay auditorías registradas aún.")

    lines = []
    for r in rows:
        s = r["summary"]
        # intenta extraer datos clave del resumen JSON
        try:
            inv = s.get("invalid_date", {}).get("count")
            dup = s.get("duplicates_tx", {}).get("count")
            unb = s.get("unbalanced_tx", {}).get("count")
            req = s.get("required_nulls", {}).get("count")
            headline = f"invalid_date={inv}, duplicates={dup}, unbalanced={unb}, required_nulls={req}"
        except Exception:
            headline = (s[:120] + "…") if isinstance(s, str) and len(s) > 120 else str(s)

        lines.append(
            f"#{r['id']} · {r['file_name']} · { _fmt_ts(r['created_at']) }\n"
            f"  {headline}\n"
            f"  {r['run_url'] or ''}"
        )

    await update.message.reply_text("Últimas auditorías:\n\n" + "\n".join(lines))

def register_handlers(app):
    app.add_handler(CommandHandler("audits", audits_cmd))
