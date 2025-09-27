# handlers/audits_list.py
# ────────────────────────────────────────────────
# Lista las últimas auditorías guardadas en SQLite
# Comando: /audits
# ────────────────────────────────────────────────

import datetime as dt
import json
from telegram import Update
from telegram.ext import ContextTypes, CommandHandler
from db import sqlite_store as store


def _fmt_ts(ts: float) -> str:
    """Convierte timestamp en string legible."""
    try:
        return dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


async def audits_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra las últimas auditorías del usuario."""
    store.init()
    rows = store.list_audits(update.effective_chat.id, limit=5)

    if not rows:
        return await update.message.reply_text("No hay auditorías registradas aún.")

    lines = []
    for r in rows:
        summary = r["summary"]
        headline = ""

        try:
            # Si summary es JSON válido → parseamos
            if isinstance(summary, str):
                summary = json.loads(summary)

            inv = summary.get("invalid_date", {}).get("count", 0)
            dup = summary.get("duplicates_tx", {}).get("count", 0)
            unb = summary.get("unbalanced_tx", {}).get("count", 0)
            req = summary.get("required_nulls", {}).get("count", 0)

            headline = f"invalid_date={inv}, duplicates={dup}, unbalanced={unb}, required_nulls={req}"
        except Exception:
            # Si no es JSON → mostramos como texto plano truncado
            if isinstance(summary, str):
                headline = (summary[:120] + "…") if len(summary) > 120 else summary
            else:
                headline = str(summary)

        # Construir línea de salida
        lines.append(
            f"#{r['id']} · {r['file_name']} · { _fmt_ts(r['created_at']) }\n"
            f"  {headline}\n"
            f"  {r.get('run_url', '')}"
        )

    await update.message.reply_text("📊 Últimas auditorías:\n\n" + "\n".join(lines))


def register_handlers(app):
    """Registra el comando /audits en el bot."""
    app.add_handler(CommandHandler("audits", audits_cmd))
