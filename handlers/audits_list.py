# handlers/audits_list.py
# ------------------------------------------------------------
# Lista las últimas auditorías guardadas en SQLite (utils.db)
# Comando: /audits
# ------------------------------------------------------------

import datetime as dt
import json
from telegram import Update
from telegram.ext import CommandHandler, ContextTypes
from utils.db import get_last_audits, init_db


def _fmt_ts(ts) -> str:
    """Convierte timestamp (ISO o epoch) a un formato legible."""
    if ts is None:
        return "-"
    # Intento con ISO 8601 (str)
    try:
        return dt.datetime.fromisoformat(str(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    # Intento con epoch (float/int)
    try:
        return dt.datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


async def audits_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Muestra las últimas auditorías (incluye ejecuciones en curso)."""
    init_db()
    rows = get_last_audits(limit=6)

    if not rows:
        return await update.message.reply_text("📭 No hay auditorías registradas aún.")

    lines = []
    for (aid, filename, created_at, summary_json, run_url, llm_summary) in rows:
        headline = ""
        summary = {}

        if summary_json:
            try:
                summary = json.loads(summary_json)
            except Exception:
                summary = {}

        if summary and summary.get("status") == "running":
            run_id = summary.get("run_id", "?")
            headline = f"⏳ En ejecución (run_id={run_id})"
        elif summary:
            inv = summary.get("invalid_date", {}).get("count", 0)
            dup = summary.get("duplicates_tx", {}).get("count", 0)
            unb = summary.get("unbalanced_tx", {}).get("count", 0)
            req = summary.get("required_nulls", {}).get("count", 0)
            inv_cur = summary.get("invalid_currency", {}).get("count", 0)
            headline = (
                f"🔁 Duplicadas: {dup}  •  ⚖️ Desbalanceadas: {unb}\n"
                f"📅 Fechas inválidas: {inv}  •  🧱 Requeridos nulos: {req}\n"
                f"💱 Monedas inválidas: {inv_cur}"
            )
        elif llm_summary:
            headline = llm_summary.splitlines()[0][:120]
        else:
            headline = "(sin resumen disponible)"

        lines.append(
            f"📄 #{aid} · {filename}\n"
            f"   🕒 {_fmt_ts(created_at)}\n"
            f"   {headline}\n"
            f"   🔗 {run_url or 'Sin enlace'}"
        )

    body = "\n".join(lines)
    await update.message.reply_text(f"📊 Últimas auditorías\n\n{body}")


def register_handlers(app):
    """Registra el comando /audits en el bot."""
    app.add_handler(CommandHandler("audits", audits_cmd))
