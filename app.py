# app.py
# ─────────────────────────────────────────────────────────────
# Bot PTB con comandos básicos + auditoría de Excel (Databricks)
# ─────────────────────────────────────────────────────────────

import logging, time
from dotenv import load_dotenv

#  .env debe cargarse antes de importar handlers
load_dotenv()

from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram import Update
from telegram.error import NetworkError

from config import setup_logging, load_token
from db import sqlite_store as store
from handlers.audit_xlsx import register_handlers as register_audit
from handlers.audits_list import register_handlers as register_audits_list
from utils.db import init_db

# ───llm--------

from handlers.history import register_handlers as register_history
from handlers.llm_chat import register_handlers as register_llm_chat

# ─── Comandos básicos ───────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("¡Hola! Bot listo ✅ (usa /help)")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Comandos:\n"
        "/start — saludo\n"
        "/help — ayuda\n"
        "/health — estado\n"
        "/say <msg>\n"
        "/echo <msg>\n"
        "/audit — sube un Excel para auditar\n"
        "/history — ver historial de auditorías\n"
        "/audits — ver últimos resultados"
    )

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("OK")

async def say(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = " ".join(context.args) if context.args else "(vacío)"
    await update.message.reply_text(f"Dijiste: {msg}")

async def echo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = " ".join(context.args) if context.args else "(vacío)"
    await update.message.reply_text(f"Eco: {msg}")

async def echo_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Eco: {update.message.text}")

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Error en handler: %s | update=%r", context.error, update)


# ─── Construcción de la app ─────────────────────────────────
def build_app() -> Application:
    load_dotenv()
    setup_logging("INFO")
    logging.getLogger("httpx").setLevel(logging.WARNING)

    token = load_token()
    app = Application.builder().token(token).build()

    # ─── comandos principales ──────────────────────────────────────
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("say", say))
    app.add_handler(CommandHandler("echo", echo_cmd))
    app.add_error_handler(on_error)

    return app


# ─── Arranque ───────────────────────────────────────────────

app = build_app()


# ─── Inicialización de DB y registro de handlers ─────────────────────
init_db()                     # asegura tabla audits en SQLite
register_audit(app)           # /audit → subida de Excel y auditoría
register_audits_list(app)     # /audits → últimos resultados simples
register_history(app)         # /history → historial con llm_summary si existe
register_llm_chat(app)        # /chat, /reset → conversación con LLM (texto/voz)




if __name__ == "__main__":
    while True:
        try:
            app.run_polling(close_loop=False)
            break
        except NetworkError as e:
            logging.warning("Conectividad inestable (%s). Reintentamos en 5 s…", e)
            time.sleep(5)
        except Exception:
            logging.exception("Error no controlado. Abortando.")
            break
