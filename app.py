# app.py
# ─────────────────────────────────────────────────────────────────────────────
# Bot PTB: /start /help /health /say /echo + registro de /audit y /audits
# ─────────────────────────────────────────────────────────────────────────────

import logging, time
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram import Update
from telegram.error import NetworkError

from config import setup_logging, load_token
from db import sqlite_store as store
from handlers.audit import register_handlers as register_audit
from handlers.audits_list import register_handlers as register_audits_list

# ---- comandos básicos ----

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
        "/audit — sube un CSV para auditar\n"
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

# ---- construcción de la app ----

def build_app() -> Application:
    setup_logging("INFO")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    token = load_token()

    app = Application.builder().token(token).build()

    # comandos
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("say", say))
    app.add_handler(CommandHandler("echo", echo_cmd))

    # texto no-comando
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo_text))

    # errores
    app.add_error_handler(on_error)
    return app

app = build_app()

# registra handlers externos y base de datos
store.init()
register_audit(app)
register_audits_list(app)

# ---- arranque ----
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
