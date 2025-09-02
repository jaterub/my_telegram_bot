# app.py
# ─────────────────────────────────────────────────────────────────────────────
# APP mínima con logging + token desde .env + 3 handlers (/start, /help, /health)
# ─────────────────────────────────────────────────────────────────────────────

import logging                           # ➊ logging estándar para ver qué ocurre
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    MessageHandler, filters,           # ← IMPORTANTE
) 
from telegram import Update
from config import setup_logging, load_token  # ➋ usamos tu config centralizada
from telegram.error import NetworkError
import time
#


# 2) Handlers (funciones asíncronas que responden a comandos)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ➐ Mensaje de bienvenida simple
    await update.message.reply_text("¡Hola! Bot en que puedo hacer por ti ✅ (usa /help), cazoleta")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ➑ Pequeña ayuda con los comandos disponibles
    await update.message.reply_text("Comandos:\n/start — saludo\n/help — ayuda\n/health — estado")

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ➒ Endpoint de “salud” para verificar que el bot responde
    await update.message.reply_text("OK")

# ✦ Handler global de errores: loggea pero no tumba el proceso
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Error en handler: %s | update=%r", context.error, update)


# /say <mensaje> → responde con lo que le pases
async def say(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # context.args = lista de palabras detrás del comando
    
    msg = " ".join(context.args) if context.args else "(vacío)"
    await update.message.reply_text(f"Dijiste: {msg}")

# eco de cualquier texto que no sea comando
async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(f"Eco: {update.message.text}")


# --- handlers de comandos ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("¡Hola! Bot en que puedo hacer por ti ✅ (usa /help), cazoleta")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Comandos:\n/start — saludo\n/help — ayuda\n/health — estado\n/say <msg>\n/echo <msg>")

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("OK")

async def say(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = " ".join(context.args) if context.args else "(vacío)"
    await update.message.reply_text(f"Dijiste: {msg}")

async def echo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # /echo <mensaje> (como comando)
    msg = " ".join(context.args) if context.args else "(vacío)"
    await update.message.reply_text(f"Eco: {msg}")

# --- handler de texto no-comando ---
async def echo_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # cualquier texto que NO sea comando
    await update.message.reply_text(f"Eco: {update.message.text}")

# --- error handler global ---
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Error en handler: %s | update=%r", context.error, update)

def build_app() -> Application:
    from config import setup_logging, load_token
    setup_logging("INFO")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    token = load_token()

    app = Application.builder().token(token).build()

    # 1) comandos primero
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("health", health))
    app.add_handler(CommandHandler("say", say))
    app.add_handler(CommandHandler("echo", echo_cmd))     # ← /echo como comando

    # 2) luego texto no-comando
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo_text))

    # errores
    app.add_error_handler(on_error)
    return app

app = build_app()

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