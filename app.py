# app.py
# ─────────────────────────────────────────────────────────────────────────────
# APP mínima con logging + token desde .env + 3 handlers (/start, /help, /health)
# ─────────────────────────────────────────────────────────────────────────────

import logging                           # ➊ logging estándar para ver qué ocurre
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram import Update
from config import setup_logging, load_token  # ➋ usamos tu config centralizada

# 1) Configuración básica
setup_logging("INFO")                    # ➌ formato de logs y nivel INFO
logging.getLogger("httpx").setLevel(logging.WARNING)  # ➍ menos ruido de httpx

TOKEN = load_token()                     # ➎ lee TELEGRAM_TOKEN de .env / entorno
assert TOKEN, "Falta TELEGRAM_TOKEN"     # ➏ fail-fast si no hay token

# 2) Handlers (funciones asíncronas que responden a comandos)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ➐ Mensaje de bienvenida simple
    await update.message.reply_text("¡Hola! Bot en que puedo hacer por ti ✅ (usa /help)")

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ➑ Pequeña ayuda con los comandos disponibles
    await update.message.reply_text("Comandos:\n/start — saludo\n/help — ayuda\n/health — estado")

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # ➒ Endpoint de “salud” para verificar que el bot responde
    await update.message.reply_text("OK")

# ✦ Handler global de errores: loggea pero no tumba el proceso
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Error en handler: %s | update=%r", context.error, update)

def build_app() -> Application:
    setup_logging("INFO")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    token = load_token()
    # (Opcional) timeouts algo más holgados para redes inestables
    app = (
        Application.builder()
        .token(token)
        # .connect_timeout(10).read_timeout(30).write_timeout(10).pool_timeout(10)  # descomenta si lo quieres
        .build()
    )
    # Registra handlers de comandos
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("health", health))
    # Registra el manejador de errores
    app.add_error_handler(on_error)
    return app

app = build_app()

if __name__ == "__main__":
    # ✦ Bucle de reintento ante caídas de red
    while True:
        try:
            app.run_polling(close_loop=False)
            break  # salió limpio (p.ej. Ctrl+C)
        except NetworkError as e:
            logging.warning("Conectividad inestable (%s). Reintentamos en 5 s…", e)
            time.sleep(5)
            continue
        except Exception:
            logging.exception("Error no controlado. Abortando.")
            break