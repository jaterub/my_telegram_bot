# scripts/check_token.py
import asyncio
import sys
from telegram.ext import Application
from telegram.error import InvalidToken, NetworkError
from config import load_token, setup_logging

async def check_token_async() -> int:
    token = load_token()
    app = Application.builder().token(token).build()
    await app.initialize()
    try:
        me = await app.bot.get_me()
        print("Bot OK ✅")
        print(f"id: {me.id} | username: @{me.username} | name: {me.first_name}")
        return 0
    finally:
        await app.shutdown()

def main() -> None:
    setup_logging("INFO")
    try:
        sys.exit(asyncio.run(check_token_async()))
    except InvalidToken:
        print("❌ Token inválido. Revisa/rota tu TELEGRAM_TOKEN y vuelve a probar.")
        sys.exit(1)
    except NetworkError as e:
        print(f"🌐 Error de red: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"⚠️ Error inesperado: {e}")
        sys.exit(99)

if __name__ == "__main__":
    main()
