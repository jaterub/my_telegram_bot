# scripts/set_token.py
"""
Script CLI mínimo y robusto para guardar/rotar el TELEGRAM_TOKEN en .env.
Uso:
  1) Interactivo (te pedirá el token si no lo pasas):
     python scripts/set_token.py

  2) Por argumento:
     python scripts/set_token.py -t "123456789:AA...tu_token..."

  3) Por variable de entorno:
     (Windows) set TELEGRAM_TOKEN=123:AA...
     (Linux/Mac) export TELEGRAM_TOKEN=123:AA...
     python scripts/set_token.py

Notas:
- No subas .env a git (añádelo a .gitignore).
- Si mostraste tu token antes, rótalo en @BotFather y guarda el nuevo.
"""

import argparse
import os
import re
from getpass import getpass
from dotenv import set_key

ENV_FILE = ".env"                 # Archivo de configuración local (no versionar)
ENV_KEY = "TELEGRAM_TOKEN"        # Clave que usaremos en .env
# Validación básica del formato del token: <digits>:<token>
TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{20,}$")

def save_token(token: str, env_file: str = ENV_FILE) -> None:
    """Guarda/actualiza el token en .env y lo deja en el entorno del proceso."""
    token = (token or "").strip()
    if not TOKEN_RE.match(token):
        raise ValueError("Token inválido o con formato inesperado.")
    # Deja el token disponible en este proceso (útil si luego ejecutas otro script).
    os.environ[ENV_KEY] = token
    # Persiste el token en .env (para futuras ejecuciones con load_dotenv()).
    set_key(env_file, ENV_KEY, token)
    print(f"[OK] {ENV_KEY} guardado/actualizado en {env_file}")

def main() -> None:
    parser = argparse.ArgumentParser(description="Guardar/rotar TELEGRAM_TOKEN en .env")
    parser.add_argument("-t", "--token", help="Token del bot de Telegram")
    args = parser.parse_args()

    # Orden de preferencia: argumento CLI > variable de entorno > entrada interactiva
    token = args.token or os.environ.get(ENV_KEY) or getpass("Pega tu TELEGRAM_TOKEN: ")
    save_token(token)

if __name__ == "__main__":
    main()
