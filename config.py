# config.py
# ─────────────────────────────────────────────────────────────────────────────
# Propósito:
# - Centralizar configuración transversal (logging y carga del TELEGRAM_TOKEN).
# - Sin efectos secundarios al importar (no escribe .env, no pide input).
# ─────────────────────────────────────────────────────────────────────────────

import os                    # Leer variables de entorno (os.environ / os.getenv)
import logging               # Configuración de logs estándar en toda la app
from typing import Optional  # Para tipar que un str puede ser None (Optional[str])
from dotenv import load_dotenv  # Carga variables desde un archivo .env al entorno


def setup_logging(level: str = "INFO") -> None:
    """
    Configura el logging global de la aplicación.
    - Debe llamarse una sola vez (en app.py normalmente).
    - 'level' acepta 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.
    """
    logging.basicConfig(
        # Convierte el string a constante de logging (p.ej. "INFO" -> logging.INFO).
        level=getattr(logging, level.upper(), logging.INFO),
        # Formato de línea de log:
        #  - %(asctime)s  → marca de tiempo
        #  - %(levelname)s→ nivel (INFO/ERROR...)
        #  - %(name)s     → logger que emite el mensaje
        #  - %(message)s  → contenido del mensaje
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def load_token() -> str:
    """
    Carga .env al entorno y devuelve TELEGRAM_TOKEN.
    - 'load_dotenv()' lee el archivo .env (en el cwd o sus padres) y carga
      pares KEY=VALUE en os.environ, sin sobrescribir variables ya existentes.
    - Si no existe la clave, fallamos pronto (fail-fast) con un RuntimeError.
    """
    # Carga perezosa: solo inyecta al entorno si aún no existe la variable.
    # Si quisieras que .env SOBREESCRIBA el entorno, usa load_dotenv(override=True).
    load_dotenv()

    # os.getenv devuelve str o None → por eso usamos Optional[str] en el tipo.
    token: Optional[str] = os.getenv("TELEGRAM_TOKEN")

    # Validación mínima: fallar pronto ayuda a detectar mal config al inicio.
    if not token:
        raise RuntimeError("Falta TELEGRAM_TOKEN. Usa scripts/set_token.py o crea .env")

    return token
