# Auditor Master Bot

[![Demo del Bot](demo/thumbnail_bot.png)](demo/bot_demo.mp4)

## Contenido

-   [Descripción general](#descripción-general)
-   [Flujo de trabajo](#flujo-de-trabajo)
-   [Comandos disponibles](#comandos-disponibles)
-   [Arquitectura técnica](#arquitectura-técnica)
-   [Configuración y despliegue](#configuración-y-despliegue)
-   [Guía rápida de contribución](#guía-rápida-de-contribución)

## Descripción general

Auditor Master Bot es un asistente de Telegram orientado a auditorías contables. Permite subir archivos Excel, los procesa en Databricks y muestra los hallazgos de forma resumida, incluyendo un resumen generado por LLM. Además, mantiene un histórico de resultados en SQLite y expone utilidades para consultar el estado reciente de las auditorías.

## Flujo de trabajo

1. **Subida de Excel**: el usuario envía el fichero `.xlsx` al bot.
2. **Workspace Databricks**: el archivo se carga en Workspace Files y se lanza un job (`audit_xlsx_workspace_summary`).
3. **Procesamiento**: el notebook calcula duplicados, desbalances, monedas inválidas, etc.
4. **Registro en SQLite**: `utils/db.py` guarda (o actualiza) el resultado en `audits.db`.
5. **Resumen LLM**: se genera un informe amigable con `utils/llm.summarize_audit_spanish`.
6. **Respuesta al usuario**: el bot devuelve el resumen y deja la información disponible para futuras consultas.

## Comandos disponibles

-   `/audit`: sube un Excel y ejecuta la auditoría completa.
-   `/audits`: lista las últimas auditorías con métricas clave.
-   `/history`: historial compacto con `llm_summary` si está disponible.
-   `/session`: muestra los archivos procesados en la sesión actual.
-   `/chat`: activa el modo conversación con el LLM (texto y voz).
-   `/reset`: limpia el historial de conversación del LLM.

Los mensajes sin comando también se envían al LLM cuando el modo chat está activo.

## Arquitectura técnica

-   **Handlers de Telegram** (`handlers/`):
    -   `audit_xlsx.py`: flujo principal de auditoría y registro en SQLite.
    -   `audits_list.py`, `history.py`: comandos de consulta.
    -   `llm_chat.py`: conversación contextual usando los resultados más recientes.
-   **Integraciones**:
    -   `utils/databricks_upload.py` y `wsfiles_check.py`: interacción con Workspace Files.
    -   `utils/llm.py`: acceso a OpenAI (chat y Whisper).
-   **Persistencia**:
    -   `utils/db.py`: esquema único (tabla `audits`) con upsert por `run_url`.
    -   `audits.db`: base SQLite alojada localmente (ruta configurable via `SQLITE_PATH`).

## Configuración y despliegue

1. Crea y rellena `.env` (token de Telegram, host/token de Databricks, OpenAI API key).
2. Instala dependencias: `pip install -r requirements.txt`.
3. Inicializa la base local: `python -c "from utils.db import init_db; init_db()"`.
4. Ejecuta el bot: `python app.py`.

### Variables principales en `.env`

```
TELEGRAM_BOT_TOKEN=...
DATABRICKS_HOST=...
DATABRICKS_TOKEN=...
OPENAI_API_KEY=...
SQLITE_PATH=audits.db
```

## Guía rápida de contribución

1. Crea una rama (`git checkout -b feature/nueva-funcionalidad`).
2. Añade/actualiza pruebas o scripts según corresponda.
3. Ejecuta formatters y linting (consulta `formatters/`).
4. Envía un PR describiendo el cambio y cualquier requerimiento adicional (migraciones, tokens, etc.).

¡Listo! Revisa el vídeo de la demo para ver el bot en acción antes de presentar tus cambios.
