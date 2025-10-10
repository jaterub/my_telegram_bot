#audit_xlsx.py

# ====== IMPORTS REQUERIDOS (arriba del archivo) ===============================
import requests
import os
import asyncio
from telegram import Update
from telegram.ext import ContextTypes
from telegram.ext import CommandHandler
from telegram.ext import MessageHandler, filters
from pathlib import Path
from utils.databricks_upload import comprimir_excel_a_zip, subir_a_workspace


import json, base64, uuid,gzip
from wsfiles_check import verificar_existencia_por_export,listar_y_exportar_archivo_debug

from utils.db import insert_audit

from utils.db import get_last_audits




def _run_now_latest(job_id: str, ws_path: str, host: str, token: str) -> int:
    """
    Lanza el job indicando la ruta completa del archivo (ws_path).
    El notebook recibirá directamente el archivo a procesar.
    """
    r = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {token}"},
        json={"job_id": job_id, "notebook_params": {"ws_path": ws_path}},
        timeout=60,
    )
    if r.status_code != 200:
        raise Exception(f"Run-now API error: {r.status_code} {r.text}")
    run_id = r.json().get("run_id")
    if not run_id:
        raise Exception(f"Respuesta sin run_id: {r.text}")
    return run_id


WS_INBOX_DIR = "/Users/ja.tejeror@gmail.com/bot_audit"

def _workspace_files_upload(data: bytes, host: str, token: str, filename: str) -> str:
    """
    Sube 'data' a Workspace Files en WS_INBOX_DIR con nombre único.
    Devuelve la ruta lógica (ws_path), p.ej. /Shared/bot_audit/inbox/abc.xlsx
    """
    # crea nombre único conservando extensión
    name = f"{uuid.uuid4().hex}_{filename}"
    ws_path = f"{WS_INBOX_DIR}/{name}"
    b64 = base64.b64encode(data).decode()
    r = requests.post(
        f"{host}/api/2.0/workspace-files/import",
        headers={"Authorization": f"Bearer {token}"},
        json={"path": ws_path, "overwrite": True, "content": b64},
        timeout=120,
    )
    if r.status_code != 200:
        raise Exception(f"Workspace Files import error: {r.status_code} {r.text}")
    return ws_path

# --- Formatter para el resumen en Markdown -----------------------------------
def _md_escape(s: str) -> str:
    # escape mínimo para Telegram Markdown (usa reply_markdown normal)
    if s is None:
        return ""
    return str(s).replace("_", r"\_").replace("*", r"\*").replace("[", r"\[").replace("`", r"\`")

def _fmt_items(items, keys, max_rows=10):
    out = []
    for i, it in enumerate(items[:max_rows], 1):
        cols = []
        for k in keys:
            v = it.get(k, "")
            cols.append(f"{k}=`{_md_escape(v)}`")
        out.append(f"  • {', '.join(cols)}")
    more = len(items) - len(items[:max_rows])
    if more > 0:
        out.append(f"  • … y {more} más")
    return "\n".join(out) if out else "  • (sin muestras)"

def fmt_summary(s: dict) -> str:
    meta = s.get("meta", {})
    filename   = meta.get("filename") or meta.get("path") or "input.xlsx"
    source     = meta.get("source", "?")
    path       = meta.get("path") or meta.get("local_path") or ""
    rows_raw   = meta.get("rows_pandas_raw", "¿?")
    rows_final = s.get("rows", "¿?")

    invalid    = s.get("invalid_date", {}) or {}
    dups       = s.get("duplicates_tx", {}) or {}
    unbal      = s.get("unbalanced_tx", {}) or {}
    reqnull    = s.get("required_nulls", {}) or {}

    lines = []
    lines.append(f"*Auditoría de Excel*  —  `{_md_escape(filename)}`")
    lines.append("")
    lines.append(f"- Origen: `{_md_escape(source)}`")
    if path:
        lines.append(f"- Ruta: `{_md_escape(path)}`")
    lines.append(f"- Filas (pandas crudo): `{rows_raw}`")
    lines.append(f"- Filas (tras normalización): `{rows_final}`")
    lines.append("")

    # Resumen de conteos
    lines.append("*Resumen de validaciones*")
    lines.append(f"- Fechas inválidas: `{invalid.get('count', 0)}`")
    lines.append(f"- Duplicados por clave: `{dups.get('count', 0)}`")
    lines.append(f"- Asientos desbalanceados: `{unbal.get('count', 0)}`")
    lines.append(f"- Obligatorios nulos: `{reqnull.get('count', 0)}`")
    lines.append("")

    # Muestras
    if invalid.get("count", 0):
        lines.append("*Fechas inválidas (muestra)*")
        lines.append(_fmt_items(invalid.get("items", []), ["row","company_code","tx_id","date"]))
        lines.append("")

    if dups.get("count", 0):
        lines.append("*Duplicados por clave (muestra)*")
        lines.append(_fmt_items(dups.get("items", []), ["row","company_code","tx_id","date","account","debit","credit","currency"]))
        lines.append("")

    if unbal.get("count", 0):
        lines.append("*Asientos desbalanceados (muestra)*")
        # group_cols + sums/diff ya vienen en items
        lines.append(_fmt_items(unbal.get("items", []), ["company_code","tx_id","currency","sum_debit","sum_credit","diff"]))
        lines.append("")

    if reqnull.get("count", 0):
        lines.append("*Campos obligatorios nulos (muestra)*")
        lines.append(_fmt_items(reqnull.get("items", []), ["row","company_code","tx_id","account","date"]))
        lines.append("")

    # Si todo está OK
    if not any([invalid.get("count"), dups.get("count"), unbal.get("count"), reqnull.get("count")]):
        lines.append("✅ *Sin incidencias relevantes*")

    return "\n".join(lines)



def _dbfs_upload(data: bytes, host: str, token: str, path: str | None = None) -> str:
    """Sube 'data' a DBFS (create/add-block/close) y devuelve la ruta dbfs:/..."""
    if path is None:
        path = f"dbfs:/tmp/tg/{uuid.uuid4().hex}.xlsx"

    # create
    r = requests.post(
        f"{host}/api/2.0/dbfs/create",
        headers={"Authorization": f"Bearer {token}"},
        json={"path": path, "overwrite": True},
        timeout=60,
    )
    if r.status_code != 200:
        raise Exception(f"DBFS create error: {r.status_code} {r.text}")
    handle = r.json().get("handle")
    if handle is None:
        raise Exception(f"DBFS create sin handle: {r.text}")

    # add-block en chunks de 2MB
    CHUNK = 2 * 1024 * 1024
    for i in range(0, len(data), CHUNK):
        b64 = base64.b64encode(data[i:i+CHUNK]).decode()
        r = requests.post(
            f"{host}/api/2.0/dbfs/add-block",
            headers={"Authorization": f"Bearer {token}"},
            json={"handle": handle, "data": b64},
            timeout=120,
        )
        if r.status_code != 200:
            # intenta cerrar para no dejar handle colgado
            try:
                requests.post(
                    f"{host}/api/2.0/dbfs/close",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"handle": handle},
                    timeout=30,
                )
            except Exception:
                pass
            raise Exception(f"DBFS add-block error: {r.status_code} {r.text}")

    # close
    r = requests.post(
        f"{host}/api/2.0/dbfs/close",
        headers={"Authorization": f"Bearer {token}"},
        json={"handle": handle},
        timeout=60,
    )
    if r.status_code != 200:
        raise Exception(f"DBFS close error: {r.status_code} {r.text}")

    return path



def _workspace_files_upload(data: bytes, host: str, token: str, path: str | None = None) -> str:
    if path is None:
        path = f"/tmp/tg/{uuid.uuid4().hex}.xlsx"
    b64 = base64.b64encode(data).decode()
    r = requests.post(
        f"{host}/api/2.0/workspace-files/import",
        headers={"Authorization": f"Bearer {token}"},
        json={"path": path, "overwrite": True, "content": b64},
        timeout=60,
    )
    if r.status_code != 200:
        raise Exception(f"Workspace Files import error: {r.status_code} {r.text}")
    return path


def _run_now_sync_inline(job_id: str, filename: str, data: bytes, host: str, token: str) -> int:
    """
    1) Si cabe, inline pequeño (file_b64)  → rápido.
    2) Si no, sube a Workspace Files y pasa ws_path + host + pat (todo muy corto).
    """
    def run_now(body: dict) -> int:
        r = requests.post(
            f"{host}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {token}"},
            json=body, timeout=60,
        )
        if r.status_code != 200:
            raise Exception(f"Run-now API error: {r.status_code} {r.text}")
        rid = r.json().get("run_id")
        if not rid:
            raise Exception(f"Respuesta sin run_id: {r.text}")
        return rid

    # 1) Intento inline mini (seguro <10k total). Margen: 6k chars.
    gz_b64 = base64.b64encode(gzip.compress(data)).decode()
    if len(gz_b64) <= 6000:
        body = {
            "job_id": job_id,
            "notebook_params": {"file_b64": gz_b64, "filename": filename},
        }
        try:
            return run_now(body)
        except Exception:
            pass  # si falla, seguimos al plan B

    # 2) Plan B: Workspace Files + descarga HTTP en el notebook
    ws_path = _workspace_files_upload(data, host, token)
    body = {
        "job_id": job_id,
        "notebook_params": {
            "ws_path": ws_path,      # p.ej. /tmp/tg/abc.xlsx
            "filename": filename,
            "host": host,            # p.ej. https://adb-xxxx.azuredatabricks.net
            "pat": token,            # token corto (unos ~40-50 chars)
        },
    }
    return run_now(body)


# ====== HELPERS COMUNES =======================================================
def _h(token: str):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _url(host: str, p: str) -> str:
    return f"{host}{p}"

# ====== WORKSPACE FILES (SUBIDA/EXISTE) ======================================
def _wsfiles_mkdirs_sync(path: str, host: str, token: str):
    """
    Crea (idempotente) la carpeta en Workspace Files.
    """
    r = requests.post(
        f"{host}/api/2.0/workspace-files/mkdirs",
        headers=_h(token),
        json={"path": path},
        timeout=30,
    )
    # 200 OK, 409 Already Exists
    if r.status_code not in (200, 409):
        raise RuntimeError(f"mkdirs failed ({r.status_code}): {r.text}")

def _wsfiles_import_sync(ws_path: str, data: bytes, host: str, token: str, overwrite: bool=True):
    """
    Sube el archivo a Workspace Files en la ruta ws_path.
    """
    payload = {
        "path": ws_path,
        "overwrite": overwrite,
        "format": "AUTO",
        "content": base64.b64encode(data).decode("utf-8"),
    }
    r = requests.post(
        f"{host}/api/2.0/workspace-files/import",
        headers=_h(token),
        json=payload,
        timeout=120,
    )
    if r.status_code >= 400:
        # mensaje más legible
        try:
            msg = r.json().get("message") or r.text
        except Exception:
            msg = r.text
        raise RuntimeError(f"Workspace Files import failed ({r.status_code}): {msg}")
def debug_listar_archivos_wsfiles(ws_dir: str, host: str, token: str):
    """Debug: lista archivos .xlsx en una carpeta Workspace Files."""
    import requests
    url = f"{host}/api/2.0/workspace-files/list"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"path": ws_dir}
    
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"[DEBUG] Listando archivos en {ws_dir} → status {r.status_code}")
        if r.status_code != 200:
            print(f"[ERROR] No se pudo listar: {r.text}")
            return
        
        data = r.json()
        archivos = data.get("files", [])
        if not archivos:
            print(f"[DEBUG] No hay archivos en {ws_dir}")
        else:
            print(f"[DEBUG] Archivos encontrados en {ws_dir}:")
            for f in archivos:
                print(f"  - {f['path']} (modificado: {f['modification_time']})")
    except Exception as e:
        print(f"[ERROR] Exception al listar archivos: {e}")

def _wsfiles_exists_sync(ws_path: str, host: str, token: str) -> bool:
    import requests
    from urllib.parse import quote
    url = f"{host}/api/2.0/workspace-files/get-status?path={quote(ws_path)}" 
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=20)
    print(f"[DEBUG] GET {url} → Status {r.status_code}")
    return r.status_code == 200

# ====== JOBS API (RUN + ESTADO + OUTPUT) =====================================
def _run_now_sync_with_ws_path(job_id: int, ws_path: str, host: str, token: str) -> int:
    """
    Lanza el Job pasando el parámetro 'ws_path' para que el notebook lea desde Workspace Files.
    """
    payload = {"job_id": job_id, "notebook_params": {"ws_path": ws_path}}
    r = requests.post(
        f"{host}/api/2.2/jobs/run-now",
        headers=_h(token),
        json=payload,
        timeout=60,
    )
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.reason}: {r.text}")
    return r.json()["run_id"]

def _get_state_sync(run_id: int, host: str, token: str) -> dict:
    r = requests.get(
        _url(host, "/api/2.2/jobs/runs/get"),
        headers=_h(token),
        params={"run_id": run_id},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["state"]

def _safe_get_output_sync(run_id: int, host: str, token: str) -> str:
    """
    Devuelve '' si no hay salida aún o si la API responde error.
    """
    try:
        r = requests.get(
            _url(host, "/api/2.1/jobs/runs/get-output"),
            headers=_h(token),
            params={"run_id": run_id},
            timeout=60,
        )
        r.raise_for_status()
        j = r.json()
        return (j.get("notebook_output") or {}).get("result", "") or ""
    except Exception:
        return ""

def _get_run_sync(run_id: int, host: str, token: str) -> dict:
    r = requests.get(
        _url(host, "/api/2.1/jobs/runs/get"),
        headers=_h(token),
        params={"run_id": run_id},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()

def _list_task_run_ids_sync(parent_run_id: int, host: str, token: str) -> list[int]:
    """
    Si el run es 'parent' (multi-task), devuelve los run_id de las tareas.
    """
    data = _get_run_sync(parent_run_id, host, token)
    tasks = data.get("tasks") or []
    return [t.get("run_id") for t in tasks if t.get("run_id")]


DBFS_BASE = os.getenv("DATABRICKS_DBFS_BASE", "").rstrip("/")

# Constantes + cfg únicas
TASK_KEY = os.getenv("DATABRICKS_TASK_KEY", "")

if not DBFS_BASE:
    raise RuntimeError("Falta DATABRICKS_DBFS_BASE en .env (ej: /Users/tu_email@empresa.com/bot_audit)")

def _cfg():
    host  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN") or ""
    job   = int(os.getenv("DATABRICKS_JOB_ID_AUDIT", "0") or "0")
    return host, token, job


def _h(token: str):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def _url(host: str, p: str) -> str:
    return f"{host}{p}"

# ===== DBFS helpers (upload por bloques) =====================================



def _dbfs_create_sync(path: str, host: str, token: str, overwrite: bool = True) -> int:
    r = requests.post(
        f"{host}/api/2.0/dbfs/create",
        headers=_h(token),
        json={"path": path, "overwrite": overwrite},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["handle"]

def _dbfs_add_block_sync(handle: int, data_bytes: bytes, host: str, token: str):
    chunk_b64 = _b64.b64encode(data_bytes).decode("utf-8")
    r = requests.post(
        f"{host}/api/2.0/dbfs/add-block",
        headers=_h(token),
        json={"handle": handle, "data": chunk_b64},
        timeout=60,
    )
    r.raise_for_status()

def _dbfs_close_sync(handle: int, host: str, token: str):
    r = requests.post(
        f"{host}/api/2.0/dbfs/close",
        headers=_h(token),
        json={"handle": handle},
        timeout=60,
    )
    r.raise_for_status()


def _wsfiles_mkdirs_sync(path: str, host: str, token: str):
    r = requests.post(f"{host}/api/2.0/workspace-files/mkdirs",
                      headers=_h(token), json={"path": path}, timeout=30)
    if r.status_code not in (200, 409):  # 409 = ya existe
        raise RuntimeError(f"mkdirs failed ({r.status_code}): {r.text}")

def _wsfiles_import_sync(ws_path: str, data: bytes, host: str, token: str, overwrite: bool = True):
    b64 = base64.b64encode(data).decode("utf-8")
    payload = {"path": ws_path, "overwrite": overwrite, "format": "AUTO", "content": b64}
    r = requests.post(f"{host}/api/2.0/workspace-files/import",
                      headers=_h(token), json=payload, timeout=60)
    if r.status_code >= 400:
        # No expongas la URL en el chat
        try:
            msg = r.json().get("message") or r.text
        except Exception:
            msg = r.text
        raise RuntimeError(f"Workspace Files import failed ({r.status_code}): {msg}")

def _wsfiles_exists_sync(ws_path: str, host: str, token: str) -> bool:
    parent = ws_path.rsplit("/", 1)[0]
    name   = ws_path.rsplit("/", 1)[1]
    r = requests.get(f"{host}/api/2.0/workspace-files/list",
                     headers=_h(token), params={"path": parent}, timeout=30)
    if r.status_code != 200:
        return False
    items = r.json().get("files") or r.json().get("objects") or []
    return any((it.get("path") == ws_path) or (it.get("name") == name) for it in items)

def _run_now_sync_with_ws_path(job_id: int, ws_path: str, host: str, token: str) -> int:
    payload = {"job_id": job_id, "notebook_params": {"ws_path": ws_path}}
    r = requests.post(f"{host}/api/2.2/jobs/run-now",
                      headers=_h(token), json=payload, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"run-now failed ({r.status_code}): {r.text}")
    return r.json()["run_id"]

def _safe_get_output_sync(run_id: int, host: str, token: str) -> str:
    try:
        r = requests.get(f"{host}/api/2.1/jobs/runs/get-output",
                         headers=_h(token), params={"run_id": run_id}, timeout=60)
        r.raise_for_status()
        j = r.json()
        return (j.get("notebook_output") or {}).get("result", "") or ""
    except Exception:
        return ""




def _run_now_with_ws_path(job_id: str, ws_path: str, host: str, token: str) -> int:
    """
    Lanza el job y pasa `ws_path` como notebook_param.
    """
    r = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "job_id": job_id,
            "notebook_params": {
                "ws_path": ws_path  # <<< AQUÍ PASAS LA RUTA COMPLETA
            }
        },
        timeout=60,
    )
    if r.status_code != 200:
        raise Exception(f"Run-now API error: {r.status_code} {r.text}")
    run_id = r.json().get("run_id")
    if not run_id:
        raise Exception(f"Respuesta sin run_id: {r.text}")
    return run_id



def _dbfs_upload_sync(path: str, data: bytes, host: str, token: str, chunk_size: int = 1024 * 1024):
    """Sube 'data' a DBFS en bloques (~1 MB)."""
    h = _dbfs_create_sync(path, host, token, overwrite=True)
    try:
        for i in range(0, len(data), chunk_size):
            _dbfs_add_block_sync(h, data[i:i + chunk_size], host, token)
    finally:
        _dbfs_close_sync(h, host, token)

# ===== Jobs API helpers =======================================================
def _run_now_sync_with_path(job_id: int, file_path: str, host: str, token: str) -> int:
    """Lanza el Job pasando 'file_path' (dbfs:/...) como parámetro de notebook."""
    payload = {"job_id": job_id, "notebook_params": {"file_path": file_path}}
    r = requests.post(
        f"{host}/api/2.2/jobs/run-now",
        headers=_h(token),
        json=payload,
        timeout=60,
    )
    if r.status_code >= 400:
        raise requests.HTTPError(
            f"{r.status_code} {r.reason}: {r.text}  (host={host}, job_id={job_id})"
        )
    return r.json()["run_id"]

def _get_state_sync(run_id: int, host: str, token: str) -> dict:
    r = requests.get(_url(host, "/api/2.2/jobs/runs/get"), headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return r.json()["state"]

def _get_output_sync(run_id: int, host: str, token: str) -> str:
    r = requests.get(_url(host, "/api/2.1/jobs/runs/get-output"), headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return (r.json().get("notebook_output") or {}).get("result", "") or ""

def _get_run_sync(run_id: int, host: str, token: str) -> dict:
    r = requests.get(f"{host}/api/2.1/jobs/runs/get", headers=_h(token), params={"run_id": run_id}, timeout=60)
    r.raise_for_status()
    return r.json()

def _list_task_run_ids_sync(parent_run_id: int, host: str, token: str) -> list[int]:
    """Devuelve los run_id de tareas si el run es 'parent' (multi-task); si no, lista vacía."""
    data = _get_run_sync(parent_run_id, host, token)
    tasks = data.get("tasks") or []
    return [t.get("run_id") for t in tasks if t.get("run_id")]

# ----------



# ---------- (A) FORMATEADOR EN VIÑETAS PARA TELEGRAM ----------
def _fmt_summary(summary: dict) -> str:
    """Convierte el JSON del notebook a un texto legible con viñetas."""
    def sec(title, key, emoji):
        blk = summary.get(key, {}) or {}
        cnt = blk.get("count", 0)
        items = blk.get("items", [])[:5]  # muestra hasta 5
        lines = [f"{emoji} *{title}*: *{cnt}*"]
        for it in items:
            if key == "unbalanced_tx":
                lines.append(
                    f"  • tx `{it.get('tx_id')}` — debit={it.get('sum_debit')} "
                    f"credit={it.get('sum_credit')} diff={it.get('diff')}  _{it.get('suggestion')}_"
                )
            else:
                lines.append(
                    f"  • fila {it.get('row')} tx `{it.get('tx_id')}` — {it.get('reason')}  _{it.get('suggestion')}_"
                )
        return "\n".join(lines)

    rows = summary.get("rows", 0)
    parts = [
        "📊 *Auditoría contable*",
        f"Total de filas: `{rows}`",
        sec("Fechas inválidas", "invalid_date", "🗓️"),
        sec("Duplicados (tx_id)", "duplicates_tx", "🔁"),
        sec("Desbalances", "unbalanced_tx", "⚖️"),
        sec("Obligatorios nulos", "required_nulls", "❗"),
    ]
    return "\n".join(parts)



MAX_SIZE = 20_000_000


# Persistencia opcional (SQLite) Borrar??
try:
    from db import sqlite_store as store
    _HAS_STORE = True
except Exception:
    _HAS_STORE = False

# ===== Handlers ===============================================================


async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = get_last_audits(5)  # últimas 5 auditorías
    if not rows:
        return await update.message.reply_text("📭 No hay auditorías registradas todavía.")

    msg = ["📜 Últimas auditorías:"]
    for id, filename, created_at, summary_json in rows:
        msg.append(f"- {filename} ({created_at})")

    await update.message.reply_text("\n".join(msg))

async def audit_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    host, token, job_id = _cfg()
    if not (host and token and job_id):
        return await update.message.reply_text(
            "⚠️ Configura DATABRICKS_HOST, DATABRICKS_TOKEN y DATABRICKS_JOB_ID_AUDIT en .env"
        )
    await update.message.reply_text(
        "🔎 Auditoría contable: envíame tu Excel (.xlsx) como *documento*.\n"
        "Validaré fechas, duplicados, desbalances y campos obligatorios. ⚖️"
    )


async def audit_doc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # 0) Config + validaciones
        host, token, job_id = _cfg()
        if not (host and token and job_id):
            return await update.message.reply_text("⚠️ Config de Databricks incompleta. Revisa .env")

        doc = update.message.document
        if not doc or not doc.file_name.lower().endswith(".xlsx"):
            return await update.message.reply_text("⚠️ Necesito un Excel .xlsx (envíalo como *documento*).")

        # 1) Descarga local
        await update.message.reply_text("📅 Recibido. Descargando archivo…")
        tmp_dir = Path("tmp"); tmp_dir.mkdir(exist_ok=True)
        file_name = doc.file_name
        local_path = tmp_dir / file_name
        tg_file = await context.bot.get_file(doc.file_id)
        await tg_file.download_to_drive(str(local_path))

        # ✅ Verifica cabecera ZIP
        print("[DEBUG] Validando contenido real del archivo...")
        with open(local_path, "rb") as f:
            cabecera = f.read(4)
        if cabecera[:2] == b'PK':
            print("[OK] Parece un archivo Excel real (.xlsx)")
        else:
            print("[❌] NO parece un Excel. Posible JSON o formato incorrecto.")

        # 2) Subida a Workspace
        await update.message.reply_text("📤 Subiendo a Workspace Files…")
        try:
            ws_path = await asyncio.to_thread(subir_a_workspace, str(local_path))
        except Exception as e:
            return await update.message.reply_text(f"❌ Error al subir archivo a Databricks:\n{e}")
        print("[DEBUG] Archivo subido a Workspace:", ws_path)

        await asyncio.to_thread(listar_y_exportar_archivo_debug, WS_INBOX_DIR, file_name)

        # 3) Verificación
        await update.message.reply_text("🔍 Verificando disponibilidad del archivo en Workspace Files…")
        for intento in range(20):
            exists = await asyncio.to_thread(verificar_existencia_por_export, ws_path)
            if exists:
                print(f"[✅] Archivo encontrado en el intento {intento + 1}.")
                break
            await asyncio.sleep(1)
        else:
            return await update.message.reply_text("⚠️ El archivo aún no está disponible en Workspace Files. Intenta más tarde.")

        # 4) Lanza auditoría
        await update.message.reply_text(
            f"✅ Subido: `{ws_path}`\n"
            f"🚀 Lanzando auditoría sobre la *última* subida en `{WS_INBOX_DIR}`…",
        )
        run_id = _run_now_with_ws_path(job_id, ws_path, host, token)
        run_url = f"{host}/jobs/runs/{run_id}"
        await update.message.reply_text(f"⏳ run_id={run_id}\n{run_url}")

        # 5) Polling de estado
        max_secs, interval = 600, 5
        waited, final_state = 0, None
        while waited < max_secs:
            state = await asyncio.to_thread(_get_state_sync, run_id, host, token)
            life = state.get("life_cycle_state")
            if life in {"TERMINATED", "INTERNAL_ERROR", "SKIPPED"}:
                final_state = state
                break
            await asyncio.sleep(interval)
            waited += interval
        if not final_state:
            return await update.message.reply_text("⏱ Timeout: la ejecución sigue en curso. Intenta luego.")

        # 6) Leer salida
        output, task_run_ids = "", []
        for _ in range(24):  # ~120s extra
            output = await asyncio.to_thread(_safe_get_output_sync, run_id, host, token)
            if output:
                break
            if not task_run_ids:
                try:
                    task_run_ids = await asyncio.to_thread(_list_task_run_ids_sync, run_id, host, token)
                except Exception:
                    task_run_ids = []
            for tr in task_run_ids:
                output = await asyncio.to_thread(_safe_get_output_sync, tr, host, token)
                if output:
                    break
            if output:
                break
            await asyncio.sleep(5)

        if not output:
            status = f"{final_state.get('life_cycle_state')}/{final_state.get('result_state')}"
            return await update.message.reply_text(
                "⚠️ No pude leer la salida del Job todavía.\n"
                f"Estado final: {status}\n"
                "Asegúrate de que el notebook termina con dbutils.notebook.exit(JSON)."
            )

        # 7) Parsear JSON
        try:
            summary = json.loads(output) if isinstance(output, str) else output
        except Exception as e:
            return await update.message.reply_text(
                f"⚠️ Salida no válida (no es JSON parseable): {e}\n\nOutput crudo:\n{output[:2000]}"
            )

        # 8) Resumen bonito con LLM
        from utils.llm import summarize_audit_spanish
        from utils.db import insert_audit
        try:
            pretty = summarize_audit_spanish(summary)
        except Exception as e:
            pretty = None

        # 9) Guardar en SQLite
        try:
            insert_audit(file_name, json.dumps(summary, ensure_ascii=False), run_url=run_url, llm_summary=pretty)
        except Exception as e:
            print(f"[WARN] No se pudo guardar en SQLite: {e}")

        # 10) Respuesta al usuario
        if pretty:
            await update.message.reply_text(pretty)
        else:
            await update.message.reply_markdown(fmt_summary(summary))

    except Exception as e:
        await update.message.reply_text(f"❌ Error inesperado en audit_doc: {e}")


def register_handlers(app):



    app.add_handler(CommandHandler("history", history_cmd))
    app.add_handler(CommandHandler("audit", audit_cmd))
    app.add_handler(
        MessageHandler(
            filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            audit_doc,
        )
    )
    app.add_handler(MessageHandler(filters.Document.FileExtension("xlsx"), audit_doc))
