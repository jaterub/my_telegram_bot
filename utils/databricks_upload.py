import os
import base64
import zipfile
import requests

def comprimir_excel_a_zip(excel_path: str) -> str:
    """
    Comprime un archivo Excel (.xlsx) en un ZIP con el mismo nombre base.
    Devuelve la ruta al ZIP creado.
    """
    zip_path = excel_path.replace(".xlsx", ".zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(excel_path, arcname=os.path.basename(excel_path))
    return zip_path

def subir_a_workspace(tmp_path: str) -> str:
    """
    Sube un archivo local a Workspace Files usando la API de Databricks.
    Devuelve el path remoto (ws_path).
    """
    host     = os.environ["DATABRICKS_HOST"]
    token    = os.environ["DATABRICKS_TOKEN"]
    ws_dir   = os.environ["DATABRICKS_WS_BASE"]  # ej: /Users/tuemail@databricks.com/bot_audit
    file_name = os.path.basename(tmp_path)
    ws_path   = f"{ws_dir}/{file_name}"

    print("[INFO] Subiendo archivo a Workspace Files:")
    print(f"  ➤ Local : {tmp_path}")
    print(f"  ➤ Remoto: {ws_path}")

    with open(tmp_path, "rb") as f:
        file_content = f.read()
    b64_content = base64.b64encode(file_content).decode("utf-8")

    url = f"{host}/api/2.0/workspace/import"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "path": ws_path,
        "format": "AUTO",
        "language": "PYTHON",
        "overwrite": True,
        "content": b64_content
    }

    response = requests.post(url, headers=headers, json=payload)

    print("[STATUS]", response.status_code)
    print("[RESPONSE]", response.text)

    if response.status_code == 200:
        print("[✅] Subida completada con éxito.")
        return ws_path
    else:
        raise Exception(f"❌ Falló la subida: {response.status_code} {response.text}")
