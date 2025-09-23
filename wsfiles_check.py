import os
import requests
from urllib.parse import quote
# wsfiles_check.py


def verificar_existencia_por_export(ws_path: str) -> bool:
    """
    Verifica si un archivo existe en Workspace Files usando /workspace/export.
    """
    host  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN") or ""
    
    url = f"{host}/api/2.0/workspace/export?path={quote(ws_path)}&format=AUTO"
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        r = requests.get(url, headers=headers, timeout=10)
        print(f"[DEBUG] Verificación via /workspace/export → status {r.status_code}")
        return r.status_code == 200
    except Exception as e:
        print("[ERROR] Excepción al verificar existencia:", e)
        return False

def listar_y_exportar_archivo_debug(ws_dir: str, file_name: str):
    host  = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
    token = os.getenv("DATABRICKS_TOKEN") or ""
    file_path = f"{ws_dir}/{file_name}"

    # Listar archivos en bot_audit
    print(f"\\n📂 Listando objetos en {ws_dir}…")
    r_list = requests.get(
        f"{host}/api/2.0/workspace/list",
        headers={"Authorization": f"Bearer {token}"},
        params={"path": ws_dir},
        timeout=20,
    )

    print("[STATUS LIST]", r_list.status_code)
    if r_list.status_code == 200:
        data = r_list.json()
        for obj in data.get("objects", []):
            print(f"  ➤ {obj['object_type']:10} {obj['path']}")
    else:
        print("[ERROR]", r_list.text)

    # Descargar archivo con /workspace/export
    print(f"\\n⬇️ Descargando: {file_path}…")
    url = f"{host}/api/2.0/workspace/export?path={quote(file_path)}&format=AUTO"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    r_export = requests.get(url, headers=headers)

    print("[STATUS EXPORT]", r_export.status_code)
    if r_export.status_code == 200:
        content = r_export.content
        with open("descarga_temporal.xlsx", "wb") as f:
            f.write(content)
        print("[✅] Archivo exportado correctamente como descarga_temporal.xlsx")
    else:
        print("[❌] Falló la descarga con /workspace/export")
        print("[RESPONSE]", r_export.text)

