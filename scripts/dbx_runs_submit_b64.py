import os, time, base64, requests, json
from dotenv import load_dotenv
load_dotenv()

HOST=(os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN=os.getenv("DATABRICKS_TOKEN") or ""
NB_PATH=os.getenv("DATABRICKS_NB_PATH") or ""
if not (HOST and TOKEN and NB_PATH):
    raise RuntimeError("Falta DATABRICKS_HOST/TOKEN/NB_PATH en .env")

H={"Authorization": f"Bearer {TOKEN}", "Content-Type":"application/json"}
U=lambda p: f"{HOST}{p}"

# CSV pequeñito con una fecha inválida
csv=("tx_id,date,account,debit,credit,desc\n"
     "1,2025-01-01,430,100,0,venta A\n"
     "1,2025-01-01,700,0,100,contrapartida A\n"
     "2,2025-13-05,430,50,0,fecha invalida\n")
b64=base64.b64encode(csv.encode()).decode()

payload={
  "run_name":"bot_audit_test",
  "tasks":[{
    "task_key":"t1",
    "notebook_task":{
      "notebook_path": NB_PATH,
      "base_parameters": {"csv_b64": b64, "file_name": "mini.csv"}
    },
    # Serverless si está disponible; si no, Databricks lo adapta
    "new_cluster":{"spark_version":"16.3.x-photon-scala2.12","num_workers":0,"node_type_id":"serverless"}
  }]
}

print("HOST:", HOST); print("NB_PATH:", NB_PATH)
r=requests.post(U("/api/2.1/jobs/runs/submit"), headers=H, json=payload, timeout=60)
print("SUBMIT STATUS:", r.status_code)
if r.status_code!=200:
    print(r.text); r.raise_for_status()

run_id=r.json()["run_id"]
print("RUN:", run_id, f"{HOST}/jobs/runs/{run_id}")

# polling simple
for _ in range(25):
    s=requests.get(U("/api/2.2/jobs/runs/get"), headers=H, params={"run_id": run_id}, timeout=60)
    s.raise_for_status()
    st=s.json()["state"]
    life=st.get("life_cycle_state"); res=st.get("result_state")
    print("life=",life," result=",res)
    if life in {"TERMINATED","INTERNAL_ERROR","SKIPPED"}: break
    time.sleep(6)

out=requests.get(U("/api/2.1/jobs/runs/get-output"), headers=H, params={"run_id": run_id}, timeout=60)
out.raise_for_status()
nb=out.json().get("notebook_output",{})
print("\n=== RESULT ===")
print(nb.get("result") or "(sin resultado)")
