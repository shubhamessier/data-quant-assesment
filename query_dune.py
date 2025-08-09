import os
import requests
import time
import json
import csv
from typing import Dict, Any

DUNE_BASE_URL = "https://api.dune.com/api/v1"
DUNE_API_KEY = os.getenv("DUNE_API_KEY", "")

HEADERS = {"X-Dune-API-Key": DUNE_API_KEY}

RATE_LIMIT_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60


def require_api_key():
    if not DUNE_API_KEY:
        raise RuntimeError(
            "DUNE_API_KEY environment variable is not set. Export it first, e.g.\n"
            "  export DUNE_API_KEY=\"<your_api_key>\""
        )


def dune_post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{DUNE_BASE_URL}{path}"
    backoff = RATE_LIMIT_BACKOFF_SECONDS
    while True:
        res = requests.post(url, headers=HEADERS, json=payload)
        if res.status_code in (429, 502, 503):
            # rate-limited or transient error — backoff and retry
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue
        return res


def dune_get(path: str) -> requests.Response:
    url = f"{DUNE_BASE_URL}{path}"
    backoff = RATE_LIMIT_BACKOFF_SECONDS
    while True:
        res = requests.get(url, headers=HEADERS)
        if res.status_code in (429, 502, 503):
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
            continue
        return res


# Load wallets
with open("wallets.json", "r") as f:
    wallets = json.load(f)

SOURCE = wallets["source_wallets"][0]
TARGET = wallets["target_wallets"][0]

# SQL Query
SQL_QUERY = f"""
WITH filtered_txs AS (
  SELECT
    tx.id AS tx_id,
    tx.block_time,
    tx.signer AS account_owner,
    tx.pre_balances[1] AS pre_balance,
    tx.post_balances[1] AS post_balance
  FROM solana.transactions AS tx
  WHERE
    tx.signer IN ('{SOURCE}', '{TARGET}')
)
SELECT
  f1.tx_id AS tx_hash,
  MIN(f1.block_time) AS timestamp,
  src.pre_balance AS source_pre_balance,
  src.post_balance AS source_post_balance,
  tgt.pre_balance AS target_pre_balance,
  tgt.post_balance AS target_post_balance,
  (
    tgt.post_balance - tgt.pre_balance
  ) AS transfer_amount
FROM filtered_txs AS f1
JOIN filtered_txs AS src
  ON f1.tx_id = src.tx_id
  AND src.account_owner = '{SOURCE}'
JOIN filtered_txs AS tgt
  ON f1.tx_id = tgt.tx_id
  AND tgt.account_owner = '{TARGET}'
GROUP BY
  f1.tx_id,
  src.pre_balance,
  src.post_balance,
  tgt.pre_balance,
  tgt.post_balance
ORDER BY
  timestamp DESC
"""


def execute_dune_query(sql: str):
    require_api_key()

    # Step 1: Create a new query
    # Docs: POST /query with {"query": <sql>}
    create_res = dune_post("/query", {"query": sql})
    if create_res.status_code == 403:
        raise RuntimeError(
            f"403 Forbidden from Dune /query. Check API key/plan. Body: {create_res.text}"
        )
    create_res.raise_for_status()
    query_id = create_res.json()["query_id"]

    # Step 2: Execute the query
    # Docs: POST /query/{query_id}/execute -> returns execution_id
    exec_res = dune_post(f"/query/{query_id}/execute", {"parameters": []})
    if exec_res.status_code == 403:
        raise RuntimeError(
            f"403 Forbidden from Dune execute. Check API key/plan. Body: {exec_res.text}"
        )
    exec_res.raise_for_status()
    execution_id = exec_res.json()["execution_id"]

    # Step 3: Poll execution status
    while True:
        status_res = dune_get(f"/execution/{execution_id}/status")
        status_res.raise_for_status()
        status = status_res.json()
        state = status.get("state") or status.get("execution_state")
        if state == "QUERY_STATE_COMPLETED":
            break
        if state == "QUERY_STATE_FAILED":
            raise RuntimeError(f"Dune execution failed: {status}")
        time.sleep(2)

    # Step 4: Fetch results
    results_res = dune_get(f"/execution/{execution_id}/results")
    results_res.raise_for_status()
    data = results_res.json()
    # Dune response shape: { "result": { "rows": [...] } }
    result = data.get("result", {})
    rows = result.get("rows", [])
    return rows


def save_to_csv(data, filename="transactions.csv"):
    if not data:
        print("No data to save.")
        return
    keys = data[0].keys()
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data)} rows to {filename}")


if __name__ == "__main__":
    print("Running Dune query...")
    try:
        data = execute_dune_query(SQL_QUERY)
        save_to_csv(data)
    except Exception as e:
        print(f"[error] {e}")
        # Print a helpful hint if API key missing
        if not DUNE_API_KEY:
            print("Hint: export DUNE_API_KEY=\"<your_key>\"")
