import requests
import time
import json
import csv

# Load wallets
with open("wallets.json", "r") as f:
    wallets = json.load(f)

SOURCE = wallets["source_wallets"][0]
TARGET = wallets["target_wallets"][0]

# Your Dune API key
DUNE_API_KEY = "YOUR_DUNE_API_KEY"  # <-- put your API key here
DUNE_BASE_URL = "https://api.dune.com/api/v1"

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

def execute_dune_query(sql):
    # Step 1: Submit query
    res = requests.post(
        f"{DUNE_BASE_URL}/query/",
        headers={"X-Dune-API-Key": DUNE_API_KEY},
        json={"query": sql, "parameters": []}
    )
    res.raise_for_status()
    query_id = res.json()["query_id"]

    # Step 2: Wait for execution
    while True:
        status = requests.get(
            f"{DUNE_BASE_URL}/query/{query_id}/status",
            headers={"X-Dune-API-Key": DUNE_API_KEY}
        ).json()
        if status["state"] == "QUERY_STATE_COMPLETED":
            break
        elif status["state"] == "QUERY_STATE_FAILED":
            raise Exception("Query failed")
        time.sleep(2)

    # Step 3: Fetch results
    result = requests.get(
        f"{DUNE_BASE_URL}/query/{query_id}/results",
        headers={"X-Dune-API-Key": DUNE_API_KEY}
    ).json()

    return result["result"]["rows"]

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
    data = execute_dune_query(SQL_QUERY)
    save_to_csv(data)
