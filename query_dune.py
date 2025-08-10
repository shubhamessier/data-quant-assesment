import os
import requests
import time
import json
import csv
from typing import Dict, Any, List

DUNE_BASE_URL = "https://api.dune.com/api/v1"
DUNE_API_KEY = os.getenv("DUNE_API_KEY", "")
DUNE_QUERY_ID = os.getenv("DUNE_QUERY_ID", "")  # pre-saved query id from Dune UI
PARAM_SOURCE_KEY = os.getenv("DUNE_PARAM_SOURCE_KEY", "source")  # name of the parameter in Dune UI
PARAM_TARGET_KEY = os.getenv("DUNE_PARAM_TARGET_KEY", "target")  # name of the parameter in Dune UI

HEADERS = {"X-Dune-API-Key": DUNE_API_KEY}

# DUNE QUERY = https://dune.com/queries/5608251

RATE_LIMIT_BACKOFF_SECONDS = 2
MAX_BACKOFF_SECONDS = 60


def require_api_key():
    if not DUNE_API_KEY:
        raise RuntimeError(
            "DUNE_API_KEY environment variable is not set. Export it first, e.g.\n"
            "  export DUNE_API_KEY=\"<your_api_key>\""
        )


def require_query_id():
    if not DUNE_QUERY_ID:
        raise RuntimeError(
            "DUNE_QUERY_ID environment variable is not set. Create & save your query in the Dune UI, then set:\n"
            "  export DUNE_QUERY_ID=\"<your_query_id>\""
        )


def dune_post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{DUNE_BASE_URL}{path}"
    backoff = RATE_LIMIT_BACKOFF_SECONDS
    while True:
        res = requests.post(url, headers=HEADERS, json=payload)
        if res.status_code in (429, 502, 503):
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

# NOTE: For free plans we cannot CREATE queries via API. You must save a query in Dune UI that
# uses named parameters (e.g., :source and :target). Then pass the parameter names via env or defaults above.


def execute_saved_dune_query(query_id: str, parameters: List[Dict[str, Any]], try_cached_first: bool = True):
    require_api_key()
    require_query_id()

    # Optionally fetch last cached results (if recently executed in UI or via API)
    if try_cached_first:
        cached = dune_get(f"/query/{query_id}/results")
        if cached.ok:
            data = cached.json()
            rows = data.get("result", {}).get("rows", [])
            if rows:
                return rows

    # Execute saved query (allowed on free tier)
    exec_res = dune_post(f"/query/{query_id}/execute", {"parameters": parameters})
    if exec_res.status_code == 403:
        raise RuntimeError(
            f"403 Forbidden from Dune execute. Check plan/access or parameter names. Body: {exec_res.text}"
        )
    exec_res.raise_for_status()
    execution_id = exec_res.json()["execution_id"]

    # Poll execution status
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

    # Fetch results
    results_res = dune_get(f"/execution/{execution_id}/results")
    results_res.raise_for_status()
    data = results_res.json()
    rows = data.get("result", {}).get("rows", [])
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
        params = [
            {"key": PARAM_SOURCE_KEY, "type": "text", "value": SOURCE},
            {"key": PARAM_TARGET_KEY, "type": "text", "value": TARGET},
        ]
        data = execute_saved_dune_query(DUNE_QUERY_ID, params, try_cached_first=True)
        save_to_csv(data)
    except Exception as e:
        print(f"[error] {e}")
        if not DUNE_API_KEY:
            print("Hint: export DUNE_API_KEY=\"<your_key>\"")
        if not DUNE_QUERY_ID:
            print("Hint: export DUNE_QUERY_ID=\"<your_saved_query_id>\" and ensure your saved query uses parameters '", PARAM_SOURCE_KEY, "' and '", PARAM_TARGET_KEY, "'.")
