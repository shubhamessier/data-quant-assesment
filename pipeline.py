# Jevans 5W4sXjpeY6DVw3wXrHuf1qWGunwRCjeio7gKXhagB7BR
# Full script with Helius enrichment and enriched CSV output.
# NOTE: set HELIUS_API_KEY before running.

import requests
import time
import pandas as pd
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
from datetime import datetime
import math
from typing import List, Dict
import os
import json
import numpy as np

# --- Configuration ---
DUNE_API_KEY = "melO5xADeobebESDd6HGGbnWM4HTnfc1"  # Your Dune API key
WRITE_HELIUS_RAW = False  # set True to dump raw Helius enhanced JSON for debugging
OUTPUT_DIR = "/home/shubham/Downloads/subsidy-working/output"  # Global output directory

# Jito API and Proxy Configuration
JITO_BUNDLES_URL = "https://bundles.jito.wtf/api/v1/bundles"
proxy_list = [
    'http://zissujith:Gx4iaqiiLU@31.59.236.252:50100', 
    'http://zissujith:Gx4iaqiiLU@31.59.237.79:50100',
    'http://zissujith:Gx4iaqiiLU@31.59.239.20:50100', 
    'http://zissujith:Gx4iaqiiLU@31.59.237.148:50100'
]
proxy_cycle = cycle(proxy_list)


    # 'astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm', 'astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF',
    # 'astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk', 'astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL'

def log(message):
    """Prints a message with a timestamp."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

SQL_QUERY = """
WITH all_transactions AS (
  SELECT
    signature,
    block_time,
    success,
    account_keys,
    pre_balances,
    post_balances
  FROM
    solana.transactions
  WHERE
    signer = '999TYHARPrwdM2rSFs7jBCRPrXCBPwQLpgeYR45WY999'
AND block_time >= CAST('2025-07-06 00:00:00' AS TIMESTAMP) AND block_time < CAST('2025-08-10 00:00:00' AS TIMESTAMP)
    -- include both success and failure txns
),
balance_changes AS (
    SELECT
        signature,
        (acc.post_balance - acc.pre_balance) as lamports_change
    FROM all_transactions,
    UNNEST(account_keys, pre_balances, post_balances) AS acc(key, pre_balance, post_balance)
    WHERE acc.key IN (
        '5QuV4TS5TJFWPu7Yd56VaPvf4nKUicPvTfC3mwnb7dNW', 'Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3', '3Rz8uD83QsU8wKvZbgWAPvCNDU6Fy8TSZTMcPm3RB6zt', 
        '4HiwLEP2Bzqj3hM2ENxJuzhcPCdsafwiet3oGkMkuQY4', 'AsEF2SWSEZ1xpGZ5fdzDKaoka1XEtFSjGo39YUXkpvAh', 'Cix2bHfqPcKcM233mzxbLk14kSggUUiz2A87fJtGivXr', 
        'HRyRhQ86t3H4aAtgvHVpUJmw64BDrb61gRiKcdKUXs5c', 'Gai4gFvzQV9iNfHg5CBaTHAgjy35B6rJznEaD9WjZoTz', 'Dz8rMcdokTLfbnNz2ZdYocZixgaA1TMqbA31xtwPgcxb', 
        '2WoQNgmc4SEXrR3rKQypmeWmsxGqHHE6rApnVrP6Pt77', '3vGEsQA5jzvN8TBgytuYEdZxW6P2pK1c6pq56JiFuygS', 'D8f3WkQu6dCF33cZxuAsrKHrGsqGP2yvAHf8mX6RXnwf', 
        'ENxTEjSQ1YabmUpXAdCgevnHQ9MHdLv8tzFiuiYJqa13', 'GQPFicsy3P3NXxB5piJohoxACqTvWE9fKpLgdsMduoHE', 'Ey2JEr8hDkgN8qKJGrLf2yFjRhW7rab99HVxwi5rcvJE', 
        'TpdxgNJBWZRL8UXF5mrEsyWxDWx9HQexA9P1eTWQ42p', '6MgjyQU7G988jgL6EGAgfHYoeesCnwYMyPeh1fpJ71FP', 'J9BMEWFbCBEjtQ1fG5Lo9kouX1HfrKQxeUxetwXrifBw', 
        '12pHu2j2DDShyCVFU7vtSLXga74et9y83VD38mw6XYhB', '6SiVU5WEwqfFapRuYCndomztEwDjvS5xgtEof3PLEGm9', '6rYLG55Q9RpsPGvqdPNJs4z5WTxJVatMB8zV3WJhs5EK', 
        'AumQWSLrWwDXRq1yDEYPiw8vT5NUBYzrbdWCprJ4ZUa8', '7toBU3inhmrARGngC7z6SjyP85HgGMmCTEwGNRAcYnEK', '4gh9m7RV7G4WwRftA6qV7RhDfytdepb3XbxFRfTtneYJ', 
        'ForLDu55GfA2U1aTUaitmjzjs92vvVn1MSqzY3D9HtAK', '7y4whZmw388w1ggjToDLSBLv47drw5SUXcLk6jtmwixd', 'DiTmWENJsHQdawVUUKnUXkconcpW4Jv52TnMWhkncF6t', 
        '8mR3wB1nh4D6J9RUCugxUpc6ya8w38LPxZ3ZjcBhgzws', '4iUgjMT8q2hNZnLuhpqZ1QtiV8deFPy2ajvvjEpKKgsS', 'CZubxabMM7CPFSDAfMUhxNuvXRDLjDf6yVVq1RoJ66rk', 
        '8U1JPQh3mVQ4F5jwRdFTBzvNRQaYFQppHQYoH38DJGSQ', 'FCjUJZ1qozm1e8romw216qyfQMaaWKxWsuySnumVCCNe'
    ) AND (acc.post_balance - acc.pre_balance) > 0
),
aggregated_tips AS (
    SELECT
        signature,
        SUM(lamports_change) as total_tip
    FROM balance_changes
    GROUP BY signature
)
SELECT
  t.signature,
  t.success,
  at.total_tip AS on_chain_tip_lamports
FROM
  all_transactions t
LEFT JOIN
  aggregated_tips at ON t.signature = at.signature
ORDER BY
  t.block_time DESC;
"""

def run_dune_query(query_sql: str, api_key: str):
    """Creates, executes, fetches results for, and then archives a Dune query."""
    headers = {"x-dune-api-key": api_key, "Content-Type": "application/json"}
    query_id = None
    try:
        # Create a temporary query on Dune
        create_resp = requests.post(
            "https://api.dune.com/api/v1/query",
            headers=headers,
            json={"name": "Temp - API Jito Subsidy Analysis", "query_sql": query_sql, "is_private": True}
        )
        create_resp.raise_for_status()
        query_id = create_resp.json()["query_id"]

        # Execute the query
        execute_resp = requests.post(f"https://api.dune.com/api/v1/query/{query_id}/execute", headers=headers)
        execute_resp.raise_for_status()
        execution_id = execute_resp.json()["execution_id"]
        log(f"Executing Dune query... Execution ID: {execution_id}")

        # Poll for results until completion
        while True:
            result_resp = requests.get(f"https://api.dune.com/api/v1/execution/{execution_id}/results", headers=headers)
            result_resp.raise_for_status()
            data = result_resp.json()
            state = data["state"]

            if state == "QUERY_STATE_COMPLETED":
                log("Dune query finished successfully.")
                return data["result"]["rows"]
            elif state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
                error_details = data.get('error', 'No error details provided.')
                log(f"Query failed with state '{state}': {error_details}")
                return None
            
            log("Dune query is running, waiting...")
            time.sleep(5)

    except requests.exceptions.HTTPError as e:
        log(f"An HTTP error occurred with the Dune API: {e}")
        if e.response:
            log(f"Response Body: {e.response.text}")
        return None
    except Exception as e:
        log(f"An unexpected error occurred: {e}")
        return None
    finally:
        if query_id:
            log(f"Archiving temporary Dune query (ID: {query_id})...")
            try:
                archive_headers = {"x-dune-api-key": api_key}
                archive_resp = requests.post(f"https://api.dune.com/api/v1/query/{query_id}/archive", headers=archive_headers)
                
                # Check for success based on the presence of 'query_id' in the response
                if archive_resp.status_code == 200 and 'query_id' in archive_resp.json():
                    log(f"Successfully archived Dune query {query_id}.")
                else:
                    log(f"Warning: Failed to archive Dune query {query_id}. Status: {archive_resp.status_code}, Response: {archive_resp.text}")
            except requests.exceptions.RequestException as e:
                log(f"Warning: An exception occurred while trying to archive Dune query {query_id}: {e}")




# --- Jito API Functions (Improved) ---
def robust_get(url, proxies=None):
    """Tries a GET request with a proxy, then falls back to no proxy."""
    try:
        response = requests.get(url, proxies=proxies, timeout=10)
        if response.status_code == 200: return response
    except requests.exceptions.RequestException: pass
    try: return requests.get(url, timeout=10)
    except requests.exceptions.RequestException: return None

def get_bundle_details(signature: str, proxy_url: str) -> dict:
    """For a given signature, finds its bundle details and landed tip from the Jito API."""
    proxies = {'http': proxy_url, 'https': proxy_url}
    id_resp = robust_get(f"{JITO_BUNDLES_URL}/transaction/{signature}", proxies=proxies)
    if not (id_resp and id_resp.status_code == 200): 
        return None
    try:
        id_data = id_resp.json()
        # Handle "Bundle not found" error response
        if isinstance(id_data, dict) and "error" in id_data:
            if "Bundle not found" in id_data["error"]:
                return None  # Normal case - most transactions are not in bundles
            else:
                log(f"Jito API error for {signature}: {id_data['error']}")
                return None
        
        if not isinstance(id_data, list) or not id_data: 
            return None
        bundle_id = id_data[0].get("bundle_id")
        if not bundle_id: 
            return None
    except (IndexError, KeyError, requests.exceptions.JSONDecodeError): 
        return None

    bundle_resp = robust_get(f"{JITO_BUNDLES_URL}/bundle/{bundle_id}", proxies=proxies)
    if not (bundle_resp and bundle_resp.status_code == 200): 
        return None
    try:
        bundle_data_list = bundle_resp.json()
        if not isinstance(bundle_data_list, list) or not bundle_data_list: 
            return None
        bundle_data = bundle_data_list[0]
        landed_tip = bundle_data.get("landedTipLamports")
        if landed_tip is not None:
            return {"signature": signature, "jito_tip_lamports": landed_tip}
        return None
    except (IndexError, KeyError, requests.exceptions.JSONDecodeError): 
        return None

# --- Helius enrichment + final CSV pipeline ---

# CONFIG: set your Helius API key here (or env var)
HELIUS_API_KEY = "1ac8e73b-5da0-454a-a6df-c97e500fdf9d"
HELIUS_TXNS_ENDPOINT = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}"
HELIUS_BATCH_SIZE = 100  # Helius accepts up to 100 txns per request
# New: single-transaction endpoint base
HELIUS_SINGLE_TX_ENDPOINT = "https://api.helius.xyz/v0/transactions"
# New: Helius RPC endpoint for reliable blockTime/fee/slot
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
HELIUS_BACKOFF_SECONDS = 0.5
HELIUS_MAX_BACKOFF = 8

# Use an up-to-date block compute limit (adjustable)
# Historically this has been ~48_000_000 CU per block; tune if needed.
MAX_COMPUTE_PER_BLOCK = 48_000_000

def chunked(iterable: List, size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def fetch_helius_enhanced(sigs: List[str]) -> List[dict]:
    """Batch POST to Helius /v0/transactions. Returns list aligned to requested signatures.
    Fills with None on failures, includes robust backoff and result-shape handling."""
    results: List[dict] = []
    headers = {"Content-Type": "application/json"}
    backoff = HELIUS_BACKOFF_SECONDS
    for batch in chunked(sigs, HELIUS_BATCH_SIZE):
        payload = {"transactions": batch}
        try:
            resp = requests.post(HELIUS_TXNS_ENDPOINT, headers=headers, json=payload, timeout=30)
            if resp.status_code == 429:
                time.sleep(backoff)
                backoff = min(backoff * 2, HELIUS_MAX_BACKOFF)
                resp = requests.post(HELIUS_TXNS_ENDPOINT, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # Helius typically returns a list of enhanced tx objects. If dict with key present, unwrap.
            if isinstance(data, dict):
                # try common keys
                if "transactions" in data and isinstance(data["transactions"], list):
                    data_list = data["transactions"]
                elif "result" in data and isinstance(data["result"], list):
                    data_list = data["result"]
                else:
                    data_list = []
            else:
                data_list = data or []
            # Align to requested batch length by padding with None if API omitted some
            if len(data_list) < len(batch):
                data_list = list(data_list) + [None] * (len(batch) - len(data_list))
            results.extend(data_list[:len(batch)])
        except requests.exceptions.RequestException as e:
            log(f"Helius fetch error for batch starting with {batch[0]}: {e}")
            results.extend([None] * len(batch))
    return results

def safe_get_compute(enh):
    """Try multiple paths to extract compute units used from Helius enhanced object."""
    if not enh:
        return None
    
    # Enhanced API does NOT provide computeUnitsConsumed - only RPC API does
    # First try: meta.computeUnitsConsumed (RPC API only)
    try:
        if 'meta' in enh and isinstance(enh['meta'], dict):
            meta = enh['meta']
            if 'computeUnitsConsumed' in meta:
                return meta['computeUnitsConsumed']
            if 'computeUnits' in meta:
                return meta['computeUnits']
            if 'execution' in meta and isinstance(meta['execution'], dict) and 'computeUnitsConsumed' in meta['execution']:
                return meta['execution']['computeUnitsConsumed']
    except Exception:
        pass
    
    # Second try: direct computeUnitsConsumed field (unlikely in Enhanced API)
    try:
        if 'computeUnitsConsumed' in enh:
            return enh['computeUnitsConsumed']
    except Exception:
        pass
    
    # Third try: executionCost field (fallback)
    try:
        if 'executionCost' in enh:
            return enh['executionCost'].get('computeUnitsConsumed') if isinstance(enh['executionCost'], dict) else None
    except Exception:
        pass
    
    # Enhanced API doesn't provide compute units - return None
    return None

# New: fetch one transaction's enhanced data directly from Helius
def fetch_helius_single(signature: str) -> dict:
    # Use the batch endpoint with single signature instead of individual endpoint
    url = f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}"
    try:
        resp = requests.post(url, 
                           headers={"Content-Type": "application/json"},
                           json={"transactions": [signature]}, 
                           timeout=15)
        if resp.status_code == 429:
            time.sleep(0.5)
            resp = requests.post(url, 
                               headers={"Content-Type": "application/json"},
                               json={"transactions": [signature]}, 
                               timeout=15)
        resp.raise_for_status()
        data = resp.json()
        # Return first result or empty dict
        if isinstance(data, list) and len(data) > 0:
            return data[0] or {}
        return {}
    except requests.exceptions.RequestException:
        return {}

# New: build realtime metrics dict per signature
def fetch_single_metrics(signature: str) -> dict:
    data = fetch_helius_single(signature)
    compute_units = safe_get_compute(data)
    
    # Fee in lamports - Enhanced API has 'fee' directly
    fee_lamports = None
    try:
        fee_lamports = data.get('fee') or (data.get('meta') or {}).get('fee')
    except Exception:
        fee_lamports = None
    fee_sol = (fee_lamports / 1e9) if isinstance(fee_lamports, (int, float)) else None
    
    # Block time - Enhanced API uses 'timestamp', RPC uses 'blockTime'
    block_time = data.get('timestamp') or data.get('blockTime') or (data.get('meta') or {}).get('blockTime')
    latency_seconds = None
    try:
        if block_time:
            now_ts = int(time.time())
            latency_seconds = max(0, now_ts - int(block_time))
    except Exception:
        latency_seconds = None
    
    return {
        'signature': signature,
        'compute_units_rt': compute_units,
        'fee_lamports_rt': fee_lamports,
        'fee_sol_rt': fee_sol,
        'latency_seconds_rt': latency_seconds,
    }

def parse_helius_enhanced_record(enh, fallback_signature: str = None) -> Dict:
    """Extract robust list of features from one enhanced transaction object (Helius)."""
    if not enh:
        return {"signature": fallback_signature}
    out = {}
    
    # Standard basics
    try:
        out['signature'] = enh.get('signature') or enh.get('txSignature') or fallback_signature
    except Exception:
        out['signature'] = fallback_signature
    
    # Slot - Enhanced API has 'slot', RPC has nested structure
    try:
        out['slot'] = enh.get('slot') or (enh.get('meta') or {}).get('slot') or None
    except Exception:
        out['slot'] = None
    
    # Block time - Enhanced API has 'timestamp', RPC has 'blockTime'
    try:
        out['block_time'] = (
            enh.get('timestamp') or 
            enh.get('blockTime') or 
            (enh.get('meta') or {}).get('blockTime') or 
            None
        )
    except Exception:
        out['block_time'] = None
    
    # Fee - both APIs have it but in different places
    try:
        out['fee'] = (
            enh.get('fee') or 
            (enh.get('meta') or {}).get('fee') or 
            None
        )
    except Exception:
        out['fee'] = None

    # Compute units used - use our improved function
    out['compute_units'] = safe_get_compute(enh)

    # Instruction count and program ids
    instrs = []
    try:
        # Enhanced API has instructions directly, RPC has nested structure
        instructions = (
            enh.get('instructions') or 
            enh.get('transaction', {}).get('message', {}).get('instructions') or 
            enh.get('message', {}).get('instructions') or 
            []
        )
        out['instruction_count'] = len(instructions)
        for ins in instructions:
            # Handle different instruction formats
            if isinstance(ins, dict):
                pid = ins.get('programName') or ins.get('programId') or ins.get('program')
            else:
                pid = str(ins)
            if pid:
                instrs.append(str(pid))
    except Exception:
        out['instruction_count'] = None
    out['program_ids'] = ", ".join(sorted(set(instrs))) if instrs else None

    # Account count (unique accounts impacted)
    try:
        accounts = set()
        # Enhanced API has accountData array
        if 'accountData' in enh and isinstance(enh['accountData'], list):
            for acc in enh['accountData']:
                if isinstance(acc, dict) and 'account' in acc:
                    accounts.add(acc['account'])
        
        # Fallback to other account structures
        if not accounts and 'accounts' in enh and isinstance(enh['accounts'], list):
            for a in enh['accounts']:
                if isinstance(a, dict) and 'pubkey' in a:
                    accounts.add(a['pubkey'])
                elif isinstance(a, str):
                    accounts.add(a)
                    
        # RPC structure fallback
        if not accounts:
            message = enh.get('transaction', {}).get('message') or enh.get('message') or {}
            for ak in (message.get('accountKeys') or []):
                if isinstance(ak, dict) and 'pubkey' in ak:
                    accounts.add(ak['pubkey'])
                elif isinstance(ak, str):
                    accounts.add(ak)
        
        out['account_count'] = len(accounts)
    except Exception:
        out['account_count'] = None

    # Balance changes summary - Enhanced API has accountData with nativeBalanceChange
    try:
        total_positive = 0
        total_negative = 0
        
        # Enhanced API structure
        if 'accountData' in enh and isinstance(enh['accountData'], list):
            for acc in enh['accountData']:
                if isinstance(acc, dict) and 'nativeBalanceChange' in acc:
                    change = acc['nativeBalanceChange']
                    if change > 0:
                        total_positive += change
                    else:
                        total_negative += abs(change)
        else:
            # Fallback to other balance change structures
            bal_changes = enh.get('balanceChanges') or enh.get('meta', {}).get('prePostBalancesChanges') or []
        for bc in bal_changes:
            if isinstance(bc, dict):
                pre = bc.get('preBalance') or bc.get('pre_balance') or 0
                post = bc.get('postBalance') or bc.get('post_balance') or 0
                delta = (post or 0) - (pre or 0)
                if delta > 0:
                    total_positive += delta
                else:
                    total_negative += abs(delta)
        
        out['total_positive_balance_changes'] = total_positive
        out['total_negative_balance_changes'] = total_negative
    except Exception:
        out['total_positive_balance_changes'] = None
        out['total_negative_balance_changes'] = None

    # Convenience: hour / weekday from timestamp
    try:
        if out.get('block_time'):
            dt = datetime.fromtimestamp(int(out['block_time']))
            out['hour'] = dt.hour
            out['weekday'] = dt.weekday()  # 0=Mon
        else:
            out['hour'] = None
            out['weekday'] = None
    except Exception:
        out['hour'] = None
        out['weekday'] = None

    return out

# New: fetch blockTime, fee, slot via Helius RPC getTransaction
def fetch_helius_rpc_tx(signature: str) -> dict:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "commitment": "finalized"}
        ]
    }
    try:
        resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=20)
        if resp.status_code == 429:
            time.sleep(HELIUS_BACKOFF_SECONDS)
            resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=20)
        resp.raise_for_status()
        j = resp.json() or {}
        res = j.get("result") or {}
        meta = res.get("meta") or {}
        return {
            "signature": signature,
            "slot_rpc": res.get("slot"),
            "block_time_rpc": res.get("blockTime"),
            "fee_lamports_rpc": meta.get("fee"),
            "compute_units_rpc": meta.get("computeUnitsConsumed")  # CRITICAL: Extract compute units
        }
    except requests.exceptions.RequestException:
        return {
            "signature": signature, 
            "slot_rpc": None, 
            "block_time_rpc": None, 
            "fee_lamports_rpc": None,
            "compute_units_rpc": None
        }

# --- Slot Leader and Density Functions ---
def hel_rpc_post(method: str, params: list) -> dict:
    """Helper to make Helius RPC calls."""
    if not HELIUS_API_KEY:
        return {}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=30)
        if resp.status_code == 429:
            time.sleep(HELIUS_BACKOFF_SECONDS)
            resp = requests.post(HELIUS_RPC_URL, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json() or {}
    except requests.exceptions.RequestException:
        return {}

def fetch_slot_leaders(start_slot: int, end_slot: int) -> dict:
    """Fetch slot leaders for a range of slots. Returns {slot: validator_identity}.
    Handles API limit of 5000 slots per request with batching."""
    if not HELIUS_API_KEY:
        return {}
    
    leaders_map = {}
    total_slots = end_slot - start_slot + 1
    MAX_BATCH_SIZE = 5000  # API limit
    
    try:
        # Process in batches if range is too large
        for batch_start in range(start_slot, end_slot + 1, MAX_BATCH_SIZE):
            batch_end = min(batch_start + MAX_BATCH_SIZE - 1, end_slot)
            batch_size = batch_end - batch_start + 1
            
            log(f"Fetching slot leaders: {batch_start} to {batch_end} ({batch_size} slots)")
            
            result = hel_rpc_post("getSlotLeaders", [batch_start, batch_size])
            
            # Check for RPC errors (like unavailable leader schedule)
            if "error" in result:
                error_msg = result["error"].get("message", "Unknown RPC error")
                log(f"[warn] getSlotLeaders RPC error for slots {batch_start}-{batch_end}: {error_msg}")
                continue  # Skip this batch, continue with others
                
            leaders = result.get("result", [])
            if not leaders:
                log(f"[warn] No slot leaders returned for range {batch_start}-{batch_end}")
                continue
            
            # Map slot numbers to validators
            for i, leader in enumerate(leaders):
                if leader:  # Skip null/empty leaders
                    slot_num = batch_start + i
                    leaders_map[slot_num] = leader
            
            # Add small delay between batches to be nice to API
            if batch_end < end_slot:
                time.sleep(0.1)
                
        log(f"Successfully fetched {len(leaders_map)} slot leaders out of {total_slots} requested slots")
        return leaders_map
        
    except Exception as e:
        log(f"[warn] fetch_slot_leaders failed: {e}")
        return {}

def fetch_individual_slot_leader(slot: int) -> str:
    """Fetch individual slot leader using getSlotLeader RPC method."""
    if not HELIUS_API_KEY:
        return None
    
    try:
        result = hel_rpc_post("getSlotLeader", [slot])
        if "error" in result:
            return None
        return result.get("result")
    except Exception:
        return None

def add_validator_identity(df: pd.DataFrame) -> pd.DataFrame:
    """Add validator_identity column by mapping slot to validator.
    Uses sampling approach for large datasets and stores results in array format."""
    if 'slot' not in df.columns:
        df['validator_identity'] = None
        df['validator_data'] = None
        return df
    
    slots = pd.to_numeric(df['slot'], errors='coerce').dropna().astype(int)
    if slots.empty or not HELIUS_API_KEY:
        if not HELIUS_API_KEY:
            log("[warn] HELIUS_API_KEY not set; skipping validator mapping")
        df['validator_identity'] = None
        df['validator_data'] = None
        return df
    
    lo, hi = int(slots.min()), int(slots.max())
    span = hi - lo
    unique_slots = sorted(slots.unique())
    
    log(f"[info] Validator mapping: {len(unique_slots)} unique slots, range {lo} to {hi} (span: {span})")
    
    # Always use sampling approach for efficiency and to handle old epochs
    MAX_SAMPLE_SLOTS = 500  # Reasonable sample size for API calls
    BATCH_SIZE = 50  # Process in small batches to avoid rate limits
    
    # Sample slots evenly across the range
    if len(unique_slots) > MAX_SAMPLE_SLOTS:
        sample_indices = np.linspace(0, len(unique_slots)-1, MAX_SAMPLE_SLOTS, dtype=int)
        sample_slots = [unique_slots[i] for i in sample_indices]
        log(f"[info] Sampling {len(sample_slots)} slots from {len(unique_slots)} unique slots")
    else:
        sample_slots = unique_slots
        log(f"[info] Processing all {len(sample_slots)} unique slots")
    
    # Fetch validator data for sampled slots using individual getSlotLeader calls
    validator_data = []
    leaders_map = {}
    
    log(f"[info] Fetching validators for {len(sample_slots)} slots in batches of {BATCH_SIZE}")
    
    for i in range(0, len(sample_slots), BATCH_SIZE):
        batch_slots = sample_slots[i:i+BATCH_SIZE]
        batch_start = i + 1
        batch_end = min(i + BATCH_SIZE, len(sample_slots))
        
        log(f"[info] Processing batch {batch_start}-{batch_end}/{len(sample_slots)}")
        
        for slot in batch_slots:
            validator = fetch_individual_slot_leader(slot)
            if validator:
                leaders_map[slot] = validator
                validator_data.append({
                    'slot': slot,
                    'validator_identity': validator,
                    'epoch': slot // 432000,  # Approximate epoch calculation
                    'slot_in_epoch': slot % 432000
                })
        
        # Small delay between batches to be nice to API
        if batch_end < len(sample_slots):
            time.sleep(0.2)
    
    log(f"[info] Successfully fetched {len(leaders_map)} validator mappings from {len(sample_slots)} sampled slots")
    
    if not leaders_map:
        log("[warn] No validator data retrieved; likely due to old epoch data")
        df['validator_identity'] = None
        df['validator_data'] = None
        return df
    
    # Map slots to validators (using nearest available data for unmapped slots)
    def get_validator_for_slot(s):
        if pd.isna(s):
            return None
        slot_int = int(s)
        
        # Direct match first
        if slot_int in leaders_map:
            return leaders_map[slot_int]
        
        # Find nearest slot with validator data (within reasonable range)
        nearest_slot = None
        min_distance = float('inf')
        
        for mapped_slot in leaders_map.keys():
            distance = abs(slot_int - mapped_slot)
            if distance < min_distance and distance <= 10000:  # Within 10k slots (~1 hour)
                min_distance = distance
                nearest_slot = mapped_slot
        
        return leaders_map.get(nearest_slot) if nearest_slot else None
    
    df['validator_identity'] = df['slot'].apply(get_validator_for_slot)
    
    # Store complete validator data as JSON string for analysis
    df['validator_data'] = df['slot'].apply(lambda s: 
        json.dumps([v for v in validator_data if abs(v['slot'] - int(s)) <= 10000]) if pd.notna(s) and validator_data else None
    )
    
    mapped_count = df['validator_identity'].notna().sum()
    success_rate = (mapped_count / len(df)) * 100 if len(df) > 0 else 0
    
    log(f"[info] Validator mapping complete: {mapped_count}/{len(df)} transactions mapped ({success_rate:.1f}%)")
    log(f"[info] Unique validators found: {df['validator_identity'].nunique()}")
    
    # Save validator data summary
    if validator_data:
        validator_summary = {
            'total_slots_sampled': len(sample_slots),
            'successful_mappings': len(leaders_map),
            'unique_validators': len(set(v['validator_identity'] for v in validator_data)),
            'validator_list': list(set(v['validator_identity'] for v in validator_data)),
            'slot_range': {'min': lo, 'max': hi, 'span': span},
            'sample_data': validator_data[:50]  # First 50 for debugging
        }
        
        try:
            validator_file = os.path.join(OUTPUT_DIR, f"validator_mappings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(validator_file, 'w') as f:
                json.dump(validator_summary, f, indent=2)
            log(f"[info] Validator summary saved: {validator_file}")
        except Exception as e:
            log(f"[warn] Could not save validator summary: {e}")
    
    return df

def add_density_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add density features: tx per slot, rolling 5min window counts."""
    if df.empty or 'slot' not in df.columns:
        df['slot_tx_count'] = None
        df['win5m_tx_count'] = None
        df['win5m_program_count'] = None
        return df
    
    # Convert block_time to datetime for rolling windows
    df['ts'] = pd.to_datetime(df['block_time'], unit='s', errors='coerce')
    
    # 1. Transactions per slot
    slot_counts = df.groupby('slot').size().rename('slot_tx_count')
    df = df.merge(slot_counts.reset_index(), on='slot', how='left')
    
    # 2. Rolling 5-minute window features (only for valid timestamps)
    valid_ts_df = df.dropna(subset=['ts']).copy().sort_values('ts')
    if not valid_ts_df.empty:
        try:
            # Rolling 5min transaction count - use count() instead of size()
            valid_ts_df = valid_ts_df.set_index('ts')
            valid_ts_df['win5m_tx_count'] = valid_ts_df.rolling('5min')['signature'].count()
            
            # Rolling 5min program count per transaction
            if 'program_ids' in valid_ts_df.columns:
                # Create a simpler approach for program counting
                valid_ts_df['program_count'] = valid_ts_df['program_ids'].apply(
                    lambda x: len(str(x).split(', ')) if pd.notna(x) and str(x) != '' else 0
                )
                valid_ts_df['win5m_program_count'] = valid_ts_df.rolling('5min')['program_count'].sum()
            else:
                valid_ts_df['win5m_program_count'] = None
            
            # Merge back to original dataframe
            valid_ts_df = valid_ts_df.reset_index()
            df = df.merge(
                valid_ts_df[['signature', 'win5m_tx_count', 'win5m_program_count']],
                on='signature', how='left'
            )
        except Exception as e:
            log(f"Warning: Rolling window calculation failed: {e}")
            df['win5m_tx_count'] = None
            df['win5m_program_count'] = None
    else:
        df['win5m_tx_count'] = None
        df['win5m_program_count'] = None
    
    # Fill missing values
    for col in ['slot_tx_count', 'win5m_tx_count', 'win5m_program_count']:
        if col not in df.columns:
            df[col] = None
    
    return df

# --- Analysis Functions ---
def analyze_tip_distribution(df: pd.DataFrame) -> dict:
    """Analyze tip distribution with variance and correlation metrics."""
    analysis = {}
    
    # Convert to SOL for analysis
    df['on_chain_tip_sol'] = df['on_chain_tip_lamports'] / 1e9
    df['jito_tip_sol'] = df['jito_tip_lamports'] / 1e9
    df['profit_sol'] = df['on_chain_tip_sol'] - df['jito_tip_sol'].fillna(0)
    
    # Basic tip statistics - handle empty data
    valid_tips = df['on_chain_tip_sol'].dropna()
    if len(valid_tips) > 0:
        tip_stats = valid_tips.describe()
        analysis['tip_stats'] = tip_stats.to_dict()
        
        # Variance and correlation analysis
        if len(valid_tips) > 1:
            analysis['tip_variance'] = float(valid_tips.var())
            analysis['tip_std'] = float(valid_tips.std())
        else:
            analysis['tip_variance'] = 0.0
            analysis['tip_std'] = 0.0
    else:
        analysis['tip_stats'] = {'count': 0, 'mean': 0, 'std': 0, 'min': 0, 'max': 0}
        analysis['tip_variance'] = 0.0
        analysis['tip_std'] = 0.0
    
    # Correlation analysis
    valid_profits = df['profit_sol'].dropna()
    if len(valid_tips) > 1 and len(valid_profits) > 1:
        # Pearson correlation between tips and profits
        common_idx = df.dropna(subset=['on_chain_tip_sol', 'profit_sol']).index
        if len(common_idx) > 1:
            try:
                correlation = df.loc[common_idx, 'on_chain_tip_sol'].corr(df.loc[common_idx, 'profit_sol'])
                analysis['tip_profit_correlation'] = float(correlation) if not pd.isna(correlation) else 0.0
            except Exception:
                analysis['tip_profit_correlation'] = 0.0
        else:
            analysis['tip_profit_correlation'] = 0.0
    else:
        analysis['tip_profit_correlation'] = 0.0
    
    log(f"Tip Distribution Analysis: Mean={analysis['tip_stats'].get('mean', 0):.6f} SOL, "
        f"Std={analysis.get('tip_std', 0):.6f}, Correlation={analysis.get('tip_profit_correlation', 'N/A')}")
    
    return analysis

def time_varied_analysis(df: pd.DataFrame) -> dict:
    """Perform time-varied analysis on subsidy data."""
    if 'ts' not in df.columns or df['ts'].isna().all():
        return {"error": "No valid timestamps for time analysis"}
    
    analysis = {}
    
    # Filter out invalid subsidy data
    valid_subsidy_df = df.dropna(subset=['subsidy_percentage'])
    if valid_subsidy_df.empty:
        return {"error": "No valid subsidy data for time analysis"}
    
    try:
        # Hourly subsidy patterns
        valid_subsidy_df['hour'] = valid_subsidy_df['ts'].dt.hour
        hourly_subsidies = valid_subsidy_df.groupby('hour')['subsidy_percentage'].agg(['mean', 'count', 'std']).reset_index()
        hourly_subsidies = hourly_subsidies.fillna(0)  # Fill NaN std values
        analysis['hourly_patterns'] = hourly_subsidies.to_dict('records')
        
        # Daily patterns
        valid_subsidy_df['date'] = valid_subsidy_df['ts'].dt.date
        daily_subsidies = valid_subsidy_df.groupby('date')['subsidy_percentage'].agg(['mean', 'count', 'std']).reset_index()
        daily_subsidies = daily_subsidies.fillna(0)  # Fill NaN std values
        daily_subsidies['date'] = daily_subsidies['date'].astype(str)  # for JSON serialization
        analysis['daily_patterns'] = daily_subsidies.to_dict('records')
        
        # Weekday patterns
        valid_subsidy_df['weekday'] = valid_subsidy_df['ts'].dt.dayofweek
        weekday_subsidies = valid_subsidy_df.groupby('weekday')['subsidy_percentage'].agg(['mean', 'count', 'std']).reset_index()
        weekday_subsidies = weekday_subsidies.fillna(0)  # Fill NaN std values
        analysis['weekday_patterns'] = weekday_subsidies.to_dict('records')
        
        log(f"Time Analysis: Found patterns across {len(hourly_subsidies)} hours, "
            f"{len(daily_subsidies)} days, {len(weekday_subsidies)} weekdays")
    except Exception as e:
        log(f"Warning: Time analysis failed: {e}")
        analysis = {"error": f"Time analysis failed: {str(e)}"}
    
    return analysis

def analyze_validator_patterns(df: pd.DataFrame) -> dict:
    """Analyze stake delegation patterns on validators with varying subsidy levels."""
    if 'validator_identity' not in df.columns or df['validator_identity'].isna().all():
        return {"error": "No validator identity data available"}
    
    analysis = {}
    
    try:
        # Filter valid validator data
        valid_validator_df = df.dropna(subset=['validator_identity'])
        if valid_validator_df.empty:
            return {"error": "No valid validator data available"}
        
        # Validator subsidy statistics
        validator_stats = valid_validator_df.groupby('validator_identity').agg({
            'subsidy_percentage': ['mean', 'count', 'std'],
            'on_chain_tip_lamports': ['sum', 'mean'],
            'jito_tip_lamports': ['sum', 'mean'],
            'slot_tx_count': 'mean'
        }).reset_index()
        
        # Flatten column names and fill NaN values
        validator_stats.columns = ['validator_identity'] + [
            f"{col[0]}_{col[1]}" if col[1] else col[0] 
            for col in validator_stats.columns[1:]
        ]
        validator_stats = validator_stats.fillna(0)  # Fill NaN values
        
        analysis['validator_patterns'] = validator_stats.to_dict('records')
        
        # Top validators by tip volume
        if 'on_chain_tip_lamports_sum' in validator_stats.columns:
            top_validators = validator_stats.nlargest(10, 'on_chain_tip_lamports_sum')
            analysis['top_tip_validators'] = top_validators.to_dict('records')
        
        log(f"Validator Analysis: Found {len(validator_stats)} unique validators")
    except Exception as e:
        log(f"Warning: Validator analysis failed: {e}")
        analysis = {"error": f"Validator analysis failed: {str(e)}"}
    
    return analysis

# --- Main script execution (integrated: Dune -> Jito -> Helius -> CSV + plots) ---
if __name__ == "__main__":
    dune_results = run_dune_query(SQL_QUERY, DUNE_API_KEY)
    
    OUTPUT_DIR = "/home/shubham/Downloads/subsidy-working/output"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    if not dune_results:
        log("Could not retrieve initial data from Dune.")
        exit(1)

    dune_df = pd.DataFrame(dune_results)
    dune_csv = os.path.join(OUTPUT_DIR, f"dune_results_{timestamp_str}.csv")
    dune_df.to_csv(dune_csv, index=False)
    log(f"✅ Dune results saved to: {dune_csv}")

    # CRITICAL: Remove duplicates to prevent data quality issues
    initial_count = len(dune_df)
    dune_df = dune_df.drop_duplicates(subset=['signature'], keep='first')
    final_count = len(dune_df)
    duplicates_removed = initial_count - final_count
    if duplicates_removed > 0:
        log(f"⚠️  Removed {duplicates_removed} duplicate signatures ({initial_count} → {final_count})")
    else:
        log(f"✅ No duplicates found in {initial_count} transactions")

    signatures = dune_df['signature'].tolist()
    log(f"Fetched {len(signatures)} transactions from Dune. Now checking Jito API for bundle details...")

    jito_bundle_info = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_sig = {executor.submit(get_bundle_details, sig, next(proxy_cycle)): sig for sig in signatures}
        for i, future in enumerate(as_completed(future_to_sig)):
            print(f"  - Processed signature {i+1}/{len(signatures)}...", end='\r')
            try:
                result = future.result()
            except Exception as e:
                log(f"Error in Jito future for signature {future_to_sig.get(future)}: {e}")
                result = None
            if result:
                jito_bundle_info.append(result)
    print()
    log("Jito API processing complete.")

    jito_df = pd.DataFrame(jito_bundle_info)
    jito_csv = os.path.join(OUTPUT_DIR, f"jito_results_{timestamp_str}.csv")
    jito_df.to_csv(jito_csv, index=False)
    log(f"✅ Jito results saved to: {jito_csv}")

    merged_dune_jito = pd.merge(dune_df, jito_df, on='signature', how='left')

    sigs_for_helius = merged_dune_jito['signature'].tolist()
    log(f"Fetching Helius enhanced data for {len(sigs_for_helius)} signatures in batches of {HELIUS_BATCH_SIZE}...")
    helius_enhanced_raw = fetch_helius_enhanced(sigs_for_helius)
    
    helius_raw_json = os.path.join(OUTPUT_DIR, f"helius_enhanced_raw_{timestamp_str}.json")
    if WRITE_HELIUS_RAW:
        try:
            with open(helius_raw_json, 'w', encoding='utf-8') as f:
                json.dump(helius_enhanced_raw, f, ensure_ascii=False)
            log(f"✅ Raw Helius enhanced data saved to: {helius_raw_json}")
        except Exception as e:
            log(f"Warning: could not write raw Helius JSON: {e}")

    parsed_records = [parse_helius_enhanced_record(enh, sig) for sig, enh in zip(sigs_for_helius, helius_enhanced_raw)]
    helius_df = pd.DataFrame(parsed_records)
    # Ensure at least signature column exists
    if 'signature' not in helius_df.columns and len(sigs_for_helius) > 0:
        helius_df['signature'] = sigs_for_helius

    # Fetch RPC blockTime/fee/slot concurrently and merge into helius_df
    # NOTE: Enhanced API doesn't provide computeUnitsConsumed, so we rely on RPC data for compute units
    # CRITICAL: We need RPC for ALL transactions to get complete data including compute units
    try:
        log(f"Fetching Helius RPC getTransaction for {len(sigs_for_helius)} signatures (concurrent)...")
        rpc_results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futs = {executor.submit(fetch_helius_rpc_tx, s): s for s in sigs_for_helius}
            for i, fut in enumerate(as_completed(futs)):
                if (i + 1) % 50 == 0 or i + 1 == len(futs):
                    print(f"  - Helius RPC {i+1}/{len(futs)}", end='\r')
                try:
                    r = fut.result()
                except Exception:
                    r = None
                if r:
                    rpc_results.append(r)
        print()
        if rpc_results:
            rpc_df = pd.DataFrame(rpc_results)
            # Merge RPC cols - this now provides the MAIN source of compute units
            helius_df = helius_df.merge(rpc_df, on='signature', how='left')
            
            # Use RPC data as PRIMARY source, Enhanced API as fallback
            if 'slot' in helius_df.columns:
                helius_df['slot'] = helius_df['slot_rpc'].where(pd.notna(helius_df['slot_rpc']), helius_df['slot'])
            else:
                helius_df['slot'] = helius_df.get('slot_rpc')
            if 'block_time' in helius_df.columns:
                helius_df['block_time'] = helius_df['block_time_rpc'].where(pd.notna(helius_df['block_time_rpc']), helius_df['block_time'])
            else:
                helius_df['block_time'] = helius_df.get('block_time_rpc')
            if 'fee' in helius_df.columns:
                helius_df['fee'] = helius_df['fee_lamports_rpc'].where(pd.notna(helius_df['fee_lamports_rpc']), helius_df['fee'])
            else:
                helius_df['fee'] = helius_df.get('fee_lamports_rpc')
                
            # CRITICAL: Set compute_units from RPC data (Enhanced API doesn't have this)
            helius_df['compute_units'] = None  # Clear any incorrect Enhanced API data
            # Use RPC compute units as the primary source
            if 'compute_units_rpc' in rpc_df.columns:
                helius_df = helius_df.merge(
                    rpc_df[['signature', 'compute_units_rpc']], 
                    on='signature', 
                    how='left', 
                    suffixes=('', '_new')
                )
                helius_df['compute_units'] = helius_df['compute_units_rpc']
                log(f"Compute units populated from RPC: {helius_df['compute_units'].notna().sum()}/{len(helius_df)} transactions")
            else:
                log("Warning: No compute_units_rpc column found")
                    
    except Exception as e:
        log(f"Warning: RPC merge step failed: {e}")

    helius_csv = os.path.join(OUTPUT_DIR, f"helius_parsed_results_{timestamp_str}.csv")
    helius_df.to_csv(helius_csv, index=False)
    log(f"✅ Parsed Helius results saved to: {helius_csv}")
    log(f"Helius enrichment returned {len(helius_df)} rows (including empty rows). Columns: {list(helius_df.columns)}")

    full_df = pd.merge(merged_dune_jito, helius_df, on='signature', how='left')

    try:
        sigs = full_df['signature'].dropna().astype(str).tolist()
        log(f"Fetching realtime Helius metrics for {len(sigs)} signatures (concurrent)...")
        rt_results = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            fut_map = {executor.submit(fetch_single_metrics, s): s for s in sigs}
            for i, fut in enumerate(as_completed(fut_map)):
                if (i + 1) % 50 == 0 or i + 1 == len(fut_map):
                    print(f"  - Helius RT {i+1}/{len(fut_map)}", end='\r')
                try:
                    res = fut.result()
                except Exception:
                    res = None
                if res:
                    rt_results.append(res)
        print()
        if rt_results:
            rt_df = pd.DataFrame(rt_results)
            rt_csv = os.path.join(OUTPUT_DIR, f"helius_rt_results_{timestamp_str}.csv")
            rt_df.to_csv(rt_csv, index=False)
            log(f"✅ Realtime Helius metrics saved to: {rt_csv}")

            for col in ['compute_units_rt', 'fee_lamports_rt', 'fee_sol_rt', 'latency_seconds_rt']:
                if col in rt_df.columns:
                    rt_df[col] = pd.to_numeric(rt_df[col], errors='coerce')
            full_df = full_df.merge(rt_df, on='signature', how='left')
            log("Realtime Helius metrics merged into DataFrame.")
    except Exception as e:
        log(f"Warning: realtime Helius metrics step failed: {e}")

    numeric_cols = ['on_chain_tip_lamports', 'jito_tip_lamports', 'fee', 'compute_units',
                    'total_positive_balance_changes', 'total_negative_balance_changes']
    for c in numeric_cols:
        if c in full_df.columns:
            full_df[c] = pd.to_numeric(full_df[c], errors='coerce')

    def calculate_subsidy(row):
        j = row.get('jito_tip_lamports')
        o = row.get('on_chain_tip_lamports')
        if pd.isna(j): return None
        if pd.notna(o) and o > 0: return ((j / o) - 1) * 100.0
        if pd.notna(j) and (pd.isna(o) or o == 0): return float('inf')
        return None

    full_df['subsidy_percentage'] = full_df.apply(calculate_subsidy, axis=1)

    if 'compute_units' in full_df.columns and 'slot' in full_df.columns:
        slot_sum = full_df.groupby('slot')['compute_units'].sum().rename('slot_compute_sum').reset_index()
        full_df = full_df.merge(slot_sum, on='slot', how='left')
        full_df['slot_congestion'] = full_df['slot_compute_sum'] / float(MAX_COMPUTE_PER_BLOCK)
    else:
        full_df['slot_compute_sum'] = None
        full_df['slot_congestion'] = None

    full_df['tip_over_fee'] = None
    if 'on_chain_tip_lamports' in full_df and 'fee' in full_df:
        full_df['tip_over_fee'] = full_df.apply(
            lambda r: (r['on_chain_tip_lamports'] / r['fee']) if pd.notna(r['on_chain_tip_lamports']) and pd.notna(r['fee']) and r['fee'] != 0 else None,
            axis=1
        )
    full_df['compute_units_final'] = full_df['compute_units_rt'].where(pd.notna(full_df.get('compute_units_rt')), full_df.get('compute_units'))
    full_df['fee_lamports_final'] = full_df['fee_lamports_rt'].where(pd.notna(full_df.get('fee_lamports_rt')), full_df.get('fee'))
    full_df['fee_sol_final'] = full_df['fee_lamports_final'] / 1e9
    full_df['tip_over_fee_rt'] = full_df.apply(lambda r: (r['on_chain_tip_lamports'] / r['fee_lamports_final']) if pd.notna(r.get('on_chain_tip_lamports')) and pd.notna(r.get('fee_lamports_final')) and r['fee_lamports_final'] != 0 else None, axis=1)
    full_df['jito_tip_over_fee_rt'] = full_df.apply(lambda r: (r['jito_tip_lamports'] / r['fee_lamports_final']) if pd.notna(r.get('jito_tip_lamports')) and pd.notna(r.get('fee_lamports_final')) and r['fee_lamports_final'] != 0 else None, axis=1)
    full_df['onchain_tip_per_cu'] = full_df.apply(lambda r: (r['on_chain_tip_lamports'] / r['compute_units_final']) if pd.notna(r.get('on_chain_tip_lamports')) and pd.notna(r.get('compute_units_final')) and r['compute_units_final'] not in (0, None) else None, axis=1)
    full_df['jito_tip_per_cu'] = full_df.apply(lambda r: (r['jito_tip_lamports'] / r['compute_units_final']) if pd.notna(r.get('jito_tip_lamports')) and pd.notna(r.get('compute_units_final')) and r['compute_units_final'] not in (0, None) else None, axis=1)
    full_df['fee_per_cu'] = full_df.apply(lambda r: (r['fee_lamports_final'] / r['compute_units_final']) if pd.notna(r.get('fee_lamports_final')) and pd.notna(r.get('compute_units_final')) and r['compute_units_final'] not in (0, None) else None, axis=1)

    def latency_bucket(x):
        try:
            if pd.isna(x): return None
            x = float(x)
            if x < 1: return '<1s'
            if x < 5: return '1-5s'
            if x < 10: return '5-10s'
            if x < 30: return '10-30s'
            if x < 60: return '30-60s'
            return '>60s'
        except Exception: return None
    if 'latency_seconds_rt' in full_df.columns:
        full_df['latency_bucket'] = full_df['latency_seconds_rt'].apply(latency_bucket)

    # Add additional calculated fields for comprehensive analysis
    log("Adding comprehensive calculated fields...")
    
    # Profit calculations (tip to provider - jito tip)
    full_df['profit_lamports'] = full_df['on_chain_tip_lamports'].fillna(0) - full_df['jito_tip_lamports'].fillna(0)
    full_df['profit_sol'] = full_df['profit_lamports'] / 1e9
    
    # Success rate and failure analysis
    full_df['is_success'] = full_df['success'].astype(bool)
    full_df['is_jito_bundle'] = full_df['jito_tip_lamports'].notna() & (full_df['jito_tip_lamports'] > 0)
    
    # CU efficiency metrics
    full_df['cu_efficiency'] = full_df.apply(
        lambda r: (r['profit_lamports'] / r['compute_units_final']) if pd.notna(r.get('compute_units_final')) and r['compute_units_final'] > 0 else None, 
        axis=1
    )
    
    # Tip intensity (tip as % of total transaction cost)
    full_df['tip_intensity'] = full_df.apply(
        lambda r: (r['on_chain_tip_lamports'] / (r['fee_lamports_final'] + r['on_chain_tip_lamports'])) * 100 
        if pd.notna(r.get('on_chain_tip_lamports')) and pd.notna(r.get('fee_lamports_final')) and (r['fee_lamports_final'] + r['on_chain_tip_lamports']) > 0 
        else None, axis=1
    )
    
    # Congestion-adjusted metrics
    full_df['tip_per_congestion'] = full_df.apply(
        lambda r: (r['on_chain_tip_lamports'] / r['slot_congestion']) if pd.notna(r.get('slot_congestion')) and r['slot_congestion'] > 0 else None,
        axis=1
    )

    # Add validator identity mapping
    log("Adding validator identity mapping...")
    full_df = add_validator_identity(full_df)
    
    # Add density features
    log("Adding density features (tx per slot, rolling windows)...")
    full_df = add_density_features(full_df)
    
    # Perform comprehensive analysis
    log("Performing tip distribution analysis...")
    tip_analysis = analyze_tip_distribution(full_df.copy())
    
    log("Performing time-varied analysis...")
    time_analysis = time_varied_analysis(full_df.copy())
    
    log("Performing validator pattern analysis...")
    validator_analysis = analyze_validator_patterns(full_df.copy())
    
    # Save comprehensive analysis results
    analysis_results = {
        'tip_distribution': tip_analysis,
        'time_patterns': time_analysis,
        'validator_patterns': validator_analysis,
        'generated_at': datetime.now().isoformat()
    }
    
    analysis_file = os.path.join(OUTPUT_DIR, f"comprehensive_analysis_{timestamp_str}.json")
    try:
        with open(analysis_file, 'w') as f:
            json.dump(analysis_results, f, indent=2, default=str)
        log(f"✅ Comprehensive analysis saved: {analysis_file}")
    except Exception as e:
        log(f"Warning: could not save analysis results: {e}")

    output_filename = os.path.join(OUTPUT_DIR, f"jito_subsidy_enriched_{timestamp_str}.csv")
    full_df.to_csv(output_filename, index=False)
    log(f"✅ Full enriched CSV saved: {output_filename}")

    try:
        summary_cols = ['on_chain_tip_lamports','jito_tip_lamports','fee_lamports_final','compute_units_final','tip_over_fee_rt','jito_tip_over_fee_rt','onchain_tip_per_cu','jito_tip_per_cu','fee_per_cu']
        summary = full_df.groupby('success')[summary_cols].agg(['count','mean','median']).reset_index()
        summary_file = os.path.join(OUTPUT_DIR, f"jito_enriched_summary_{timestamp_str}.csv")
        summary.to_csv(summary_file, index=False)
        log(f"✅ Summary CSV saved: {summary_file}")
    except Exception as e:
        log(f"Warning: summary stats failed: {e}")

    # Create organized final CSV with logical column ordering
    log("Creating organized final CSV with all metrics...")
    
    # Define column order for better analysis
    base_cols = ['signature', 'success', 'is_success', 'is_jito_bundle']
    timing_cols = ['slot', 'block_time', 'ts', 'hour', 'weekday', 'latency_seconds_rt', 'latency_bucket']
    tip_cols = ['on_chain_tip_lamports', 'jito_tip_lamports', 'profit_lamports', 'profit_sol', 'subsidy_percentage']
    fee_cols = ['fee', 'fee_lamports_final', 'fee_sol_final', 'fee_lamports_rt']
    cu_cols = ['compute_units', 'compute_units_final', 'compute_units_rt']
    ratio_cols = ['tip_over_fee', 'tip_over_fee_rt', 'jito_tip_over_fee_rt', 'onchain_tip_per_cu', 'jito_tip_per_cu', 'fee_per_cu', 'cu_efficiency', 'tip_intensity']
    density_cols = ['slot_tx_count', 'win5m_tx_count', 'win5m_program_count', 'slot_compute_sum', 'slot_congestion', 'tip_per_congestion']
    validator_cols = ['validator_identity', 'validator_data']
    detail_cols = ['instruction_count', 'program_ids', 'account_count', 'total_positive_balance_changes', 'total_negative_balance_changes']
    rpc_cols = ['slot_rpc', 'block_time_rpc', 'fee_lamports_rpc']
    
    # Combine all columns in logical order, only including those that exist
    ordered_cols = []
    for col_group in [base_cols, timing_cols, tip_cols, fee_cols, cu_cols, ratio_cols, density_cols, validator_cols, detail_cols, rpc_cols]:
        ordered_cols.extend([col for col in col_group if col in full_df.columns])
    
    # Add any remaining columns not in our organized list
    remaining_cols = [col for col in full_df.columns if col not in ordered_cols]
    ordered_cols.extend(remaining_cols)
    
    # Create organized DataFrame
    organized_df = full_df[ordered_cols].copy()
    
    # Handle NA values with appropriate defaults
    organized_df = organized_df.fillna({
        'is_success': False,
        'is_jito_bundle': False,
        'slot_tx_count': 0,
        'win5m_tx_count': 0,
        'win5m_program_count': 0
    })
    
    # Save organized CSV
    organized_filename = os.path.join(OUTPUT_DIR, f"comprehensive_analysis_dataset_{timestamp_str}.csv")
    organized_df.to_csv(organized_filename, index=False)
    log(f"✅ Comprehensive organized dataset saved: {organized_filename}")
    
    # Generate final summary report
    log("Generating final summary report...")
    final_summary = {
        'dataset_info': {
            'total_transactions': len(organized_df),
            'successful_transactions': organized_df['is_success'].sum(),
            'jito_bundle_transactions': organized_df['is_jito_bundle'].sum(),
            'unique_validators': organized_df['validator_identity'].nunique() if 'validator_identity' in organized_df else 0,
            'date_range': {
                'start': organized_df['ts'].min().isoformat() if 'ts' in organized_df and not organized_df['ts'].isna().all() else None,
                'end': organized_df['ts'].max().isoformat() if 'ts' in organized_df and not organized_df['ts'].isna().all() else None
            }
        },
        'key_metrics': {
            'avg_tip_sol': float(organized_df['on_chain_tip_lamports'].mean() / 1e9) if organized_df['on_chain_tip_lamports'].notna().any() else 0,
            'avg_profit_sol': float(organized_df['profit_sol'].mean()) if organized_df['profit_sol'].notna().any() else 0,
            'avg_subsidy_percentage': float(organized_df['subsidy_percentage'].mean()) if organized_df['subsidy_percentage'].notna().any() else 0,
            'success_rate': float(organized_df['is_success'].mean()) if len(organized_df) > 0 else 0,
            'jito_bundle_rate': float(organized_df['is_jito_bundle'].mean()) if len(organized_df) > 0 else 0
        }
    }
    
    summary_report_file = os.path.join(OUTPUT_DIR, f"final_summary_report_{timestamp_str}.json")
    try:
        with open(summary_report_file, 'w') as f:
            json.dump(final_summary, f, indent=2, default=str)
        log(f"✅ Final summary report saved: {summary_report_file}")
    except Exception as e:
        log(f"Warning: could not save final summary: {e}")

        # Console display and graph (unchanged)
        display_df = full_df.copy()
        display_df.fillna({'subsidy_percentage': 'N/A'}, inplace=True)
        display_cols = ['signature', 'success', 'on_chain_tip_lamports', 'jito_tip_lamports', 'subsidy_percentage']
        print(display_df[display_cols].to_string())
        
        non_jito_mask = (full_df['jito_tip_lamports'].isnull()) | (full_df['jito_tip_lamports'] == 0)
        sum_non_jito_lamports = full_df.loc[non_jito_mask, 'on_chain_tip_lamports'].sum()
        log("--- Additional Analysis ---")
        log(f"Total On-Chain Tip for non-Jito txns: {sum_non_jito_lamports:,.0f} lamports ({sum_non_jito_lamports / 1e9:.6f} SOL)")

        subsidy_data = full_df['subsidy_percentage'].replace([float('inf'), -float('inf')], pd.NA).dropna()

        if not subsidy_data.empty:
            log("Generating subsidy distribution histogram...")
            plt.figure(figsize=(12, 7))
            plt.hist(subsidy_data, bins=30, edgecolor='black', alpha=0.7)
            plt.title('Distribution of Jito Subsidy Percentages', fontsize=16)
            plt.xlabel('Subsidy Percentage (%)', fontsize=12)
            plt.ylabel('Frequency (Count of Occurrences)', fontsize=12)
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.axvline(0, color='r', linestyle='dashed', linewidth=2)
            plt.text(0.5, plt.ylim()[1]*0.9, ' On-Chain Tip = Jito Tip', color='r')
            plt.show()
        else:
            log("No Jito transactions with subsidies found to generate a graph.")
        try:
            if 'slot' in full_df.columns:
                slot_agg = full_df.groupby('slot').agg({
                    'signature': 'count',
                    'compute_units': 'sum',
                    'on_chain_tip_lamports': 'sum',
                    'jito_tip_lamports': 'sum'
                }).rename(columns={'signature': 'tx_count'}).reset_index()
                slot_agg['slot_congestion'] = slot_agg['compute_units'] / float(MAX_COMPUTE_PER_BLOCK)
                slot_outfile = os.path.join(OUTPUT_DIR, f"slot_aggregates_{timestamp_str}.csv")
                slot_agg.to_csv(slot_outfile, index=False)
                log(f"✅ Slot aggregates saved: {slot_outfile}")
        except Exception as e:
            log(f"Warning: could not compute/save slot aggregates: {e}")

    log("Pipeline completed successfully!")
    else:
        log("Could not retrieve initial data from Dune.")