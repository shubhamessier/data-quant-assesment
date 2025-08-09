import asyncio
import csv
import json
import time
from typing import Optional, List, Dict, Any
from datetime import datetime
from collections import deque

import httpx
from aiolimiter import AsyncLimiter
import redis.asyncio as redis
from tenacity import (
    retry, retry_if_exception_type, stop_after_attempt,
    wait_random_exponential, RetryError
)

# -------------------------
# CONFIG
# -------------------------
HELIUS_API_KEY = "d26fc6e2-62f2-42ec-9480-d2324d64c530"
HELIUS_BASE_URL = "https://api.helius.xyz/v0"
JITO_MAINNET_URL = "https://mainnet.block-engine.jito.wtf"
JITO_API_VERSION = "/api/v1"
CLIENT_ADDRESS = "51ePnC5NJpv4MXCttjrJLPsBimBpsoH2nHi29U9QvYcT"
COMPETITOR_ADDRESS = "29UqxHhQDiWNfoZBe2PnKCjEYrjFVfU42vs3DK3oiYc9"

# Rate limits
HELIUS_RATE = 5           # requests per interval
HELIUS_INTERVAL = 1       # interval seconds
JITO_RATE = 2            # requests per second
JITO_BURST = 5           # allowed burst

# Batch processing config
BATCH_SIZE = 10          # Number of transactions to process in parallel
BATCH_DELAY = 0.5        # Delay between batches in seconds

# Pagination and limits
TRANSACTIONS_PER_PAGE = 100
MAX_PAGES = 200

# Output files
CSV_FILE = "client_to_competitor_transactions.csv"
JSON_FILE = "client_to_competitor_transactions.json"
JITO_BUNDLES_FILE = "jito_bundles.json"

class SolanaWorker:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=30.0)
        self.helius_limiter = AsyncLimiter(HELIUS_RATE, HELIUS_INTERVAL)
        self.jito_limiter = AsyncLimiter(JITO_RATE, 1)
        self.transactions_data: List[Dict[str, Any]] = []
        self.jito_bundles_data: List[Dict[str, Any]] = []
        self.csv_writer = None
        self.csv_file = None
        self.json_file = None
        self.jito_bundles_file = None
        self.current_page = 0
        self.tip_accounts = []

    async def init_files(self):
        # Initialize CSV file with headers
        self.csv_file = open(CSV_FILE, "w", newline="")
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=[
            "signature", 
            "timestamp", 
            "block", 
            "fee",
            "source",
            "destination", 
            "amount_sol",
            "type",
            "description",
            "is_jito_bundle",
            "source_is_client",
            "destination_is_competitor"
        ])
        self.csv_writer.writeheader()
        
        # Initialize JSON file
        self.json_file = open(JSON_FILE, "w")
        self.json_file.write("[\n")
        
        # Initialize Jito bundles file
        self.jito_bundles_file = open(JITO_BUNDLES_FILE, "w")
        self.jito_bundles_file.write("[\n")
        
        print(f"[init] created {CSV_FILE}, {JSON_FILE}, and {JITO_BUNDLES_FILE} for real-time writing")

    async def close(self):
        await self.http.aclose()
        if self.csv_file:
            self.csv_file.close()
        if self.json_file:
            if self.transactions_data:
                self.json_file.write("\n]")
            else:
                self.json_file.write("]")
            self.json_file.close()
        if self.jito_bundles_file:
            if self.jito_bundles_data:
                self.jito_bundles_file.write("\n]")
            else:
                self.jito_bundles_file.write("]")
            self.jito_bundles_file.close()

    async def write_transaction(self, transaction_data: Dict[str, Any]):
        if self.csv_writer:
            # Create a copy of the data without the new fields that aren't in CSV headers
            csv_data = {k: v for k, v in transaction_data.items() 
                       if k not in ['tip_value_sol', 'bundle_source']}
            self.csv_writer.writerow(csv_data)
            self.csv_file.flush()  # Force write to disk

        if self.json_file:
            # Add comma if not first entry
            if self.transactions_data:
                self.json_file.write(",\n")
            json.dump(transaction_data, self.json_file, indent=2)
            self.json_file.flush()  # Force write to disk

        self.transactions_data.append(transaction_data)
        
        # Print detailed transaction info
        print("\n=== Transaction Details ===")
        print(f"Signature: {transaction_data['signature']}")
        print(f"Timestamp: {transaction_data['timestamp']}")
        print(f"Block: {transaction_data['block']}")
        print(f"Fee: {transaction_data['fee']} lamports")
        print(f"Type: {transaction_data['type']}")
        print(f"Is Jito Bundle: {transaction_data['is_jito_bundle']}")
        if transaction_data['is_jito_bundle']:
            print(f"Bundle Source: {transaction_data['bundle_source']}")
            print(f"Tip Value: {transaction_data['tip_value_sol']} SOL")
        print(f"From: {transaction_data['source']}")
        print(f"To: {transaction_data['destination']}")
        print(f"Amount: {transaction_data['amount_sol']} SOL")
        if transaction_data.get('description'):
            print(f"Description: {transaction_data['description']}")
        print("===========================\n")

    async def get_transaction_history(self, before: Optional[str] = None) -> List[Dict]:
        """Get transaction history using Helius enhanced API"""
        url = f"{HELIUS_BASE_URL}/addresses/{COMPETITOR_ADDRESS}/transactions"
        
        params = {
            "api-key": HELIUS_API_KEY,
            "before": before,
            "limit": TRANSACTIONS_PER_PAGE
        }

        async with self.helius_limiter:
            r = await self.http.get(url, params=params)
            r.raise_for_status()
            return r.json()

    async def process_transaction(self, txn: Dict) -> Dict:
        """Process a transaction and extract relevant information"""
        signature = txn["signature"]
        timestamp = datetime.fromtimestamp(txn["timestamp"]).isoformat()
        block = txn.get("slot")
        fee = txn.get("fee", 0)
        description = txn.get("description", "")
        tx_type = txn.get("type", "unknown")

        # Look for native SOL transfers
        native_transfers = txn.get("nativeTransfers", [])
        if native_transfers:
            for transfer in native_transfers:
                source = transfer.get("fromUserAccount", "unknown")
                destination = transfer.get("toUserAccount", "unknown")
                amount = transfer.get("amount", 0) / 1e9  # Convert lamports to SOL
                
                return {
                    "signature": signature,
                    "timestamp": timestamp,
                    "block": block,
                    "fee": fee,
                    "source": source,
                    "destination": destination,
                    "amount_sol": amount,
                    "type": tx_type,
                    "description": description,
                    "is_jito_bundle": False,  # Will be updated in post-processing
                    "source_is_client": source == CLIENT_ADDRESS,
                    "destination_is_competitor": destination == COMPETITOR_ADDRESS
                }
        
        # Look for token transfers
        token_transfers = txn.get("tokenTransfers", [])
        if token_transfers:
            for transfer in token_transfers:
                source = transfer.get("fromUserAccount", "unknown")
                destination = transfer.get("toUserAccount", "unknown")
                amount = transfer.get("amount", 0) / 1e9  # Convert to SOL equivalent
                
                return {
                    "signature": signature,
                    "timestamp": timestamp,
                    "block": block,
                    "fee": fee,
                    "source": source,
                    "destination": destination,
                    "amount_sol": amount,
                    "type": "token_" + tx_type.lower(),
                    "description": description,
                    "is_jito_bundle": False,  # Will be updated in post-processing
                    "source_is_client": source == CLIENT_ADDRESS,
                    "destination_is_competitor": destination == COMPETITOR_ADDRESS
                }
        
        # Return basic transaction info if no transfers found
        return {
            "signature": signature,
            "timestamp": timestamp,
            "block": block,
            "fee": fee,
            "source": "unknown",
            "destination": "unknown",
            "amount_sol": 0,
            "type": tx_type,
            "description": description,
            "is_jito_bundle": False,  # Will be updated in post-processing
            "source_is_client": False,
            "destination_is_competitor": False
        }

    async def get_jito_tip_accounts(self) -> List[str]:
        """Fetch tip accounts from Jito API"""
        async with self.jito_limiter:
            try:
                url = f"{JITO_MAINNET_URL}{JITO_API_VERSION}/getTipAccounts"
                response = await self.http.post(
                    url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getTipAccounts",
                        "params": []
                    }
                )
                response.raise_for_status()
                data = response.json()
                if "result" in data:
                    return data["result"]
                return []
            except Exception as e:
                print(f"[error] Failed to fetch tip accounts: {e}")
                return []

    async def get_bundle_info(self, signature: str) -> Dict[str, Any]:
        """Get bundle information from Jito API"""
        async with self.jito_limiter:
            try:
                url = f"{JITO_MAINNET_URL}{JITO_API_VERSION}/getBundleStatuses"
                response = await self.http.post(
                    url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getBundleStatuses",
                        "params": [[signature]]
                    }
                )
                response.raise_for_status()
                data = response.json()
                if data.get("result", {}).get("value"):
                    return data["result"]["value"][0]
            except Exception as e:
                print(f"[error] Failed to get bundle status for {signature}: {e}")
            
            # Try inflight status if bundle status fails
            try:
                url = f"{JITO_MAINNET_URL}{JITO_API_VERSION}/getInflightBundleStatuses"
                response = await self.http.post(
                    url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getInflightBundleStatuses",
                        "params": [[signature]]
                    }
                )
                response.raise_for_status()
                data = response.json()
                if data.get("result", {}).get("value"):
                    return data["result"]["value"][0]
            except Exception as e:
                print(f"[error] Failed to get inflight bundle status for {signature}: {e}")
            
            return {}

    async def process_jito_bundles(self):
        """Post-process transactions to identify and analyze Jito bundles"""
        print("\n[jito] Starting Jito bundle processing...")
        
        # Fetch tip accounts first
        self.tip_accounts = await self.get_jito_tip_accounts()
        print(f"[jito] Loaded {len(self.tip_accounts)} tip accounts")
        
        # Process transactions in batches
        total_processed = 0
        jito_bundle_count = 0
        current_batch = []
        
        for txn in self.transactions_data:
            current_batch.append(txn)
            
            if len(current_batch) >= BATCH_SIZE:
                processed = await self._process_jito_batch(current_batch)
                total_processed += len(processed)
                jito_bundle_count += sum(1 for t in processed if t["is_jito_bundle"])
                current_batch = []
                print(f"[jito] Processed {total_processed}/{len(self.transactions_data)} transactions, found {jito_bundle_count} bundles")
        
        # Process remaining transactions
        if current_batch:
            processed = await self._process_jito_batch(current_batch)
            total_processed += len(processed)
            jito_bundle_count += sum(1 for t in processed if t["is_jito_bundle"])
        
        print(f"[jito] Completed processing {total_processed} transactions, found {jito_bundle_count} Jito bundles")
        return jito_bundle_count

    async def _process_jito_batch(self, transactions: List[Dict]) -> List[Dict]:
        """Process a batch of transactions for Jito bundle information"""
        tasks = []
        for txn in transactions:
            tasks.append(self._process_single_jito(txn))
        
        processed = await asyncio.gather(*tasks)
        await asyncio.sleep(BATCH_DELAY)  # Add delay between batches
        return processed

    async def _process_single_jito(self, txn: Dict) -> Dict:
        """Process a single transaction for Jito bundle information"""
        bundle_info = await self.get_bundle_info(txn["signature"])
        
        # Update transaction with bundle information
        txn["is_jito_bundle"] = bool(bundle_info)
        if bundle_info:
            txn["bundle_source"] = "jito_api"
            txn["bundle_info"] = bundle_info
            if txn not in self.jito_bundles_data:
                self.jito_bundles_data.append(txn)
        
        return txn

    async def run(self):
        await self.init_files()
        print(f"[start] Fetching all transactions for address: {COMPETITOR_ADDRESS}")
        print(f"[config] Processing {TRANSACTIONS_PER_PAGE} transactions per page, max {MAX_PAGES} pages")

        before = None
        while True:
            self.current_page += 1
            print(f"\n[page {self.current_page}/{MAX_PAGES}] fetching transactions before {before or 'latest'}...")
            
            if self.current_page > MAX_PAGES:
                print(f"[done] reached max pages limit ({MAX_PAGES})")
                break

            try:
                transactions = await self.get_transaction_history(before)
                if not transactions:
                    print("[done] no more transactions")
                    break

                print(f"[page] processing {len(transactions)} transactions...")
                
                for txn in transactions:
                    try:
                        transaction_data = await self.process_transaction(txn)
                        if transaction_data["amount_sol"] > 0:
                            await self.write_transaction(transaction_data)
                    except Exception as e:
                        print(f"[err] failed to process transaction {txn.get('signature')}: {e}")
                        continue

                if transactions:
                    before = transactions[-1]["signature"]
                    await asyncio.sleep(1)  # Small pause between pages

            except Exception as e:
                print(f"[err] failed to fetch transaction page: {e}. Sleeping and retrying...")
                await asyncio.sleep(2)
                continue

        # Process Jito bundles after collecting all transactions
        jito_bundle_count = await self.process_jito_bundles()

        # Print summary
        print(f"\n[summary] processed {len(self.transactions_data)} transactions across {self.current_page} pages")
        if len(self.transactions_data) == 0:
            print("[warning] no transactions were found - files may be empty")
        else:
            print(f"[success] data written to {CSV_FILE}, {JSON_FILE}, and {JITO_BUNDLES_FILE}")
            print("\nTransaction Type Summary:")
            type_counts = {}
            amount_by_type = {}
            for txn in self.transactions_data:
                tx_type = txn["type"]
                amount = txn["amount_sol"]
                type_counts[tx_type] = type_counts.get(tx_type, 0) + 1
                amount_by_type[tx_type] = amount_by_type.get(tx_type, 0) + amount
            
            for type_name, count in sorted(type_counts.items()):
                total_amount = amount_by_type[type_name]
                print(f"- {type_name}: {count} transactions, total {total_amount:.2f} SOL")

            print(f"\nJito Bundle Summary:")
            print(f"Total Bundles Found: {jito_bundle_count}")
            if self.jito_bundles_data:
                total_value = sum(bundle["amount_sol"] for bundle in self.jito_bundles_data)
                avg_value = total_value / len(self.jito_bundles_data)
                print(f"Total Bundle Value: {total_value:.4f} SOL")
                print(f"Average Value per Bundle: {avg_value:.4f} SOL")
                
                # Print bundle source distribution
                source_counts = {}
                for bundle in self.jito_bundles_data:
                    source = bundle.get('bundle_source', 'unknown')
                    source_counts[source] = source_counts.get(source, 0) + 1
                print("\nBundle Source Distribution:")
                for source, count in sorted(source_counts.items()):
                    print(f"- {source}: {count} bundles")

# -------------------------
# Entrypoint
# -------------------------
async def main():
    worker = SolanaWorker()
    try:
        await worker.run()
    finally:
        await worker.close()

if __name__ == "__main__":
    asyncio.run(main())
