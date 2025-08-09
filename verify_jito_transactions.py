import asyncio
import json
import csv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import httpx
from aiolimiter import AsyncLimiter
from asyncio import Queue, Task
import copy
import os

# Jito API Configuration
JITO_MAINNET_URL = "https://mainnet.block-engine.jito.wtf"
JITO_API_VERSION = "/api/v1"

# Rate Limiting Configuration
JITO_RATE = 1  # requests per second
JITO_BURST = 5  # allowed burst
BATCH_SIZE = 5  # transactions to process in parallel
BATCH_DELAY = 1.5  # seconds between batches

# Worker Configuration
NUM_WORKERS = 10  # Number of parallel workers

# Input/Output Files
TRANSACTIONS_CSV = "client_to_competitor_transactions.csv"
TRANSACTIONS_JSON = "client_to_competitor_transactions.json"
VERIFICATION_REPORT = "jito_verification_report.json"
VERIFICATION_SUMMARY = "jito_verification_summary.txt"

class JitoVerifier:
    def __init__(self):
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.rate_limiter = AsyncLimiter(JITO_RATE, 1)
        self.transactions: List[Dict[str, Any]] = []
        self.verification_results: List[Dict[str, Any]] = []
        self.start_time: Optional[datetime] = None
        self.tip_accounts: List[str] = []
        self.work_queue: Queue = Queue()
        self.result_queue: Queue = Queue()
        self.workers: List[Task] = []
        self.processed_count = 0
        self.total_count = 0
        self.stats = {
            "total_transactions": 0,
            "total_api_calls": 0,
            "bundle_status_calls": 0,
            "inflight_status_calls": 0,
            "confirmed_bundles": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None
        }

    async def close(self):
        await self.http_client.aclose()

    def load_transactions(self):
        """Load transactions from JSON file"""
        try:
            with open(TRANSACTIONS_JSON, 'r') as f:
                self.transactions = json.load(f)
            self.total_count = len(self.transactions)
            print(f"[info] Loaded {self.total_count} transactions from {TRANSACTIONS_JSON}")
        except Exception as e:
            print(f"[error] Failed to load transactions: {e}")
            self.transactions = []

    async def worker(self, worker_id: int):
        """Worker process to verify transactions"""
        print(f"[worker {worker_id}] Started")
        
        while True:
            try:
                # Get next batch of work
                batch = await self.work_queue.get()
                if batch is None:  # Poison pill
                    print(f"[worker {worker_id}] Shutting down")
                    break

                results = await self.process_batch(batch)
                await self.result_queue.put(results)
                
                self.processed_count += len(batch)
                confirmed = sum(1 for r in results if r["is_bundle"])
                
                # Write progress report in real time
                self.write_progress_report()
                
                print(f"[worker {worker_id}] Processed {len(batch)} transactions, "
                      f"found {confirmed} bundles. Total progress: "
                      f"{self.processed_count}/{self.total_count}")

            except Exception as e:
                print(f"[worker {worker_id}] Error processing batch: {e}")
            finally:
                self.work_queue.task_done()

    def write_progress_report(self):
        """Write progress report in real time"""
        try:
            report_data = {
                "stats": {
                    **self.stats,
                    "current_progress": {
                        "processed": self.processed_count,
                        "total": self.total_count,
                        "percentage": round((self.processed_count / self.total_count) * 100, 2) if self.total_count > 0 else 0,
                        "last_updated": datetime.now().isoformat()
                    }
                },
                "results": self.verification_results
            }
            
            # Write to a temporary file first
            temp_file = f"{VERIFICATION_REPORT}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            # Atomically replace the old file
            os.replace(temp_file, VERIFICATION_REPORT)
            
        except Exception as e:
            print(f"[error] Failed to write progress report: {e}")

    async def verify_bundle(self, signature: str) -> Dict[str, Any]:
        """Verify a transaction signature with Jito API"""
        result = {
            "signature": signature,
            "timestamp": datetime.now().isoformat(),
            "bundle_status": None,
            "inflight_status": None,
            "is_bundle": False,
            "verification_source": None,
            "error": None
        }

        try:
            # Check bundle status
            async with self.rate_limiter:
                self.stats["bundle_status_calls"] += 1
                self.stats["total_api_calls"] += 1
                response = await self.http_client.post(
                    f"{JITO_MAINNET_URL}{JITO_API_VERSION}/getBundleStatuses",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getBundleStatuses",
                        "params": [[signature]]
                    }
                )
                response.raise_for_status()
                bundle_data = response.json()
                if bundle_data.get("result", {}).get("value"):
                    result["bundle_status"] = bundle_data["result"]["value"][0]
                    result["is_bundle"] = True
                    result["verification_source"] = "bundle_status"
                    self.stats["confirmed_bundles"] += 1
                    return result

            # Check inflight status if bundle status not found
            async with self.rate_limiter:
                self.stats["inflight_status_calls"] += 1
                self.stats["total_api_calls"] += 1
                response = await self.http_client.post(
                    f"{JITO_MAINNET_URL}{JITO_API_VERSION}/getInflightBundleStatuses",
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getInflightBundleStatuses",
                        "params": [[signature]]
                    }
                )
                response.raise_for_status()
                inflight_data = response.json()
                if inflight_data.get("result", {}).get("value"):
                    result["inflight_status"] = inflight_data["result"]["value"][0]
                    result["is_bundle"] = True
                    result["verification_source"] = "inflight_status"
                    self.stats["confirmed_bundles"] += 1

        except Exception as e:
            result["error"] = str(e)
            self.stats["errors"] += 1
            print(f"[error] Failed to verify bundle {signature}: {e}")

        return result

    async def process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process a batch of transactions"""
        tasks = []
        for txn in batch:
            tasks.append(self.verify_bundle(txn["signature"]))
        
        results = await asyncio.gather(*tasks)
        
        # Update verification results immediately
        self.verification_results.extend(results)
        
        await asyncio.sleep(BATCH_DELAY)  # Add delay between batches
        return results

    def update_original_files(self):
        """Update original transaction files with verification results"""
        # Update JSON file
        updated_transactions = copy.deepcopy(self.transactions)
        for txn in updated_transactions:
            verify_result = next(
                (r for r in self.verification_results if r["signature"] == txn["signature"]),
                None
            )
            if verify_result:
                txn["is_jito_bundle"] = verify_result["is_bundle"]
                txn["jito_verification"] = {
                    "timestamp": verify_result["timestamp"],
                    "source": verify_result["verification_source"],
                    "bundle_status": verify_result["bundle_status"],
                    "inflight_status": verify_result["inflight_status"]
                }

        with open(TRANSACTIONS_JSON, 'w') as f:
            json.dump(updated_transactions, f, indent=2)

        # Update CSV file
        with open(TRANSACTIONS_CSV, 'r', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames

        for row in rows:
            verify_result = next(
                (r for r in self.verification_results if r["signature"] == row["signature"]),
                None
            )
            if verify_result:
                row["is_jito_bundle"] = str(verify_result["is_bundle"]).lower()

        with open(TRANSACTIONS_CSV, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f"[success] Updated original files with verification results")

    def save_verification_results(self):
        """Save verification results to files"""
        # Save detailed report
        with open(VERIFICATION_REPORT, 'w') as f:
            report_data = {
                "stats": self.stats,
                "results": self.verification_results
            }
            json.dump(report_data, f, indent=2)

        # Calculate statistics
        total_transactions = len(self.verification_results)
        confirmed_bundles = sum(1 for r in self.verification_results if r["is_bundle"])
        error_count = sum(1 for r in self.verification_results if r["error"])
        
        # Count verification sources
        source_counts = {}
        for result in self.verification_results:
            source = result.get("verification_source") or "unknown"
            source_counts[source] = source_counts.get(source, 0) + 1

        # Compare with original classifications
        mismatches = []
        for orig_txn in self.transactions:
            sig = orig_txn["signature"]
            verify_result = next((r for r in self.verification_results if r["signature"] == sig), None)
            if verify_result:
                if orig_txn.get("is_jito_bundle") != verify_result["is_bundle"]:
                    mismatches.append({
                        "signature": sig,
                        "original": orig_txn.get("is_jito_bundle"),
                        "verified": verify_result["is_bundle"]
                    })

        # Save summary report
        with open(VERIFICATION_SUMMARY, 'w') as f:
            duration = datetime.now() - self.start_time if self.start_time else timedelta()
            f.write("Jito Transaction Verification Summary\n")
            f.write("===================================\n\n")
            f.write(f"Verification completed at: {datetime.now().isoformat()}\n")
            f.write(f"Total duration: {duration}\n\n")
            
            f.write("Processing Statistics:\n")
            f.write(f"Total transactions processed: {self.stats['total_transactions']}\n")
            f.write(f"Total API calls made: {self.stats['total_api_calls']}\n")
            f.write(f"Bundle status calls: {self.stats['bundle_status_calls']}\n")
            f.write(f"Inflight status calls: {self.stats['inflight_status_calls']}\n")
            f.write(f"Confirmed Jito bundles: {self.stats['confirmed_bundles']}\n")
            f.write(f"Error count: {self.stats['errors']}\n")
            
            avg_calls_per_tx = self.stats['total_api_calls'] / self.stats['total_transactions'] if self.stats['total_transactions'] > 0 else 0
            f.write(f"Average API calls per transaction: {avg_calls_per_tx:.2f}\n\n")
            
            f.write("Verification Sources:\n")
            for source, count in sorted(source_counts.items()):
                f.write(f"- {source}: {count} transactions\n")
            
            f.write("\nClassification Mismatches:\n")
            f.write(f"Total mismatches: {len(mismatches)}\n")
            for mismatch in mismatches:
                f.write(f"- {mismatch['signature']}: Original={mismatch['original']}, "
                       f"Verified={mismatch['verified']}\n")

        print(f"\n[success] Saved verification results to {VERIFICATION_REPORT}")
        print(f"[success] Saved verification summary to {VERIFICATION_SUMMARY}")
        print("\nProcessing Statistics:")
        print(f"Total transactions: {self.stats['total_transactions']}")
        print(f"Total API calls: {self.stats['total_api_calls']}")
        print(f"Confirmed bundles: {self.stats['confirmed_bundles']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Average API calls per tx: {avg_calls_per_tx:.2f}")

    async def run(self):
        """Run the verification process"""
        self.start_time = datetime.now()
        self.stats["start_time"] = self.start_time.isoformat()
        print("[start] Starting Jito transaction verification...")
        print(f"[info] Progress will be written to {VERIFICATION_REPORT} in real time")

        # Initialize progress report file
        self.write_progress_report()

        # Load transactions
        self.load_transactions()
        if not self.transactions:
            print("[error] No transactions to verify")
            return

        self.stats["total_transactions"] = len(self.transactions)
        self.write_progress_report()  # Write initial state

        # Start workers
        print(f"[info] Starting {NUM_WORKERS} workers")
        for i in range(NUM_WORKERS):
            worker = asyncio.create_task(self.worker(i))
            self.workers.append(worker)

        # Distribute work
        current_batch = []
        for txn in self.transactions:
            current_batch.append(txn)
            if len(current_batch) >= BATCH_SIZE:
                await self.work_queue.put(current_batch)
                current_batch = []

        if current_batch:
            await self.work_queue.put(current_batch)

        # Send poison pills to workers
        for _ in range(NUM_WORKERS):
            await self.work_queue.put(None)

        # Wait for all work to be processed
        await self.work_queue.join()

        # Collect results
        while not self.result_queue.empty():
            results = await self.result_queue.get()

        # Wait for workers to finish
        await asyncio.gather(*self.workers)

        # Record end time
        self.stats["end_time"] = datetime.now().isoformat()

        # Update original files
        self.update_original_files()

        # Save final results
        self.save_verification_results()

        print("\nTo monitor progress in real-time, use:")
        print(f"watch -n 1 'cat {VERIFICATION_REPORT} | jq .stats.current_progress'")

async def main():
    verifier = JitoVerifier()
    try:
        await verifier.run()
    finally:
        await verifier.close()

if __name__ == "__main__":
    asyncio.run(main()) 