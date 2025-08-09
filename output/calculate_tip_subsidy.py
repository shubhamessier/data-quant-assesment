import asyncio
import json
import csv
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

import httpx
from aiolimiter import AsyncLimiter

# Files (absolute paths)
BASE_DIR = "/home/shubham/Downloads/subsidy-working"
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
TRANSACTIONS_JSON = os.path.join(OUTPUT_DIR, "client_to_competitor_transactions.json")
TIP_SUBSIDY_JSON = os.path.join(OUTPUT_DIR, "tip_subsidy_report.json")
TIP_SUBSIDY_CSV = os.path.join(OUTPUT_DIR, "tip_subsidy_report.csv")
TIP_ACCOUNTS_CACHE = os.path.join(OUTPUT_DIR, "jito_tip_accounts.json")

# Addresses (keep in sync with main script)
CLIENT_ADDRESS = "51ePnC5NJpv4MXCttjrJLPsBimBpsoH2nHi29U9QvYcT"
COMPETITOR_ADDRESS = "29UqxHhQDiWNfoZBe2PnKCjEYrjFVfU42vs3DK3oiYc9"

# Jito API
JITO_MAINNET_URL = "https://mainnet.block-engine.jito.wtf"
JITO_API_VERSION = "/api/v1"
JITO_RATE = 1  # requests per second


class TipSubsidyCalculator:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=30.0)
        self.jito_limiter = AsyncLimiter(JITO_RATE, 1)
        self.tip_accounts: List[str] = []
        self.transactions: List[Dict[str, Any]] = []

    async def close(self) -> None:
        await self.http.aclose()

    def load_transactions(self) -> None:
        with open(TRANSACTIONS_JSON, "r") as f:
            self.transactions = json.load(f)
        print(f"[load] {len(self.transactions)} transactions loaded from {TRANSACTIONS_JSON}")

    async def fetch_tip_accounts(self) -> List[str]:
        # Try cache first
        if os.path.exists(TIP_ACCOUNTS_CACHE):
            try:
                with open(TIP_ACCOUNTS_CACHE, "r") as f:
                    data = json.load(f)
                    accounts = data.get("tip_accounts")
                    if isinstance(accounts, list) and accounts:
                        print(f"[tips] loaded {len(accounts)} cached tip accounts")
                        return accounts
            except Exception:
                pass
        # Fallback to API
        try:
            async with self.jito_limiter:
                r = await self.http.post(
                    f"{JITO_MAINNET_URL}{JITO_API_VERSION}/getTipAccounts",
                    json={"jsonrpc": "2.0", "id": 1, "method": "getTipAccounts", "params": []},
                )
                r.raise_for_status()
                data = r.json()
                accounts = data.get("result", [])
                if accounts:
                    print(f"[tips] fetched {len(accounts)} tip accounts from Jito API")
                    try:
                        with open(TIP_ACCOUNTS_CACHE, "w") as f:
                            json.dump({
                                "tip_accounts": accounts,
                                "last_updated": datetime.now().isoformat(),
                            }, f, indent=2)
                    except Exception:
                        pass
                    return accounts
        except Exception as e:
            print(f"[warn] failed to fetch tip accounts from API: {e}")
        # Hardcoded minimal fallback (may be outdated)
        fallback = [
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        ]
        print(f"[tips] using fallback tip accounts ({len(fallback)})")
        return fallback

    @staticmethod
    def extract_bundle_id(txn: Dict[str, Any]) -> Optional[str]:
        """Extract bundle_id from embedded jito_verification object if present."""
        jv = txn.get("jito_verification") or {}
        bs = jv.get("bundle_status") or {}
        if isinstance(bs, dict) and bs.get("bundle_id"):
            return bs.get("bundle_id")
        is_ = jv.get("inflight_status") or {}
        if isinstance(is_, dict) and is_.get("bundle_id"):
            return is_.get("bundle_id")
        # Some APIs might nest differently; attempt alternate keys
        if jv.get("bundle_id"):
            return jv["bundle_id"]
        return None

    def compute_per_bundle(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        bundles: Dict[str, List[Dict[str, Any]]] = {}
        for txn in self.transactions:
            bundle_id = self.extract_bundle_id(txn)
            if not bundle_id:
                # skip non-bundled txns
                continue
            bundles.setdefault(bundle_id, []).append(txn)

        print(f"[group] {len(bundles)} bundles with transactions found")

        per_bundle: List[Dict[str, Any]] = []
        totals = {
            "total_bundles": 0,
            "total_competitor_tip_sol": 0.0,
            "total_jito_tip_sol": 0.0,
            "total_subsidy_sol": 0.0,
        }

        for bundle_id, txns in bundles.items():
            competitor_tip = 0.0
            jito_tip = 0.0
            competitor_sigs: List[str] = []
            jito_sigs: List[str] = []

            for t in txns:
                # We only consider native SOL transfers for tips
                t_type = str(t.get("type", "")).lower()
                if t_type.startswith("token_"):
                    continue
                amount = float(t.get("amount_sol", 0.0) or 0.0)
                destination = t.get("destination")
                source = t.get("source")

                # Tip to competitor: client -> competitor (best-effort heuristic)
                if destination == COMPETITOR_ADDRESS and source == CLIENT_ADDRESS and amount > 0:
                    competitor_tip += amount
                    competitor_sigs.append(t.get("signature", ""))

                # Tip to Jito: destination is one of the tip accounts
                if destination in self.tip_accounts and amount > 0:
                    jito_tip += amount
                    jito_sigs.append(t.get("signature", ""))

            subsidy = competitor_tip - jito_tip
            per_bundle.append({
                "bundle_id": bundle_id,
                "num_transactions": len(txns),
                "competitor_tip_sol": round(competitor_tip, 9),
                "jito_tip_sol": round(jito_tip, 9),
                "subsidy_sol": round(subsidy, 9),
                "competitor_tip_signatures": competitor_sigs,
                "jito_tip_signatures": jito_sigs,
            })

            totals["total_bundles"] += 1
            totals["total_competitor_tip_sol"] += competitor_tip
            totals["total_jito_tip_sol"] += jito_tip
            totals["total_subsidy_sol"] += subsidy

        # Round totals for presentation
        for k in list(totals.keys()):
            if k.startswith("total_") and k.endswith("_sol"):
                totals[k] = round(totals[k], 9)

        return per_bundle, totals

    def write_reports(self, per_bundle: List[Dict[str, Any]], totals: Dict[str, Any]) -> None:
        report = {
            "generated_at": datetime.now().isoformat(),
            "source_file": TRANSACTIONS_JSON,
            "client_address": CLIENT_ADDRESS,
            "competitor_address": COMPETITOR_ADDRESS,
            "stats": totals,
            "bundles": per_bundle,
        }
        with open(TIP_SUBSIDY_JSON, "w") as f:
            json.dump(report, f, indent=2)
        print(f"[write] JSON report written to {TIP_SUBSIDY_JSON}")

        # CSV (one row per bundle)
        with open(TIP_SUBSIDY_CSV, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "bundle_id",
                "num_transactions",
                "competitor_tip_sol",
                "jito_tip_sol",
                "subsidy_sol",
            ])
            for row in per_bundle:
                writer.writerow([
                    row["bundle_id"],
                    row["num_transactions"],
                    row["competitor_tip_sol"],
                    row["jito_tip_sol"],
                    row["subsidy_sol"],
                ])
        print(f"[write] CSV report written to {TIP_SUBSIDY_CSV}")


async def main() -> None:
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    calc = TipSubsidyCalculator()
    try:
        calc.load_transactions()
        calc.tip_accounts = await calc.fetch_tip_accounts()
        per_bundle, totals = calc.compute_per_bundle()
        calc.write_reports(per_bundle, totals)
    finally:
        await calc.close()


if __name__ == "__main__":
    asyncio.run(main()) 