#!/bin/bash

# Test script for debugging empty columns in pipeline.py
# Run this to identify which API calls are failing

API_KEY="1ac8e73b-5da0-454a-a6df-c97e500fdf9d"
HELIUS_RPC="https://mainnet.helius-rpc.com/?api-key=${API_KEY}"
HELIUS_ENHANCED="https://api.helius.xyz/v0/transactions?api-key=${API_KEY}"
HELIUS_SINGLE="https://api.helius.xyz/v0/transactions"

# Test signature - replace with actual signature from your data
TEST_SIG="your_test_signature_here"

echo "=== Testing Helius API Endpoints ==="
echo

echo "1. Testing Helius Enhanced API (batch)..."
curl -X POST "$HELIUS_ENHANCED" \
  -H "Content-Type: application/json" \
  -d "{\"transactions\": [\"$TEST_SIG\"]}" \
  -w "\nStatus: %{http_code}\n" \
  | jq '.[0] | {signature, slot, blockTime, meta: {fee, computeUnitsConsumed}}' 2>/dev/null || echo "Failed to parse JSON"

echo -e "\n" && read -p "Press Enter to continue..."

echo "2. Testing Helius Single Transaction API..."
curl -X GET "${HELIUS_SINGLE}/${TEST_SIG}?api-key=${API_KEY}" \
  -H "Content-Type: application/json" \
  -w "\nStatus: %{http_code}\n" \
  | jq '{signature, slot, blockTime, meta: {fee, computeUnitsConsumed}}' 2>/dev/null || echo "Failed to parse JSON"

echo -e "\n" && read -p "Press Enter to continue..."

echo "3. Testing Helius RPC getTransaction..."
curl -X POST "$HELIUS_RPC" \
  -H "Content-Type: application/json" \
  -d "{
    \"jsonrpc\": \"2.0\",
    \"id\": 1,
    \"method\": \"getTransaction\",
    \"params\": [
      \"$TEST_SIG\",
      {\"encoding\": \"jsonParsed\", \"maxSupportedTransactionVersion\": 0, \"commitment\": \"finalized\"}
    ]
  }" \
  -w "\nStatus: %{http_code}\n" \
  | jq '{result: {slot, blockTime, meta: {fee}}}' 2>/dev/null || echo "Failed to parse JSON"

echo -e "\n" && read -p "Press Enter to continue..."

echo "4. Testing Current Slot (should work)..."
curl -X POST "$HELIUS_RPC" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getSlot"
  }' \
  -w "\nStatus: %{http_code}\n"

echo -e "\n" && read -p "Press Enter to continue..."

echo "5. Testing Slot Leaders (with current slot)..."
CURRENT_SLOT=$(curl -s -X POST "$HELIUS_RPC" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc": "2.0", "id": 1, "method": "getSlot"}' \
  | jq -r '.result')

TEST_SLOT=$((CURRENT_SLOT - 1000))

echo "Testing slot leaders for slot: $TEST_SLOT"
curl -X POST "$HELIUS_RPC" \
  -H "Content-Type: application/json" \
  -d "{
    \"jsonrpc\": \"2.0\",
    \"id\": 1,
    \"method\": \"getSlotLeaders\",
    \"params\": [$TEST_SLOT, 5]
  }" \
  -w "\nStatus: %{http_code}\n"

echo -e "\n=== Test completed ==="
echo
echo "To use this script:"
echo "1. Replace 'your_test_signature_here' with an actual signature from your data"
echo "2. Run: chmod +x test_apis.sh && ./test_apis.sh"
echo
echo "To get a test signature from your data, run:"
echo "head -2 /home/shubham/Downloads/subsidy-working/output/jito_subsidy_enriched_*.csv | tail -1 | cut -d',' -f1" 