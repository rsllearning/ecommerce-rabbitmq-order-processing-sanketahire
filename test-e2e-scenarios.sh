#!/usr/bin/env bash
# E-Commerce RabbitMQ End-to-End Verification Script
set -e

echo "=========================================================================="
echo "  E-Commerce Order Processing - RabbitMQ E2E Verification Suite"
echo "=========================================================================="
echo ""

# 1. Electronics Order (Dual Routing Test)
echo "--------------------------------------------------------------------------"
echo ">> [SCENARIO 1] Publishing Electronics Order (Category: electronics, Qty: 2)"
echo "   Expected: Both Inventory Service and Analytics Service receive & process"
echo "--------------------------------------------------------------------------"
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"category":"electronics","quantity":2}' | python3 -m json.tool
echo ""
sleep 1

# 2. Clothing Order (Selective Routing Test)
echo "--------------------------------------------------------------------------"
echo ">> [SCENARIO 2] Publishing Clothing Order (Category: clothing, Qty: 5)"
echo "   Expected: Only Inventory Service receives; Analytics Service ignores"
echo "--------------------------------------------------------------------------"
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"category":"clothing","quantity":5}' | python3 -m json.tool
echo ""
sleep 1

# 3. Invalid Quantity (DLX Rejection Test)
echo "--------------------------------------------------------------------------"
echo ">> [SCENARIO 3] Publishing Invalid Order (Category: electronics, Qty: -3)"
echo "   Expected: Inventory rejects without requeue -> Broker routes to orders_dlx -> DLX Logger processes"
echo "--------------------------------------------------------------------------"
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"category":"electronics","quantity":-3}' | python3 -m json.tool
echo ""
sleep 1

# 4. Idempotency Test (Duplicate Order ID)
echo "--------------------------------------------------------------------------"
echo ">> [SCENARIO 4] Publishing Duplicate Order ID (Idempotency Deduplication)"
echo "   Expected: 1st delivery processes; 2nd delivery is skipped by in-memory deduplication set"
echo "--------------------------------------------------------------------------"
FIXED_ID="demo-idempotency-$(date +%s)"
echo "-> First Delivery:"
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-Order-Id: ${FIXED_ID}" \
  -d '{"category":"electronics","quantity":1}' | python3 -m json.tool
sleep 1

echo "-> Second Delivery (Same ID: ${FIXED_ID}):"
curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "X-Order-Id: ${FIXED_ID}" \
  -d '{"category":"electronics","quantity":1}' | python3 -m json.tool
echo ""

echo "=========================================================================="
echo "  Verification complete! Check backend terminal logs for confirmation."
echo "=========================================================================="
