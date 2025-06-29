#!/bin/bash

# Demo data script for Venice facet counting demo
# This script populates the store with representative data for aggregation testing

STORE_NAME="zpoliczer-test-hybrid"
FABRIC="ei-ltx1"

echo "=== Venice Facet Counting Demo - Data Population ==="
echo "Store: $STORE_NAME"
echo "Fabric: $FABRIC"
echo

echo "Populating store with demo data..."
echo

echo "Writing emp1..."
echo "  Data: {\"firstName\": \"John\", \"lastName\": \"Smith\", \"age\": 25}"
./produce.sh $FABRIC $STORE_NAME "emp1" '{"firstName": "John", "lastName": "Smith", "age": 25}' > /dev/null 2>&1

echo "Writing emp2..."
echo "  Data: {\"firstName\": \"Jane\", \"lastName\": \"Johnson\", \"age\": 30}"
./produce.sh $FABRIC $STORE_NAME "emp2" '{"firstName": "Jane", "lastName": "Johnson", "age": 30}' > /dev/null 2>&1

echo "Writing emp3..."
echo "  Data: {\"firstName\": \"Bob\", \"lastName\": \"Brown\", \"age\": 35}"
./produce.sh $FABRIC $STORE_NAME "emp3" '{"firstName": "Bob", "lastName": "Brown", "age": 35}' > /dev/null 2>&1

echo "Writing emp4..."
echo "  Data: {\"firstName\": \"Alice\", \"lastName\": \"Doe\", \"age\": 28}"
./produce.sh $FABRIC $STORE_NAME "emp4" '{"firstName": "Alice", "lastName": "Doe", "age": 28}' > /dev/null 2>&1

echo "Writing emp5..."
echo "  Data: {\"firstName\": \"John\", \"lastName\": \"Wilson\", \"age\": 32}"
./produce.sh $FABRIC $STORE_NAME "emp5" '{"firstName": "John", "lastName": "Wilson", "age": 32}' > /dev/null 2>&1

echo "Writing emp6..."
echo "  Data: {\"firstName\": \"Jane\", \"lastName\": \"Smith\", \"age\": 27}"
./produce.sh $FABRIC $STORE_NAME "emp6" '{"firstName": "Jane", "lastName": "Smith", "age": 27}' > /dev/null 2>&1

echo "Writing emp7..."
echo "  Data: {\"firstName\": \"Bob\", \"lastName\": \"Johnson\", \"age\": 38}"
./produce.sh $FABRIC $STORE_NAME "emp7" '{"firstName": "Bob", "lastName": "Johnson", "age": 38}' > /dev/null 2>&1

echo "Writing emp8..."
echo "  Data: {\"firstName\": \"Sarah\", \"lastName\": \"Brown\", \"age\": 42}"
./produce.sh $FABRIC $STORE_NAME "emp8" '{"firstName": "Sarah", "lastName": "Brown", "age": 42}' > /dev/null 2>&1

echo "Writing emp9..."
echo "  Data: {\"firstName\": \"John\", \"lastName\": \"Davis\", \"age\": 29}"
./produce.sh $FABRIC $STORE_NAME "emp9" '{"firstName": "John", "lastName": "Davis", "age": 29}' > /dev/null 2>&1

echo "Writing emp10..."
echo "  Data: {\"firstName\": \"Mike\", \"lastName\": \"Smith\", \"age\": 33}"
./produce.sh $FABRIC $STORE_NAME "emp10" '{"firstName": "Mike", "lastName": "Smith", "age": 33}' > /dev/null 2>&1

echo
echo "✅ Demo data population completed!"
echo "✅ 10 employees with representative data for aggregation testing"
echo "✅ Ready to run demo_script.sh for facet counting demonstration" 