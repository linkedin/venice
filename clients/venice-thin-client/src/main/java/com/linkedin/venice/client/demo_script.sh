#!/bin/bash

# Venice Facet Counting Demo Script
# This script demonstrates the countByValue and countByBucket functionality

STORE_NAME="zpoliczer-test-hybrid"
FABRIC="ei-ltx1"
KEYS="emp1,emp2,emp3,emp4,emp5,emp6,emp7,emp8,emp9,emp10"

echo "=== Venice Facet Counting Demo ==="
echo "Store: $STORE_NAME"
echo "Fabric: $FABRIC"
echo "Keys: $KEYS"
echo

echo "=== Step 1: Show Current Data ==="
echo "Let's first look at some sample data:"
echo

echo "emp1:"
./query.sh $FABRIC $STORE_NAME "emp1" false | grep "^value="
echo

echo "emp2:"
./query.sh $FABRIC $STORE_NAME "emp2" false | grep "^value="
echo

echo "emp3:"
./query.sh $FABRIC $STORE_NAME "emp3" false | grep "^value="
echo

echo "emp4:"
./query.sh $FABRIC $STORE_NAME "emp4" false | grep "^value="
echo

echo "emp5:"
./query.sh $FABRIC $STORE_NAME "emp5" false | grep "^value="
echo

echo "emp6:"
./query.sh $FABRIC $STORE_NAME "emp6" false | grep "^value="
echo

echo "emp7:"
./query.sh $FABRIC $STORE_NAME "emp7" false | grep "^value="
echo

echo "emp8:"
./query.sh $FABRIC $STORE_NAME "emp8" false | grep "^value="
echo

echo "emp9:"
./query.sh $FABRIC $STORE_NAME "emp9" false | grep "^value="
echo

echo "emp10:"
./query.sh $FABRIC $STORE_NAME "emp10" false | grep "^value="
echo

echo "=== Step 2: countByValue Demo ==="
echo "countByValue counts distinct values of specified fields"
echo "It includes a TopK feature to limit results to the most common values"
echo

echo "2.1. Count all first names:"
./query.sh $FABRIC $STORE_NAME "$KEYS" false countByValue firstName 10 | grep "firstName-counts="
echo

echo "2.2. Count top 3 most common first names (TopK feature):"
./query.sh $FABRIC $STORE_NAME "$KEYS" false countByValue firstName 3 | grep "firstName-counts="
echo

echo "2.3. Count top 2 most common first names (TopK feature):"
./query.sh $FABRIC $STORE_NAME "$KEYS" false countByValue firstName 2 | grep "firstName-counts="
echo

echo "=== Step 3: countByValue with Multiple Fields ==="
echo "countByValue can count multiple fields in one query"
echo

./query.sh $FABRIC $STORE_NAME "$KEYS" false countByValue "firstName,lastName" 10 | grep -E "(firstName-counts=|lastName-counts=)"
echo

echo "=== Step 4: countByBucket Demo ==="
echo "countByBucket groups records by bucket predicates (e.g., age ranges)"
echo

./query.sh $FABRIC $STORE_NAME "$KEYS" false countByBucket age "20-25,26-30,31-35,36-40,41-45" | grep "age-bucket-counts="
echo

echo "=== Demo Summary ==="
echo "✅ countByValue: Counts distinct values with TopK feature for performance"
echo "✅ countByBucket: Groups records by custom bucket definitions"
echo "✅ Pure client-side aggregation: No server-side compute required"
echo "✅ TopK Feature: Built into countByValue to limit result size"
echo "✅ Multi-field Support: Can aggregate multiple fields simultaneously"
echo
echo "Demo completed successfully! 🚀" 