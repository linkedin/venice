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

echo "=== Introduction ==="
echo "This demo showcases two new aggregation features in Venice Thin Client:"
echo "1. countByValue: Count distinct values with TopK optimization"
echo "2. countByBucket: Group records by custom bucket predicates"
echo "Both features run entirely on the client-side for optimal performance."
echo

echo "=== Data Source ==="
echo "We're using an EI test store ($STORE_NAME) populated with employee data"
echo "using the Venice Producer client. The data includes:"
echo "- firstName: Employee first names"
echo "- lastName: Employee last names"
echo "- age: Employee ages (for bucket analysis)"
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

echo "4.1. Age Bucket Definitions:"
echo "   - 20-25: Young employees (20 <= age < 25)"
echo "   - 26-30: Early career (26 <= age < 30)"
echo "   - 31-35: Mid career (31 <= age < 35)"
echo "   - 36-40: Senior level (36 <= age < 40)"
echo "   - 41-45: Experienced (41 <= age < 45)"
echo

echo "4.2. Age Distribution Results:"
BUCKET_RESULTS=$(./query.sh $FABRIC $STORE_NAME "$KEYS" false countByBucket age "20-25,26-30,31-35,36-40,41-45" | grep "age-bucket-counts=" | sed 's/.*age-bucket-counts=//')

BUCKET_RESULTS=$(echo "$BUCKET_RESULTS" | sed 's/^{//' | sed 's/}$//')

TEMP_FILE=$(mktemp)

echo "$BUCKET_RESULTS" | tr ',' '\n' | while IFS='=' read -r bucket count; do
    bucket=$(echo "$bucket" | tr -d ' ')
    count=$(echo "$count" | tr -d ' ')

    case $bucket in
        "20-25") echo "Young employees: $count" >> "$TEMP_FILE" ;;
        "26-30") echo "Early career: $count" >> "$TEMP_FILE" ;;
        "31-35") echo "Mid career: $count" >> "$TEMP_FILE" ;;
        "36-40") echo "Senior level: $count" >> "$TEMP_FILE" ;;
        "41-45") echo "Experienced: $count" >> "$TEMP_FILE" ;;
        *) echo "$bucket: $count" >> "$TEMP_FILE" ;;
    esac
done

for category in "Young employees" "Early career" "Mid career" "Senior level" "Experienced"; do
    if grep -q "^$category:" "$TEMP_FILE"; then
        count=$(grep "^$category:" "$TEMP_FILE" | cut -d':' -f2 | tr -d ' ')
        echo "   $category: $count"
    fi
done

rm "$TEMP_FILE"
echo

echo "=== Demo Summary ==="
echo "✅ countByValue: Counts distinct values with TopK feature for performance"
echo "✅ countByBucket: Groups records by custom bucket definitions"
echo "✅ Pure client-side aggregation: No server-side compute required"
echo "✅ TopK Feature: Built into countByValue to limit result size"
echo "✅ Multi-field Support: Can aggregate multiple fields simultaneously"
echo

echo "=== Current Project Progress ==="
echo "📋 countByValue: ✅ Implemented and tested"
echo "📋 countByBucket: ✅ Implemented and tested"
echo "📋 Unit Tests: ✅ Comprehensive test coverage"
echo "📋 Integration Tests: ✅ Working with real Venice stores"
echo "📋 Documentation: ✅ API documentation and examples"
echo "📋 Demo Scripts: ✅ Ready for presentation"
echo "📋 Code Review: 🔄 In progress"
echo "📋 Merge Status: ⏳ Pending review approval"
echo
echo "Target: Wednesday Standup presentation"
echo
echo "Demo completed successfully! 🚀"