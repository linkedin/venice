package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.avro.util.Utf8;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Core test suite for AvroComputeAggregationResponse value counting functionality.
 * Focuses on essential aggregation logic, topK ordering, and edge case handling.
 */
public class AvroComputeAggregationResponseTest {
  private static final String JOB_TYPE_FIELD = "jobType";
  private static final String LOCATION_FIELD = "location";
  private static final String EXPERIENCE_FIELD = "experienceLevel";
  private static final String SALARY_FIELD = "salary";
  private static final String AGE_FIELD = "age";

  private Map<String, ComputeGenericRecord> computeResults;
  private Map<String, Integer> fieldTopKMap;

  @BeforeMethod
  public void setUp() {
    computeResults = new HashMap<>();
    fieldTopKMap = new HashMap<>();
  }

  // Common aggregation test method
  private <T> void runCountByValueTest(
      Supplier<Map<String, ComputeGenericRecord>> dataSupplier,
      Map<String, Integer> fieldTopKMap,
      String fieldName,
      Consumer<Map<T, Integer>> assertionLogic) {
    Map<String, ComputeGenericRecord> computeResults = dataSupplier.get();
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);
    Map<T, Integer> result = response.getValueToCount(fieldName);
    assertionLogic.accept(result);
  }

  @Test(description = "Should correctly count simple field values")
  public void testSimpleFieldCounting() {
    runCountByValueTest(
        () -> createSimpleTestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          assertEquals(result.get("full-time"), Integer.valueOf(3));
          assertEquals(result.get("part-time"), Integer.valueOf(3));
          assertEquals(result.size(), 2);
        });
  }

  @Test(description = "Should respect topK limit and return descending order")
  public void testTopKAndOrdering() {
    runCountByValueTest(
        () -> createSimpleTestData(),
        Collections.singletonMap(LOCATION_FIELD, 2),
        LOCATION_FIELD,
        result -> {
          // Should only return top 2 results
          assertEquals(result.size(), 2);

          // Verify descending order: remote=3, onsite=2
          List<Map.Entry<?, Integer>> entries = new ArrayList<>(result.entrySet());
          assertEquals(entries.get(0).getKey(), "remote");
          assertEquals(entries.get(0).getValue(), Integer.valueOf(3));
          assertEquals(entries.get(1).getKey(), "onsite");
          assertEquals(entries.get(1).getValue(), Integer.valueOf(2));

          // hybrid (count=1) should not be included
          assertFalse(result.containsKey("hybrid"));
        });
  }

  @Test(description = "Should handle null values correctly")
  public void testNullValueHandling() {
    runCountByValueTest(
        () -> createNullTestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          // Verify null handling: null=2, full-time=1, part-time=1
          assertEquals(result.get(null), Integer.valueOf(2));
          assertEquals(result.get("full-time"), Integer.valueOf(1));
          assertEquals(result.get("part-time"), Integer.valueOf(1));
        });
  }

  @Test(description = "Should handle Utf8 values correctly")
  public void testUtf8ValueHandling() {
    runCountByValueTest(
        () -> createUtf8TestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          // Verify Utf8 conversion: full-time=2, part-time=1
          assertEquals(result.get("full-time"), Integer.valueOf(2));
          assertEquals(result.get("part-time"), Integer.valueOf(1));
          assertEquals(result.size(), 2);
        });
  }

  @Test(description = "Should handle numeric values correctly")
  public void testNumericValueHandling() {
    runCountByValueTest(
        () -> createNumericTestData(),
        Collections.singletonMap(SALARY_FIELD, 10),
        SALARY_FIELD,
        result -> {
          assertEquals(result.get(50000), Integer.valueOf(2));
          assertEquals(result.get(60000), Integer.valueOf(1));
          assertEquals(result.get(70000), Integer.valueOf(1));
          assertEquals(result.size(), 3);
        });
  }

  @Test(description = "Should handle empty result set")
  public void testEmptyResultSet() {
    runCountByValueTest(
        () -> new HashMap<>(), // Empty compute results
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          // Should return empty map
          assertTrue(result.isEmpty());
        });
  }

  @Test(description = "Should handle single record")
  public void testSingleRecord() {
    runCountByValueTest(
        () -> createSingleRecordTestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          // Should return single entry
          assertEquals(result.size(), 1);
          assertEquals(result.get("full-time"), Integer.valueOf(1));
        });
  }

  @Test(description = "Should handle all null values")
  public void testAllNullValues() {
    runCountByValueTest(
        () -> createAllNullTestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          // Should return only null entry
          assertEquals(result.size(), 1);
          assertEquals(result.get(null), Integer.valueOf(3));
        });
  }

  @Test(description = "Should handle topK larger than distinct values")
  public void testTopKLargerThanDistinctValues() {
    runCountByValueTest(
        () -> createSimpleTestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 100), // Much larger than distinct values
        JOB_TYPE_FIELD,
        result -> {
          // Should return all distinct values
          assertEquals(result.size(), 2);
          assertEquals(result.get("full-time"), Integer.valueOf(3));
          assertEquals(result.get("part-time"), Integer.valueOf(3));
        });
  }

  @Test(description = "Should handle field not in topK map")
  public void testFieldNotInTopKMap() {
    runCountByValueTest(
        () -> createSimpleTestData(),
        new HashMap<>(), // Don't add field to fieldTopKMap
        JOB_TYPE_FIELD,
        result -> {
          // Should return empty map when field is not configured for aggregation
          assertEquals(result.size(), 0);
          assertTrue(result.isEmpty());
        });
  }

  @Test(description = "Should handle mixed data types")
  public void testMixedDataTypes() {
    runCountByValueTest(
        () -> createMixedDataTypesTestData(),
        Collections.singletonMap(AGE_FIELD, 10),
        AGE_FIELD,
        result -> {
          assertEquals(result.get(25), Integer.valueOf(2));
          assertEquals(result.get(30), Integer.valueOf(1));
          assertEquals(result.get(35), Integer.valueOf(1));
          assertEquals(result.size(), 3);
        });
  }

  @Test(description = "Should handle getBucketNameToCount throws exception")
  public void testGetBucketNameToCountThrowsException() {
    computeResults = createSimpleTestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse response = new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    try {
      response.getBucketNameToCount(JOB_TYPE_FIELD);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("No count-by-bucket aggregation was requested for field"));
    }
  }

  @Test(description = "Should throw ClassCastException when type casting fails", expectedExceptions = ClassCastException.class)
  public void testTypeCastingThrowsException() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn("25");
    data.put("record1", record1);
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn("30");
    data.put("record2", record2);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(data, Collections.singletonMap(AGE_FIELD, 10));

    Map<Object, Integer> valueToCount = response.getValueToCount(AGE_FIELD);
    Object firstKey = valueToCount.keySet().iterator().next();
    System.identityHashCode((Integer) firstKey);
  }

  // ========== countByBucket Tests ==========

  @Test(description = "Should handle all IntPredicate methods correctly")
  public void testIntPredicateMethods() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> ageBuckets = new HashMap<>();

    // Test all IntPredicate methods
    ageBuckets.put("equal_25", IntPredicate.equalTo(25));
    ageBuckets.put("greater_30", IntPredicate.greaterThan(30));
    ageBuckets.put("greater_equal_30", IntPredicate.greaterOrEquals(30));
    ageBuckets.put("less_30", IntPredicate.lowerThan(30));
    ageBuckets.put("less_equal_30", IntPredicate.lowerOrEquals(30));
    ageBuckets.put("any_of", IntPredicate.anyOf(25, 35, 45));

    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    // Test data: 25, 30, 35, 40, 45
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(30);
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn(35);
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(AGE_FIELD)).thenReturn(40);
    computeResults.put("key4", record4);

    ComputeGenericRecord record5 = mock(ComputeGenericRecord.class);
    when(record5.get(AGE_FIELD)).thenReturn(45);
    computeResults.put("key5", record5);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    assertEquals(bucketCounts.get("equal_25"), Integer.valueOf(1)); // 25
    assertEquals(bucketCounts.get("greater_30"), Integer.valueOf(3)); // 35, 40, 45
    assertEquals(bucketCounts.get("greater_equal_30"), Integer.valueOf(4)); // 30, 35, 40, 45
    assertEquals(bucketCounts.get("less_30"), Integer.valueOf(1)); // 25
    assertEquals(bucketCounts.get("less_equal_30"), Integer.valueOf(2)); // 25, 30
    assertEquals(bucketCounts.get("any_of"), Integer.valueOf(3)); // 25, 35, 45
  }

  @Test(description = "Should handle all LongPredicate methods correctly")
  public void testLongPredicateMethods() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> salaryBuckets = new HashMap<>();

    // Test all LongPredicate methods
    salaryBuckets.put("equal_50000", LongPredicate.equalTo(50000L));
    salaryBuckets.put("greater_50000", LongPredicate.greaterThan(50000L));
    salaryBuckets.put("greater_equal_50000", LongPredicate.greaterOrEquals(50000L));
    salaryBuckets.put("less_50000", LongPredicate.lowerThan(50000L));
    salaryBuckets.put("less_equal_50000", LongPredicate.lowerOrEquals(50000L));
    salaryBuckets.put("any_of", LongPredicate.anyOf(40000L, 50000L, 60000L));

    fieldBucketMap.put(SALARY_FIELD, salaryBuckets);

    // Test data: 40000L, 50000L, 60000L, 70000L
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(SALARY_FIELD)).thenReturn(40000L);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(SALARY_FIELD)).thenReturn(50000L);
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(SALARY_FIELD)).thenReturn(60000L);
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(SALARY_FIELD)).thenReturn(70000L);
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(SALARY_FIELD);

    assertEquals(bucketCounts.get("equal_50000"), Integer.valueOf(1)); // 50000L
    assertEquals(bucketCounts.get("greater_50000"), Integer.valueOf(2)); // 60000L, 70000L
    assertEquals(bucketCounts.get("greater_equal_50000"), Integer.valueOf(3)); // 50000L, 60000L, 70000L
    assertEquals(bucketCounts.get("less_50000"), Integer.valueOf(1)); // 40000L
    assertEquals(bucketCounts.get("less_equal_50000"), Integer.valueOf(2)); // 40000L, 50000L
    assertEquals(bucketCounts.get("any_of"), Integer.valueOf(3)); // 40000L, 50000L, 60000L
  }

  @Test(description = "Should handle all FloatPredicate methods correctly")
  public void testFloatPredicateMethods() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> salaryBuckets = new HashMap<>();

    // Test all FloatPredicate methods
    salaryBuckets.put("equal_10000", FloatPredicate.equalTo(10000.0f, 0.1f));
    salaryBuckets.put("greater_10000", FloatPredicate.greaterThan(10000.0f));
    salaryBuckets.put("greater_equal_10000", FloatPredicate.greaterOrEquals(10000.0f));
    salaryBuckets.put("less_10000", FloatPredicate.lowerThan(10000.0f));
    salaryBuckets.put("less_equal_10000", FloatPredicate.lowerOrEquals(10000.0f));
    salaryBuckets.put("any_of", FloatPredicate.anyOf(5000.0f, 10000.0f, 15000.0f));

    fieldBucketMap.put(SALARY_FIELD, salaryBuckets);

    // Test data: 5000.0f, 10000.0f, 15000.0f, 20000.0f
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(SALARY_FIELD)).thenReturn(5000.0f);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(SALARY_FIELD)).thenReturn(10000.0f);
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(SALARY_FIELD)).thenReturn(15000.0f);
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(SALARY_FIELD)).thenReturn(20000.0f);
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(SALARY_FIELD);

    assertEquals(bucketCounts.get("equal_10000"), Integer.valueOf(1)); // 10000.0f
    assertEquals(bucketCounts.get("greater_10000"), Integer.valueOf(2)); // 15000.0f, 20000.0f
    assertEquals(bucketCounts.get("greater_equal_10000"), Integer.valueOf(3)); // 10000.0f, 15000.0f, 20000.0f
    assertEquals(bucketCounts.get("less_10000"), Integer.valueOf(1)); // 5000.0f
    assertEquals(bucketCounts.get("less_equal_10000"), Integer.valueOf(2)); // 5000.0f, 10000.0f
    assertEquals(bucketCounts.get("any_of"), Integer.valueOf(3)); // 5000.0f, 10000.0f, 15000.0f
  }

  @Test(description = "Should handle all DoublePredicate methods correctly")
  public void testDoublePredicateMethods() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> salaryBuckets = new HashMap<>();

    // Test all DoublePredicate methods
    salaryBuckets.put("equal_10000", DoublePredicate.equalTo(10000.0, 0.1));
    salaryBuckets.put("greater_10000", DoublePredicate.greaterThan(10000.0));
    salaryBuckets.put("greater_equal_10000", DoublePredicate.greaterOrEquals(10000.0));
    salaryBuckets.put("less_10000", DoublePredicate.lowerThan(10000.0));
    salaryBuckets.put("less_equal_10000", DoublePredicate.lowerOrEquals(10000.0));
    salaryBuckets.put("any_of", DoublePredicate.anyOf(5000.0, 10000.0, 15000.0));

    fieldBucketMap.put(SALARY_FIELD, salaryBuckets);

    // Test data: 5000.0, 10000.0, 15000.0, 20000.0
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(SALARY_FIELD)).thenReturn(5000.0);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(SALARY_FIELD)).thenReturn(10000.0);
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(SALARY_FIELD)).thenReturn(15000.0);
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(SALARY_FIELD)).thenReturn(20000.0);
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(SALARY_FIELD);

    assertEquals(bucketCounts.get("equal_10000"), Integer.valueOf(1)); // 10000.0
    assertEquals(bucketCounts.get("greater_10000"), Integer.valueOf(2)); // 15000.0, 20000.0
    assertEquals(bucketCounts.get("greater_equal_10000"), Integer.valueOf(3)); // 10000.0, 15000.0, 20000.0
    assertEquals(bucketCounts.get("less_10000"), Integer.valueOf(1)); // 5000.0
    assertEquals(bucketCounts.get("less_equal_10000"), Integer.valueOf(2)); // 5000.0, 10000.0
    assertEquals(bucketCounts.get("any_of"), Integer.valueOf(3)); // 5000.0, 10000.0, 15000.0
  }

  @Test(description = "Should handle String predicate methods correctly")
  public void testStringPredicateMethods() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> jobTypeBuckets = new HashMap<>();

    // Test String predicate methods
    jobTypeBuckets.put("equal_engineer", Predicate.equalTo("engineer"));
    jobTypeBuckets.put("any_of", Predicate.anyOf("engineer", "manager", "designer"));

    fieldBucketMap.put(JOB_TYPE_FIELD, jobTypeBuckets);

    // Test data: "engineer", "manager", "designer", "analyst"
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(JOB_TYPE_FIELD)).thenReturn("engineer");
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(JOB_TYPE_FIELD)).thenReturn("manager");
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(JOB_TYPE_FIELD)).thenReturn("designer");
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(JOB_TYPE_FIELD)).thenReturn("analyst");
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(JOB_TYPE_FIELD);

    assertEquals(bucketCounts.get("equal_engineer"), Integer.valueOf(1)); // "engineer"
    assertEquals(bucketCounts.get("any_of"), Integer.valueOf(3)); // "engineer", "manager", "designer"
  }

  @Test(description = "Should handle complex predicate combinations")
  public void testComplexPredicateCombinations() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> ageBuckets = new HashMap<>();

    // Test complex combinations
    ageBuckets.put("and_combination", Predicate.and(IntPredicate.greaterThan(20), IntPredicate.lowerThan(50)));
    ageBuckets.put("or_combination", Predicate.or(IntPredicate.lowerThan(30), IntPredicate.greaterThan(50)));
    ageBuckets.put(
        "nested_combination",
        Predicate
            .and(IntPredicate.greaterThan(20), Predicate.or(IntPredicate.lowerThan(30), IntPredicate.greaterThan(50))));

    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    // Test data: 25, 35, 55, 15, 60
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(35);
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn(55);
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(AGE_FIELD)).thenReturn(15);
    computeResults.put("key4", record4);

    ComputeGenericRecord record5 = mock(ComputeGenericRecord.class);
    when(record5.get(AGE_FIELD)).thenReturn(60);
    computeResults.put("key5", record5);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    assertEquals(bucketCounts.get("and_combination"), Integer.valueOf(2)); // 25, 35
    assertEquals(bucketCounts.get("or_combination"), Integer.valueOf(4)); // 25, 55, 15, 60
    assertEquals(bucketCounts.get("nested_combination"), Integer.valueOf(3)); // 25, 55, 60
  }

  @Test(description = "Should handle edge cases and boundary conditions")
  public void testEdgeCasesAndBoundaryConditions() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> ageBuckets = new HashMap<>();

    // Test edge cases
    ageBuckets.put("zero", IntPredicate.equalTo(0));
    ageBuckets.put("negative", IntPredicate.lowerThan(0));
    ageBuckets.put("max_value", IntPredicate.equalTo(Integer.MAX_VALUE));
    ageBuckets.put("min_value", IntPredicate.equalTo(Integer.MIN_VALUE));

    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    // Test data: 0, -5, Integer.MAX_VALUE, Integer.MIN_VALUE
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(0);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(-5);
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn(Integer.MAX_VALUE);
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(AGE_FIELD)).thenReturn(Integer.MIN_VALUE);
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    assertEquals(bucketCounts.get("zero"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("negative"), Integer.valueOf(2)); // -5, Integer.MIN_VALUE
    assertEquals(bucketCounts.get("max_value"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("min_value"), Integer.valueOf(1));
  }

  @Test(description = "Should handle null values and empty results")
  public void testNullValuesAndEmptyResults() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> ageBuckets = new HashMap<>();

    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("senior", IntPredicate.greaterOrEquals(30));

    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    // Test with null values
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(null);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(25);
    computeResults.put("key2", record2);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    assertEquals(bucketCounts.get("young"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("senior"), Integer.valueOf(0));

    // Test empty result set
    AvroComputeAggregationResponse emptyResponse =
        new AvroComputeAggregationResponse<>(new HashMap<>(), new HashMap<>(), fieldBucketMap);

    Map<String, Integer> emptyBucketCounts = emptyResponse.getBucketNameToCount(AGE_FIELD);

    assertEquals(emptyBucketCounts.get("young"), Integer.valueOf(0));
    assertEquals(emptyBucketCounts.get("senior"), Integer.valueOf(0));
  }

  @Test(description = "Should handle field not in bucket map")
  public void testFieldNotInBucketMap() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    // Don't add any buckets for AGE_FIELD

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25);
    computeResults.put("key1", record1);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    expectThrows(IllegalArgumentException.class, () -> response.getBucketNameToCount(AGE_FIELD));
  }

  @Test(description = "Should handle mixed data types in bucket counting")
  public void testMixedDataTypesInBucketCounting() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("senior", IntPredicate.greaterOrEquals(30));
    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    // Test with different data types
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25); // Integer
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(35L); // Long
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn("40"); // String
    computeResults.put("key3", record3);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    assertEquals(bucketCounts.get("young"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("senior"), Integer.valueOf(2));
  }

  @Test(description = "Should handle all numeric type conversion combinations in convertToType")
  public void testAllNumericTypeConversionCombinations() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> testBuckets = new HashMap<>();

    // Test all numeric type conversion combinations with unique values
    testBuckets.put("int_to_long", LongPredicate.equalTo(100L));
    testBuckets.put("int_to_float", FloatPredicate.equalTo(200.0f, 0.1f));
    testBuckets.put("int_to_double", DoublePredicate.equalTo(300.0, 0.1));
    testBuckets.put("long_to_int", IntPredicate.equalTo(400));
    testBuckets.put("string_to_int", IntPredicate.equalTo(500));
    testBuckets.put("string_to_long", LongPredicate.equalTo(600L));
    testBuckets.put("string_to_float", FloatPredicate.equalTo(700.0f, 0.1f));
    testBuckets.put("string_to_double", DoublePredicate.equalTo(800.0, 0.1));

    fieldBucketMap.put("testField", testBuckets);

    // Test data with different types for conversion testing - each value matches only one predicate
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get("testField")).thenReturn(100); // Integer -> Long
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get("testField")).thenReturn(200); // Integer -> Float
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get("testField")).thenReturn(300); // Integer -> Double
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get("testField")).thenReturn(400L); // Long -> Integer
    computeResults.put("key4", record4);

    ComputeGenericRecord record5 = mock(ComputeGenericRecord.class);
    when(record5.get("testField")).thenReturn("500"); // String -> Integer
    computeResults.put("key5", record5);

    ComputeGenericRecord record6 = mock(ComputeGenericRecord.class);
    when(record6.get("testField")).thenReturn("600"); // String -> Long
    computeResults.put("key6", record6);

    ComputeGenericRecord record7 = mock(ComputeGenericRecord.class);
    when(record7.get("testField")).thenReturn("700.0"); // String -> Float
    computeResults.put("key7", record7);

    ComputeGenericRecord record8 = mock(ComputeGenericRecord.class);
    when(record8.get("testField")).thenReturn("800.0"); // String -> Double
    computeResults.put("key8", record8);

    AvroComputeAggregationResponse response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount("testField");

    // Verify all type conversions work correctly - each bucket should have exactly 1 match
    assertEquals(bucketCounts.get("int_to_long"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("int_to_float"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("int_to_double"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("long_to_int"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("string_to_int"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("string_to_long"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("string_to_float"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("string_to_double"), Integer.valueOf(1));
  }

  private Map<String, ComputeGenericRecord> createSimpleTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Job 1
    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn("full-time");
    when(job1.get(LOCATION_FIELD)).thenReturn("remote");
    when(job1.get(EXPERIENCE_FIELD)).thenReturn("senior");
    data.put("job1", job1);

    // Job 2
    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(JOB_TYPE_FIELD)).thenReturn("part-time");
    when(job2.get(LOCATION_FIELD)).thenReturn("onsite");
    when(job2.get(EXPERIENCE_FIELD)).thenReturn("junior");
    data.put("job2", job2);

    // Job 3
    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(JOB_TYPE_FIELD)).thenReturn("full-time");
    when(job3.get(LOCATION_FIELD)).thenReturn("remote");
    when(job3.get(EXPERIENCE_FIELD)).thenReturn("mid-level");
    data.put("job3", job3);

    // Job 4
    ComputeGenericRecord job4 = mock(ComputeGenericRecord.class);
    when(job4.get(JOB_TYPE_FIELD)).thenReturn("part-time");
    when(job4.get(LOCATION_FIELD)).thenReturn("hybrid");
    when(job4.get(EXPERIENCE_FIELD)).thenReturn("senior");
    data.put("job4", job4);

    // Job 5
    ComputeGenericRecord job5 = mock(ComputeGenericRecord.class);
    when(job5.get(JOB_TYPE_FIELD)).thenReturn("full-time");
    when(job5.get(LOCATION_FIELD)).thenReturn("remote");
    when(job5.get(EXPERIENCE_FIELD)).thenReturn("junior");
    data.put("job5", job5);

    // Job 6
    ComputeGenericRecord job6 = mock(ComputeGenericRecord.class);
    when(job6.get(JOB_TYPE_FIELD)).thenReturn("part-time");
    when(job6.get(LOCATION_FIELD)).thenReturn("onsite");
    when(job6.get(EXPERIENCE_FIELD)).thenReturn("mid-level");
    data.put("job6", job6);

    return data;
  }

  private Map<String, ComputeGenericRecord> createNullTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Job 1: null field value
    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn(null);
    data.put("job1", job1);

    // Job 2: null field value
    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(JOB_TYPE_FIELD)).thenReturn(null);
    data.put("job2", job2);

    // Job 3: full-time
    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(JOB_TYPE_FIELD)).thenReturn("full-time");
    data.put("job3", job3);

    // Job 4: part-time
    ComputeGenericRecord job4 = mock(ComputeGenericRecord.class);
    when(job4.get(JOB_TYPE_FIELD)).thenReturn("part-time");
    data.put("job4", job4);

    return data;
  }

  private Map<String, ComputeGenericRecord> createUtf8TestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Job 1: Utf8 value
    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn(new Utf8("full-time"));
    data.put("job1", job1);

    // Job 2: String value
    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(JOB_TYPE_FIELD)).thenReturn("part-time");
    data.put("job2", job2);

    // Job 3: Utf8 value
    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(JOB_TYPE_FIELD)).thenReturn(new Utf8("full-time"));
    data.put("job3", job3);

    return data;
  }

  private Map<String, ComputeGenericRecord> createNumericTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Job 1: Integer value
    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(SALARY_FIELD)).thenReturn(50000);
    data.put("job1", job1);

    // Job 2: Integer value
    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(SALARY_FIELD)).thenReturn(60000);
    data.put("job2", job2);

    // Job 3: Integer value
    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(SALARY_FIELD)).thenReturn(50000);
    data.put("job3", job3);

    // Job 4: Integer value
    ComputeGenericRecord job4 = mock(ComputeGenericRecord.class);
    when(job4.get(SALARY_FIELD)).thenReturn(70000);
    data.put("job4", job4);

    return data;
  }

  private Map<String, ComputeGenericRecord> createSingleRecordTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Single job record
    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn("full-time");
    data.put("job1", job1);

    return data;
  }

  private Map<String, ComputeGenericRecord> createAllNullTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // All null values
    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn(null);
    data.put("job1", job1);

    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(JOB_TYPE_FIELD)).thenReturn(null);
    data.put("job2", job2);

    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(JOB_TYPE_FIELD)).thenReturn(null);
    data.put("job3", job3);

    return data;
  }

  private Map<String, ComputeGenericRecord> createMixedDataTypesTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Mixed data types
    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(AGE_FIELD)).thenReturn(25);
    data.put("job1", job1);

    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(AGE_FIELD)).thenReturn(30);
    data.put("job2", job2);

    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(AGE_FIELD)).thenReturn(25);
    data.put("job3", job3);

    ComputeGenericRecord job4 = mock(ComputeGenericRecord.class);
    when(job4.get(AGE_FIELD)).thenReturn(35);
    data.put("job4", job4);

    return data;
  }
}
