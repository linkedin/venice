package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.util.Utf8;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test suite for {@link AvroComputeAggregationResponse}.
 * 
 * Tests the aggregation response processor for handling results from compute aggregation
 * operations on Avro records.
 * 
 * Key Areas:
 * - Value counting result processing with topK ordering and null handling
 * - Bucket aggregation result processing with various predicate types
 * - Data type handling (String, Integer, Long, Float, Double, Utf8, null)
 * - Edge cases including empty results, single records, and boundary conditions
 * - Result ordering and topK limit enforcement
 * 
 * @see AvroComputeAggregationResponse
 */
public class AvroComputeAggregationResponseTest {
  private static final String JOB_TYPE_FIELD = "jobType";
  private static final String LOCATION_FIELD = "location";
  private static final String EXPERIENCE_FIELD = "experienceLevel";
  private static final String SALARY_FIELD = "salary";
  private static final String AGE_FIELD = "age";
  private static final String SCORE_FIELD = "score";

  private Map<String, ComputeGenericRecord> computeResults;
  private Map<String, Integer> fieldTopKMap;

  @BeforeMethod
  public void setUp() {
    computeResults = new HashMap<>();
    fieldTopKMap = new HashMap<>();
  }

  @Test(description = "Should correctly count simple field values")
  public void testSimpleFieldCounting() {
    computeResults = createSimpleTestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Expected counts: full-time=3, part-time=3
    assertEquals(result.get("full-time"), Integer.valueOf(3));
    assertEquals(result.get("part-time"), Integer.valueOf(3));
    assertEquals(result.size(), 2);
  }

  @Test(description = "Should respect topK limit and return descending order")
  public void testTopKAndOrdering() {
    computeResults = createSimpleTestData();
    fieldTopKMap.put(LOCATION_FIELD, 2);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(LOCATION_FIELD);

    // Should only return top 2 results
    assertEquals(result.size(), 2);

    // Verify descending order: remote=3, onsite=2
    List<Map.Entry<String, Integer>> entries = new ArrayList<>(result.entrySet());
    assertEquals(entries.get(0).getKey(), "remote");
    assertEquals(entries.get(0).getValue(), Integer.valueOf(3));
    assertEquals(entries.get(1).getKey(), "onsite");
    assertEquals(entries.get(1).getValue(), Integer.valueOf(2));

    // hybrid (count=1) should not be included
    assertFalse(result.containsKey("hybrid"));
  }

  @Test(description = "Should handle null values correctly")
  public void testNullValueHandling() {
    computeResults = createNullTestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Verify null handling: null=2, full-time=1, part-time=1
    assertEquals(result.get(null), Integer.valueOf(2));
    assertEquals(result.get("full-time"), Integer.valueOf(1));
    assertEquals(result.get("part-time"), Integer.valueOf(1));
  }

  @Test(description = "Should handle Utf8 values correctly")
  public void testUtf8ValueHandling() {
    computeResults = createUtf8TestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Verify Utf8 conversion: full-time=2, part-time=1
    assertEquals(result.get("full-time"), Integer.valueOf(2));
    assertEquals(result.get("part-time"), Integer.valueOf(1));
    assertEquals(result.size(), 2);
  }

  @Test(description = "Should handle numeric values correctly")
  public void testNumericValueHandling() {
    computeResults = createNumericTestData();
    fieldTopKMap.put(SALARY_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(SALARY_FIELD);

    // Verify numeric values: 50000=2, 60000=1, 70000=1
    assertEquals(result.get("50000"), Integer.valueOf(2));
    assertEquals(result.get("60000"), Integer.valueOf(1));
    assertEquals(result.get("70000"), Integer.valueOf(1));
    assertEquals(result.size(), 3);
  }

  @Test(description = "Should handle empty result set")
  public void testEmptyResultSet() {
    // Empty compute results
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Should return empty map
    assertTrue(result.isEmpty());
  }

  @Test(description = "Should handle single record")
  public void testSingleRecord() {
    computeResults = createSingleRecordTestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Should return single entry
    assertEquals(result.size(), 1);
    assertEquals(result.get("full-time"), Integer.valueOf(1));
  }

  @Test(description = "Should handle all null values")
  public void testAllNullValues() {
    computeResults = createAllNullTestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Should return only null entry
    assertEquals(result.size(), 1);
    assertEquals(result.get(null), Integer.valueOf(3));
  }

  @Test(description = "Should handle topK larger than distinct values")
  public void testTopKLargerThanDistinctValues() {
    computeResults = createSimpleTestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 100); // Much larger than distinct values

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Should return all distinct values
    assertEquals(result.size(), 2);
    assertEquals(result.get("full-time"), Integer.valueOf(3));
    assertEquals(result.get("part-time"), Integer.valueOf(3));
  }

  @Test(description = "Should handle field not in topK map")
  public void testFieldNotInTopKMap() {
    computeResults = createSimpleTestData();
    // Don't add field to fieldTopKMap

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(JOB_TYPE_FIELD);

    // Should return all values (no limit applied)
    assertEquals(result.size(), 2);
    assertEquals(result.get("full-time"), Integer.valueOf(3));
    assertEquals(result.get("part-time"), Integer.valueOf(3));
  }

  @Test(description = "Should handle mixed data types")
  public void testMixedDataTypes() {
    computeResults = createMixedDataTypesTestData();
    fieldTopKMap.put(AGE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(AGE_FIELD);

    // Verify mixed types are handled correctly
    assertEquals(result.get("25"), Integer.valueOf(2));
    assertEquals(result.get("30"), Integer.valueOf(1));
    assertEquals(result.get("35"), Integer.valueOf(1));
    assertEquals(result.size(), 3);
  }

  // ========== Test Data Creation Methods ==========

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

  // ========== Bucket Aggregation Tests ==========

  @Test(description = "Should handle IntPredicate correctly")
  public void testIntPredicateHandling() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("mid", Predicate.and(IntPredicate.greaterOrEquals(30), IntPredicate.lowerThan(50)));
    ageBuckets.put("senior", IntPredicate.greaterOrEquals(50));
    fieldBucketMap.put(AGE_FIELD, ageBuckets);
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25); // young
    computeResults.put("key1", record1);
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(35); // mid
    computeResults.put("key2", record2);
    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn(55); // senior
    computeResults.put("key3", record3);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);
    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);
    assertEquals(bucketCounts.get("young"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("mid"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("senior"), Integer.valueOf(1));
  }

  @Test(description = "Should handle LongPredicate correctly")
  public void testLongPredicateHandling() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> salaryBuckets = new HashMap<>();
    salaryBuckets.put("low", LongPredicate.lowerThan(50000L));
    salaryBuckets.put("high", LongPredicate.greaterOrEquals(50000L));
    fieldBucketMap.put(SALARY_FIELD, salaryBuckets);
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(SALARY_FIELD)).thenReturn(40000L); // low
    computeResults.put("key1", record1);
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(SALARY_FIELD)).thenReturn(60000L); // high
    computeResults.put("key2", record2);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);
    Map<String, Integer> bucketCounts = response.getBucketNameToCount(SALARY_FIELD);
    assertEquals(bucketCounts.get("low"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("high"), Integer.valueOf(1));
  }

  @Test(description = "Should handle FloatPredicate correctly")
  public void testFloatPredicateHandling() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> salaryBuckets = new HashMap<>();
    salaryBuckets.put("cheap", FloatPredicate.lowerThan(10000.0f));
    salaryBuckets.put("expensive", FloatPredicate.greaterOrEquals(10000.0f));
    fieldBucketMap.put(SALARY_FIELD, salaryBuckets);
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(SALARY_FIELD)).thenReturn(5000.0f); // cheap
    computeResults.put("key1", record1);
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(SALARY_FIELD)).thenReturn(20000.0f); // expensive
    computeResults.put("key2", record2);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);
    Map<String, Integer> bucketCounts = response.getBucketNameToCount(SALARY_FIELD);
    assertEquals(bucketCounts.get("cheap"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("expensive"), Integer.valueOf(1));
  }

  @Test(description = "Should handle DoublePredicate correctly")
  public void testDoublePredicateHandling() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> salaryBuckets = new HashMap<>();
    salaryBuckets.put("low", DoublePredicate.lowerThan(10000.0));
    salaryBuckets.put("high", DoublePredicate.greaterOrEquals(10000.0));
    fieldBucketMap.put(SALARY_FIELD, salaryBuckets);
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(SALARY_FIELD)).thenReturn(5000.0); // low
    computeResults.put("key1", record1);
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(SALARY_FIELD)).thenReturn(20000.0); // high
    computeResults.put("key2", record2);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);
    Map<String, Integer> bucketCounts = response.getBucketNameToCount(SALARY_FIELD);
    assertEquals(bucketCounts.get("low"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("high"), Integer.valueOf(1));
  }

  @Test(description = "Should handle StringPredicate correctly")
  public void testStringPredicateHandling() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();
    Map<String, Predicate> jobTypeBuckets = new HashMap<>();
    jobTypeBuckets.put("engineer", Predicate.equalTo("engineer"));
    jobTypeBuckets.put("manager", Predicate.equalTo("manager"));
    fieldBucketMap.put(JOB_TYPE_FIELD, jobTypeBuckets);
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(JOB_TYPE_FIELD)).thenReturn("engineer");
    computeResults.put("key1", record1);
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(JOB_TYPE_FIELD)).thenReturn("manager");
    computeResults.put("key2", record2);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);
    Map<String, Integer> bucketCounts = response.getBucketNameToCount(JOB_TYPE_FIELD);
    assertEquals(bucketCounts.get("engineer"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("manager"), Integer.valueOf(1));
  }

  // ========== Additional Bucket Counting Tests ==========

  @Test(description = "Should handle mixed data types in bucket counting")
  public void testMixedDataTypesInBucketCounting() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup mixed bucket predicates
    Map<String, Predicate> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    Map<String, Predicate> salaryBuckets = new HashMap<>();
    salaryBuckets.put("high", FloatPredicate.greaterThan(80000.0f));
    fieldBucketMap.put(SALARY_FIELD, salaryBuckets);

    Map<String, Predicate> scoreBuckets = new HashMap<>();
    scoreBuckets.put("excellent", DoublePredicate.greaterThan(90.0));
    fieldBucketMap.put(SCORE_FIELD, scoreBuckets);

    // Create test records with mixed data types
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25); // young
    when(record1.get(SALARY_FIELD)).thenReturn(90000.0f); // high
    when(record1.get(SCORE_FIELD)).thenReturn(95.5); // excellent
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(35); // not young
    when(record2.get(SALARY_FIELD)).thenReturn(70000.0f); // not high
    when(record2.get(SCORE_FIELD)).thenReturn(85.0); // not excellent
    computeResults.put("key2", record2);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    // Test all bucket types
    Map<String, Integer> ageBucketCounts = response.getBucketNameToCount(AGE_FIELD);
    Map<String, Integer> salaryBucketCounts = response.getBucketNameToCount(SALARY_FIELD);
    Map<String, Integer> scoreBucketCounts = response.getBucketNameToCount(SCORE_FIELD);

    // Expected counts
    assertEquals(ageBucketCounts.get("young"), Integer.valueOf(1));
    assertEquals(salaryBucketCounts.get("high"), Integer.valueOf(1));
    assertEquals(scoreBucketCounts.get("excellent"), Integer.valueOf(1));
  }

  @Test(description = "Should handle overlapping bucket predicates")
  public void testOverlappingBucketPredicates() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup overlapping bucket predicates
    Map<String, Predicate> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(50)); // 0-49
    ageBuckets.put("middle", IntPredicate.greaterThan(30)); // 31+
    // Note: ages 31-49 will match both buckets
    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    // Create test records
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25); // only young
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(40); // both young and middle
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn(60); // only middle
    computeResults.put("key3", record3);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    // Expected counts: young=2 (25, 40), middle=2 (40, 60)
    assertEquals(bucketCounts.get("young"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("middle"), Integer.valueOf(2));
  }

  @Test(description = "Should handle edge cases for bucket counting")
  public void testEdgeCasesForBucketCounting() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup edge case bucket predicates
    Map<String, Predicate> edgeBuckets = new HashMap<>();
    edgeBuckets.put("zero", IntPredicate.equalTo(0));
    edgeBuckets.put("negative", IntPredicate.lowerThan(0));
    edgeBuckets.put("max", IntPredicate.equalTo(Integer.MAX_VALUE));
    fieldBucketMap.put(AGE_FIELD, edgeBuckets);

    // Create test records with edge values
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(0); // zero
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(-5); // negative
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn(Integer.MAX_VALUE); // max
    computeResults.put("key3", record3);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    // Expected counts: zero=1, negative=1, max=1
    assertEquals(bucketCounts.get("zero"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("negative"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("max"), Integer.valueOf(1));
  }
}
