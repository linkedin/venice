package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
          assertEquals(result.get(null), Integer.valueOf(2));
          assertEquals(result.get("full-time"), Integer.valueOf(1));
          assertEquals(result.size(), 2);
        });
  }

  @Test(description = "Should handle Utf8 values correctly")
  public void testUtf8ValueHandling() {
    runCountByValueTest(
        () -> createUtf8TestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          assertEquals(result.get("utf8-value"), Integer.valueOf(2));
          assertEquals(result.get("normal-value"), Integer.valueOf(1));
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
          assertEquals(result.size(), 2);
        });
  }

  @Test(description = "Should handle empty result set")
  public void testEmptyResultSet() {
    runCountByValueTest(() -> new HashMap<>(), Collections.singletonMap(JOB_TYPE_FIELD, 10), JOB_TYPE_FIELD, result -> {
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
          assertEquals(result.get("single-value"), Integer.valueOf(1));
          assertEquals(result.size(), 1);
        });
  }

  @Test(description = "Should handle all null values")
  public void testAllNullValues() {
    runCountByValueTest(
        () -> createAllNullTestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 10),
        JOB_TYPE_FIELD,
        result -> {
          assertEquals(result.get(null), Integer.valueOf(3));
          assertEquals(result.size(), 1);
        });
  }

  @Test(description = "Should handle topK larger than distinct values")
  public void testTopKLargerThanDistinctValues() {
    runCountByValueTest(
        () -> createSimpleTestData(),
        Collections.singletonMap(JOB_TYPE_FIELD, 100),
        JOB_TYPE_FIELD,
        result -> {
          // Should return all distinct values even though topK is large
          assertEquals(result.get("full-time"), Integer.valueOf(3));
          assertEquals(result.get("part-time"), Integer.valueOf(3));
          assertEquals(result.size(), 2);

          // Verify order is still correct
          List<Map.Entry<?, Integer>> entries = new ArrayList<>(result.entrySet());
          assertEquals(entries.get(0).getValue(), entries.get(1).getValue()); // tied
        });
  }

  @Test(description = "Should handle field not in topK map")
  public void testFieldNotInTopKMap() {
    runCountByValueTest(
        () -> createSimpleTestData(),
        Collections.singletonMap("otherField", 10),
        JOB_TYPE_FIELD,
        result -> {
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

  // AggregationUtils tests - only test the public API
  @Test(description = "Should test AggregationUtils getBucketNameToCount")
  public void testAggregationUtilsBucketCounting() {
    // Create test records with age field
    List<ComputeGenericRecord> records = new ArrayList<>();

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get("age")).thenReturn(25);
    records.add(record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get("age")).thenReturn(35);
    records.add(record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get("age")).thenReturn(20);
    records.add(record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get("age")).thenReturn(45);
    records.add(record4);

    // Create bucket predicates
    Map<String, Predicate> buckets = new LinkedHashMap<>();
    buckets.put("young", IntPredicate.lowerThan(30));
    buckets.put("adult", Predicate.and(IntPredicate.greaterOrEquals(30), IntPredicate.lowerThan(40)));
    buckets.put("senior", IntPredicate.greaterOrEquals(40));

    // Test getBucketNameToCount
    Map<String, Integer> result = AggregationUtils.getBucketNameToCount(records, "age", buckets);

    assertEquals(result.get("young"), Integer.valueOf(2)); // ages 25, 20
    assertEquals(result.get("adult"), Integer.valueOf(1)); // age 35
    assertEquals(result.get("senior"), Integer.valueOf(1)); // age 45
    assertEquals(result.size(), 3);
  }

  @Test(description = "Should test AggregationUtils convertToType")
  public void testAggregationUtilsConvertToType() {
    // Test Integer conversion
    assertEquals(AggregationUtils.convertToType(42L, Integer.class), Integer.valueOf(42));
    assertEquals(AggregationUtils.convertToType("100", Integer.class), Integer.valueOf(100));
    assertEquals(AggregationUtils.convertToType(50, Integer.class), Integer.valueOf(50));

    // Test Long conversion
    assertEquals(AggregationUtils.convertToType(42, Long.class), Long.valueOf(42L));
    assertEquals(AggregationUtils.convertToType("200", Long.class), Long.valueOf(200L));
    assertEquals(AggregationUtils.convertToType(75L, Long.class), Long.valueOf(75L));

    // Test null handling
    assertNull(AggregationUtils.convertToType(null, Integer.class));
    assertNull(AggregationUtils.convertToType("invalid", Integer.class));
  }

  @Test(description = "Should test AggregationUtils normalizeValue for Utf8")
  public void testAggregationUtilsNormalizeUtf8() {
    // Test Utf8 normalization
    Utf8 utf8Value = new Utf8("test-string");
    Object normalized = AggregationUtils.normalizeValue(utf8Value);
    assertEquals(normalized, "test-string");
    assertTrue(normalized instanceof String);

    // Test non-Utf8 values pass through unchanged
    String stringValue = "plain-string";
    assertEquals(AggregationUtils.normalizeValue(stringValue), stringValue);

    Integer intValue = 42;
    assertEquals(AggregationUtils.normalizeValue(intValue), intValue);

    assertNull(AggregationUtils.normalizeValue(null));
  }

  @Test(description = "Should test AggregationUtils getValueToCount")
  public void testAggregationUtilsGetValueToCount() {
    // Create test records
    List<ComputeGenericRecord> records = new ArrayList<>();

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get("status")).thenReturn("active");
    records.add(record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get("status")).thenReturn("inactive");
    records.add(record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get("status")).thenReturn("active");
    records.add(record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get("status")).thenReturn("pending");
    records.add(record4);

    // Test getValueToCount with topK = 2
    Map<Object, Integer> result = AggregationUtils.getValueToCount(records, "status", 2);

    assertEquals(result.size(), 2);

    // Verify descending order by count
    List<Map.Entry<Object, Integer>> entries = new ArrayList<>(result.entrySet());
    assertEquals(entries.get(0).getKey(), "active"); // count = 2
    assertEquals(entries.get(0).getValue(), Integer.valueOf(2));
    assertEquals(entries.get(1).getKey(), "inactive"); // count = 1
    assertEquals(entries.get(1).getValue(), Integer.valueOf(1));

    // "pending" should not be included due to topK = 2
    assertFalse(result.containsKey("pending"));
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
    when(job5.get(EXPERIENCE_FIELD)).thenReturn("senior");
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

    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn(null);
    data.put("job1", job1);

    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(JOB_TYPE_FIELD)).thenReturn("full-time");
    data.put("job2", job2);

    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(JOB_TYPE_FIELD)).thenReturn(null);
    data.put("job3", job3);

    return data;
  }

  private Map<String, ComputeGenericRecord> createUtf8TestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn(new Utf8("utf8-value"));
    data.put("job1", job1);

    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(JOB_TYPE_FIELD)).thenReturn("normal-value");
    data.put("job2", job2);

    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(JOB_TYPE_FIELD)).thenReturn(new Utf8("utf8-value"));
    data.put("job3", job3);

    return data;
  }

  private Map<String, ComputeGenericRecord> createNumericTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(SALARY_FIELD)).thenReturn(50000);
    data.put("job1", job1);

    ComputeGenericRecord job2 = mock(ComputeGenericRecord.class);
    when(job2.get(SALARY_FIELD)).thenReturn(60000);
    data.put("job2", job2);

    ComputeGenericRecord job3 = mock(ComputeGenericRecord.class);
    when(job3.get(SALARY_FIELD)).thenReturn(50000);
    data.put("job3", job3);

    return data;
  }

  private Map<String, ComputeGenericRecord> createSingleRecordTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    ComputeGenericRecord job1 = mock(ComputeGenericRecord.class);
    when(job1.get(JOB_TYPE_FIELD)).thenReturn("single-value");
    data.put("job1", job1);

    return data;
  }

  private Map<String, ComputeGenericRecord> createAllNullTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

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
