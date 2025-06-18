package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  @Test(description = "Should handle getBucketNameToCount throws exception")
  public void testGetBucketNameToCountThrowsException() {
    computeResults = createSimpleTestData();
    fieldTopKMap.put(JOB_TYPE_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    try {
      response.getBucketNameToCount(JOB_TYPE_FIELD);
      assert false : "Should have thrown UnsupportedOperationException";
    } catch (UnsupportedOperationException e) {
      assertEquals(e.getMessage(), "getBucketNameToCount is not implemented");
    }
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
