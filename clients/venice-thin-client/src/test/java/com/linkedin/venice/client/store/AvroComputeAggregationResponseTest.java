package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link AvroComputeAggregationResponse} which verifies the aggregation result
 * processing with various input data and edge cases.
 * 
 * Test Strategy:
 * 1. Normal cases: Verify basic aggregation functionality
 * 2. Edge cases: Test boundary conditions and special values
 * 3. Error cases: Verify proper error handling
 * 4. Performance: Test with large data sets
 * 5. Concurrency: Test thread safety
 */
@Test(groups = { "unit" })
public class AvroComputeAggregationResponseTest extends TestUtils {
  private static final int DEFAULT_TIMEOUT_MS = 5000;
  private static final String FIELD_1 = "field1";
  private static final String FIELD_2 = "field2";
  private static final String VALUE_1 = "value1";
  private static final String VALUE_2 = "value2";
  private static final String COUNT_SUFFIX = "_count";
  private static final int MAX_CONCURRENT_REQUESTS = 100;

  @Mock
  private ComputeGenericRecord record1;
  @Mock
  private ComputeGenericRecord record2;
  @Mock
  private ComputeGenericRecord record3;
  @Mock
  private GenericRecord key1;
  @Mock
  private GenericRecord key2;
  @Mock
  private GenericRecord key3;

  private Map<String, ComputeGenericRecord> computeResults;
  private Map<String, Integer> fieldTopKMap;
  private AvroComputeAggregationResponse<String> response;

  @BeforeClass(alwaysRun = true)
  public void setUpClass() {
    // Global test setup if needed
  }

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    setupMockData();
    setupFieldTopKMap();
    setupResponse();
  }

  private void setupMockData() {
    computeResults = new HashMap<>();

    // Setup record1 with value1
    when(record1.get(FIELD_1)).thenReturn(VALUE_1);
    when(record1.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(5);
    when(record1.get(FIELD_2)).thenReturn(VALUE_2);
    when(record1.get(FIELD_2 + COUNT_SUFFIX)).thenReturn(3);

    // Setup record2 with value2
    when(record2.get(FIELD_1)).thenReturn(VALUE_2);
    when(record2.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(3);
    when(record2.get(FIELD_2)).thenReturn(VALUE_1);
    when(record2.get(FIELD_2 + COUNT_SUFFIX)).thenReturn(5);

    // Setup record3 with null values
    when(record3.get(FIELD_1)).thenReturn(null);
    when(record3.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(2);
    when(record3.get(FIELD_2)).thenReturn(null);
    when(record3.get(FIELD_2 + COUNT_SUFFIX)).thenReturn(2);

    computeResults.put("key1", record1);
    computeResults.put("key2", record2);
    computeResults.put("key3", record3);
  }

  private void setupFieldTopKMap() {
    fieldTopKMap = new HashMap<>();
    fieldTopKMap.put(FIELD_1, 10);
    fieldTopKMap.put(FIELD_2, 10);
  }

  private void setupResponse() {
    response = new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);
  }

  // Group 1: Normal Cases

  @Test(description = "Should process normal case correctly")
  public void testGetValueToCount_NormalCase() {
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 3, "Should have counts for all values");
    assertEquals(result.get(VALUE_1), Integer.valueOf(5), "Value1 count should be 5");
    assertEquals(result.get(VALUE_2), Integer.valueOf(3), "Value2 count should be 3");
    assertEquals(result.get(null), Integer.valueOf(2), "Null count should be 2");
  }

  @Test(description = "Should handle empty results")
  public void testGetValueToCount_EmptyResults_Basic() {
    response = new AvroComputeAggregationResponse<>(new HashMap<>(), fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Result should not be null even with empty input");
    assertTrue(result.isEmpty(), "Result should be empty with empty input");
  }

  @Test(description = "Should handle null field values")
  public void testGetValueToCount_NullFieldValues_Basic() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    ComputeGenericRecord record = mock(ComputeGenericRecord.class);
    when(record.get(FIELD_1)).thenReturn(null);
    when(record.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(1);
    results.put("key1", record);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.get(null), Integer.valueOf(1), "Null value count should be 1");
  }

  @Test(description = "Should handle equal counts correctly")
  public void testGetValueToCount_EqualCounts() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();

    ComputeGenericRecord record = mock(ComputeGenericRecord.class);
    when(record.get(FIELD_1)).thenReturn(VALUE_1);
    when(record.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(5);
    results.put("key1", record);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(FIELD_1)).thenReturn(VALUE_2);
    when(record2.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(5);
    results.put("key2", record2);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertEquals(result.get(VALUE_1), result.get(VALUE_2), "Values with equal counts should be preserved");
  }

  @Test(description = "Should handle non-existent field")
  public void testGetValueToCount_NonExistentField_Basic() {
    assertThrows(IllegalArgumentException.class, () -> response.getValueToCount("nonexistent_field"));
  }

  @Test(description = "Should handle null counts")
  public void testGetValueToCount_NullCounts_Basic() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    ComputeGenericRecord record = mock(ComputeGenericRecord.class);
    when(record.get(FIELD_1)).thenReturn(VALUE_1);
    when(record.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(null);
    results.put("key1", record);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Field counts should not be null");
    assertEquals(result.get(VALUE_1), Integer.valueOf(0), "Null count should be treated as 0");
  }

  @Test(description = "Should handle single result")
  public void testGetValueToCount_SingleResult_Basic() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    results.put("key1", record1);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 1, "Should have one count");
    assertEquals(result.get(VALUE_1), Integer.valueOf(5), "Should have correct count for single result");
  }

  // Group 2: Edge Cases

  @Test(description = "Should handle single result with topK=1")
  public void testGetValueToCount_SingleResult_TopK() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    ComputeGenericRecord record = mock(ComputeGenericRecord.class);
    when(record.get(FIELD_1)).thenReturn("single");
    when(record.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(1);
    results.put("key1", record);

    Map<String, Integer> topKMap = new HashMap<>();
    topKMap.put(FIELD_1, 1);
    response = new AvroComputeAggregationResponse<>(results, topKMap);

    Map<String, Integer> valueToCount = response.getValueToCount(FIELD_1);
    assertNotNull(valueToCount, "Result map should not be null");
    assertEquals(valueToCount.size(), 1, "Should return exactly one result");
    assertEquals(valueToCount.get("single").intValue(), 1, "Should contain the correct count");
  }

  @Test(description = "Should handle empty compute results")
  public void testGetValueToCount_EmptyResults_Edge() {
    response = new AvroComputeAggregationResponse<>(new HashMap<>(), fieldTopKMap);
    Map<String, Integer> valueToCount = response.getValueToCount(FIELD_1);
    assertNotNull(valueToCount, "Result map should not be null even with empty results");
    assertTrue(valueToCount.isEmpty(), "Result map should be empty");
  }

  @Test(description = "Should handle null field values")
  public void testGetValueToCount_NullFieldValues_Edge() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(FIELD_1)).thenReturn(null);
    when(record1.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(5);
    results.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(FIELD_1)).thenReturn("value2");
    when(record2.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(3);
    results.put("key2", record2);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> valueToCount = response.getValueToCount(FIELD_1);

    assertNotNull(valueToCount, "Result map should not be null");
    assertEquals(valueToCount.size(), 2, "Should include null value in results");
    assertEquals(valueToCount.get(null).intValue(), 5, "Should contain count for null value");
    assertEquals(valueToCount.get("value2").intValue(), 3, "Should contain count for non-null value");
  }

  // Group 3: Error Cases

  @Test(description = "Should handle null count values")
  public void testGetValueToCount_NullCounts_Error() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(FIELD_1)).thenReturn("value1");
    when(record1.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(null);
    results.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(FIELD_1)).thenReturn("value2");
    when(record2.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(3);
    results.put("key2", record2);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> valueToCount = response.getValueToCount(FIELD_1);

    assertNotNull(valueToCount, "Result map should not be null");
    assertEquals(valueToCount.size(), 2, "Should include both values");
    assertEquals(valueToCount.get("value1").intValue(), 0, "Should treat null count as 0");
    assertEquals(valueToCount.get("value2").intValue(), 3, "Should contain the non-null count");
  }

  @Test(description = "Should handle non-existent field")
  public void testGetValueToCount_NonExistentField_Error() {
    assertThrows(IllegalArgumentException.class, () -> response.getValueToCount("nonexistent"));
  }

  @Test(description = "Should handle concurrent access safely")
  public void testGetValueToCount_Concurrent() throws Exception {
    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(MAX_CONCURRENT_REQUESTS);
    ConcurrentLinkedQueue<Map<String, Integer>> results = new ConcurrentLinkedQueue<>();

    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < MAX_CONCURRENT_REQUESTS; i++) {
      futures.add(executor.submit(() -> {
        try {
          results.add(response.getValueToCount(FIELD_1));
        } finally {
          latch.countDown();
        }
      }));
    }

    assertTrue(latch.await(30, TimeUnit.SECONDS), "Concurrent requests timed out");
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor shutdown timed out");

    // Verify all results are consistent
    Map<String, Integer> firstResult = results.peek();
    assertNotNull(firstResult, "First result should not be null");
    assertEquals(firstResult.size(), 3, "Should have three values");

    for (Map<String, Integer> result: results) {
      assertEquals(result, firstResult, "All results should be identical");
    }
  }

  @Test(description = "Should handle performance requirements")
  public void testGetValueToCount_Performance() {
    // Setup large test data
    Map<String, ComputeGenericRecord> largeComputeResults = new HashMap<>();
    Map<String, Integer> fieldTopK = new HashMap<>();
    fieldTopK.put(FIELD_1, 10);
    int recordCount = 10000;

    for (int i = 0; i < recordCount; i++) {
      String key = "value" + (i % 100); // Create some value distribution
      ComputeGenericRecord value = mock(ComputeGenericRecord.class);
      when(value.get(FIELD_1)).thenReturn(key);
      when(value.get(FIELD_1 + COUNT_SUFFIX)).thenReturn(1);
      largeComputeResults.put("key" + i, value);
    }

    response = new AvroComputeAggregationResponse<>(largeComputeResults, fieldTopK);

    long startTime = System.nanoTime();
    Map<String, Integer> result = response.getValueToCount(FIELD_1);
    long endTime = System.nanoTime();
    long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

    assertNotNull(result, "Result should not be null");
    assertTrue(duration < 1000, "Performance test should complete within 1 second");
    assertEquals(result.size(), Math.min(100, fieldTopK.get(FIELD_1)), "Should return correct number of results");
  }

  @Test(description = "Should handle bucket name to count")
  public void testGetBucketNameToCount() {
    assertThrows(UnsupportedOperationException.class, () -> response.getBucketNameToCount(FIELD_1));
  }
}
