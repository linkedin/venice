package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for {@link AvroComputeAggregationResponse} which verifies the aggregation result
 * processing with various input data and edge cases.
 * 
 * Test Strategy:
 * 1. Normal cases: Verify basic aggregation functionality
 * 2. Edge cases: Test boundary conditions and special values
 * 3. Error cases: Verify proper error handling
 * 4. Concurrency: Test thread safety
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
  private static final String STRING_FIELD_NAME = "string_field";
  private static final String INT_FIELD_NAME = "int_field";
  private static final String LONG_FIELD_NAME = "long_field";
  private static final String FLOAT_FIELD_NAME = "float_field";
  private static final String DOUBLE_FIELD_NAME = "double_field";
  private static final String BOOLEAN_FIELD_NAME = "boolean_field";
  private static final String UNSUPPORTED_FIELD_NAME = "unsupported_field";

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

    // Setup record1 with array values
    List<String> arrayValues1 = Arrays.asList(VALUE_1, VALUE_2, VALUE_1, VALUE_1);
    when(record1.get(FIELD_1)).thenReturn(arrayValues1);

    Map<String, String> mapValues1 = new HashMap<>();
    mapValues1.put("key1", VALUE_2);
    mapValues1.put("key2", VALUE_2);
    mapValues1.put("key3", VALUE_2);
    when(record1.get(FIELD_2)).thenReturn(mapValues1);

    // Setup record2 with array values
    List<String> arrayValues2 = Arrays.asList(VALUE_2, VALUE_2, VALUE_2);
    when(record2.get(FIELD_1)).thenReturn(arrayValues2);

    Map<String, String> mapValues2 = new HashMap<>();
    mapValues2.put("key4", VALUE_1);
    mapValues2.put("key5", VALUE_1);
    mapValues2.put("key6", VALUE_1);
    mapValues2.put("key7", VALUE_1);
    mapValues2.put("key8", VALUE_1);
    when(record2.get(FIELD_2)).thenReturn(mapValues2);

    // Setup record3 with null and empty collections
    when(record3.get(FIELD_1)).thenReturn(null);
    when(record3.get(FIELD_2)).thenReturn(new HashMap<>());

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

  @DataProvider(name = "fieldTypes")
  public Object[][] getFieldTypes() {
    return new Object[][] { { STRING_FIELD_NAME, Schema.Type.STRING, true }, { INT_FIELD_NAME, Schema.Type.INT, true },
        { LONG_FIELD_NAME, Schema.Type.LONG, true }, { FLOAT_FIELD_NAME, Schema.Type.FLOAT, true },
        { DOUBLE_FIELD_NAME, Schema.Type.DOUBLE, true }, { BOOLEAN_FIELD_NAME, Schema.Type.BOOLEAN, true },
        { UNSUPPORTED_FIELD_NAME, Schema.Type.ARRAY, false } };
  }

  @Test(dataProvider = "fieldTypes", description = "Should handle different field types correctly")
  public void testGetValueToCount_FieldTypes(String fieldName, Schema.Type fieldType, boolean shouldSucceed) {
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    ComputeGenericRecord record = mock(ComputeGenericRecord.class);

    // Setup mock data based on field type - always use collections
    Object value = getMockCollectionForType(fieldType);
    when(record.get(fieldName)).thenReturn(value);
    results.put("key1", record);

    Map<String, Integer> fieldTopK = new HashMap<>();
    fieldTopK.put(fieldName, 10);
    response = new AvroComputeAggregationResponse<>(results, fieldTopK);

    Map<Object, Integer> result = response.getValueToCount(fieldName);
    assertNotNull(result, "Result should not be null");

    if (value instanceof List) {
      assertEquals(result.size(), 1, "Should have one unique value from array");
      assertTrue(result.containsKey(getMockValueForType(fieldType)));
    } else if (value instanceof Map) {
      assertEquals(result.size(), 1, "Should have one unique value from map");
      assertTrue(result.containsKey(getMockValueForType(fieldType)));
    }
  }

  private Object getMockCollectionForType(Schema.Type type) {
    Object value = getMockValueForType(type);
    return Arrays.asList(value); // Always return as array for testing
  }

  private Object getMockValueForType(Schema.Type type) {
    switch (type) {
      case STRING:
        return "test_string";
      case INT:
        return 42;
      case LONG:
        return 42L;
      case FLOAT:
        return 42.0f;
      case DOUBLE:
        return 42.0d;
      case BOOLEAN:
        return true;
      default:
        return null;
    }
  }

  // Group 1: Normal Cases

  @Test(description = "Should process normal case correctly")
  public void testGetValueToCount_NormalCase() {
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 3, "Should have counts for all values including null");
    assertEquals(result.get(VALUE_1), Integer.valueOf(3), "Value1 count should be 3");
    assertEquals(result.get(VALUE_2), Integer.valueOf(4), "Value2 count should be 4");
    assertEquals(result.get(null), Integer.valueOf(1), "Null count should be 1");
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
    List<String> values1 = Arrays.asList(VALUE_1, VALUE_1, VALUE_1, VALUE_1, VALUE_1);
    when(record.get(FIELD_1)).thenReturn(values1);
    results.put("key1", record);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    List<String> values2 = Arrays.asList(VALUE_2, VALUE_2, VALUE_2, VALUE_2, VALUE_2);
    when(record2.get(FIELD_1)).thenReturn(values2);
    results.put("key2", record2);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertEquals(result.get(VALUE_1), result.get(VALUE_2), "Values with equal counts should be preserved");
    assertEquals(result.get(VALUE_1), Integer.valueOf(5), "Both values should have count of 5");
  }

  @Test(description = "Should handle non-existent field")
  public void testGetValueToCount_NonExistentField_Basic() {
    assertThrows(IllegalArgumentException.class, () -> response.getValueToCount("nonexistent_field"));
  }

  @Test(description = "Should handle null counts")
  public void testGetValueToCount_NullCounts_Basic() {
    // This test is no longer relevant since we count actual values, not pre-computed counts
    // Removing or updating to test empty collections instead
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    ComputeGenericRecord record = mock(ComputeGenericRecord.class);
    when(record.get(FIELD_1)).thenReturn(new ArrayList<>()); // Empty array
    results.put("key1", record);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Result should not be null");
    assertTrue(result.isEmpty(), "Empty array should produce empty result");
  }

  @Test(description = "Should handle single result")
  public void testGetValueToCount_SingleResult_Basic() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    results.put("key1", record1);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> result = response.getValueToCount(FIELD_1);

    assertNotNull(result, "Result should not be null");
    assertEquals(result.size(), 2, "Should have two unique values");
    assertEquals(result.get(VALUE_1), Integer.valueOf(3), "Should have correct count for value1");
    assertEquals(result.get(VALUE_2), Integer.valueOf(1), "Should have correct count for value2");
  }

  // Group 2: Edge Cases

  @Test(description = "Should handle single result with topK=1")
  public void testGetValueToCount_SingleResult_TopK() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();
    ComputeGenericRecord record = mock(ComputeGenericRecord.class);
    List<String> values = Arrays.asList("single", "single", "other");
    when(record.get(FIELD_1)).thenReturn(values);
    results.put("key1", record);

    Map<String, Integer> topKMap = new HashMap<>();
    topKMap.put(FIELD_1, 1);
    response = new AvroComputeAggregationResponse<>(results, topKMap);

    Map<String, Integer> valueToCount = response.getValueToCount(FIELD_1);
    assertNotNull(valueToCount, "Result map should not be null");
    assertEquals(valueToCount.size(), 1, "Should return exactly one result");
    assertEquals(valueToCount.get("single").intValue(), 2, "Should contain the top count");
    assertNull(valueToCount.get("other"), "Lower count value should not be included");
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
    results.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    List<String> values = Arrays.asList("value2", "value2", "value2");
    when(record2.get(FIELD_1)).thenReturn(values);
    results.put("key2", record2);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> valueToCount = response.getValueToCount(FIELD_1);

    assertNotNull(valueToCount, "Result map should not be null");
    assertEquals(valueToCount.size(), 2, "Should include null value in results");
    assertEquals(valueToCount.get(null).intValue(), 1, "Should contain count for null value");
    assertEquals(valueToCount.get("value2").intValue(), 3, "Should contain count for non-null value");
  }

  // Group 3: Error Cases

  @Test(description = "Should handle null count values")
  public void testGetValueToCount_NullCounts_Error() {
    // Update test to check handling of mixed collection types
    Map<String, ComputeGenericRecord> results = new HashMap<>();

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    List<String> arrayValues = Arrays.asList("value1");
    when(record1.get(FIELD_1)).thenReturn(arrayValues);
    results.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    Map<String, String> mapValues = new HashMap<>();
    mapValues.put("k1", "value2");
    mapValues.put("k2", "value2");
    mapValues.put("k3", "value2");
    when(record2.get(FIELD_1)).thenReturn(mapValues);
    results.put("key2", record2);

    response = new AvroComputeAggregationResponse<>(results, fieldTopKMap);
    Map<String, Integer> valueToCount = response.getValueToCount(FIELD_1);

    assertNotNull(valueToCount, "Result map should not be null");
    assertEquals(valueToCount.size(), 2, "Should include both values");
    assertEquals(valueToCount.get("value1").intValue(), 1, "Should have count from array");
    assertEquals(valueToCount.get("value2").intValue(), 3, "Should have count from map values");
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

  @Test(description = "Should aggregate counts from array fields correctly")
  public void testGetValueToCount_ArrayField_RealScenario() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get("tags")).thenReturn(Arrays.asList("java", "python", "java"));
    results.put("user1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get("tags")).thenReturn(Arrays.asList("python", "golang"));
    results.put("user2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get("tags")).thenReturn(Arrays.asList("java", "rust"));
    results.put("user3", record3);

    Map<String, Integer> fieldTopK = new HashMap<>();
    fieldTopK.put("tags", 3);

    AvroComputeAggregationResponse<String> response = new AvroComputeAggregationResponse<>(results, fieldTopK);

    Map<String, Integer> tagCounts = response.getValueToCount("tags");

    assertEquals(tagCounts.size(), 3, "Should return exactly top 3 values");
    assertEquals(tagCounts.get("java"), Integer.valueOf(3));
    assertEquals(tagCounts.get("python"), Integer.valueOf(2));
    // Either golang or rust will be included (both have count of 1)
    assertTrue(
        tagCounts.containsKey("golang") || tagCounts.containsKey("rust"),
        "Should include one of the values with count 1");
  }

  @Test(description = "Should aggregate counts from map fields correctly")
  public void testGetValueToCount_MapField_RealScenario() {
    Map<String, ComputeGenericRecord> results = new HashMap<>();

    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    Map<String, String> preferences1 = new HashMap<>();
    preferences1.put("color", "blue");
    preferences1.put("theme", "dark");
    preferences1.put("language", "en");
    when(record1.get("preferences")).thenReturn(preferences1);
    results.put("user1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    Map<String, String> preferences2 = new HashMap<>();
    preferences2.put("color", "blue");
    preferences2.put("theme", "light");
    preferences2.put("language", "en");
    when(record2.get("preferences")).thenReturn(preferences2);
    results.put("user2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    Map<String, String> preferences3 = new HashMap<>();
    preferences3.put("color", "red");
    preferences3.put("theme", "dark");
    preferences3.put("language", "es");
    when(record3.get("preferences")).thenReturn(preferences3);
    results.put("user3", record3);

    Map<String, Integer> fieldTopK = new HashMap<>();
    fieldTopK.put("preferences", 5);

    AvroComputeAggregationResponse<String> response = new AvroComputeAggregationResponse<>(results, fieldTopK);

    Map<String, Integer> preferenceCounts = response.getValueToCount("preferences");

    assertEquals(preferenceCounts.get("blue"), Integer.valueOf(2));
    assertEquals(preferenceCounts.get("dark"), Integer.valueOf(2));
    assertEquals(preferenceCounts.get("en"), Integer.valueOf(2));
    assertEquals(preferenceCounts.get("light"), Integer.valueOf(1));
    assertEquals(preferenceCounts.get("red"), Integer.valueOf(1));
  }
}
