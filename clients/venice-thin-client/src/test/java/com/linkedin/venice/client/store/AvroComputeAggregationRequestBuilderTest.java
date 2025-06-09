package com.linkedin.venice.client.store;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.schema.SchemaReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests for {@link AvroComputeAggregationRequestBuilder} which verifies the request building
 * and execution with various configurations and edge cases.
 */
public class AvroComputeAggregationRequestBuilderTest {
  private static final String FIELD_1 = "field1";
  private static final String FIELD_2 = "field2";
  private static final int TOP_K = 10;
  private static final int MAX_CONCURRENT_REQUESTS = 100;
  private static final String STORE_NAME = "test_store";

  @Mock
  private AvroGenericReadComputeStoreClient<String, GenericRecord> storeClient;
  @Mock
  private SchemaReader schemaReader;
  @Mock
  private Schema valueSchema;
  @Mock
  private Schema.Field field1;
  @Mock
  private Schema.Field field2;
  @Mock
  private Schema field1Schema;
  @Mock
  private Schema field2Schema;
  @Mock
  private Schema arrayItemSchema;

  private AvroComputeRequestBuilderV3<String> computeRequestBuilder;
  private AvroComputeAggregationRequestBuilder<String> builder;
  private List<String> keys;
  private Map<String, Integer> fieldTopKMap;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    setupSchemaMocks();
    setupTestData();

    // Setup store name first
    when(storeClient.getStoreName()).thenReturn(STORE_NAME);

    // Create computeRequestBuilder with proper mocks
    computeRequestBuilder = spy(new AvroComputeRequestBuilderV3<>(storeClient, schemaReader));

    // Setup compute() after creating the spy
    when(storeClient.compute()).thenReturn(computeRequestBuilder);

    // Create the builder
    builder = new AvroComputeAggregationRequestBuilder<>(storeClient, schemaReader);
  }

  private void setupSchemaMocks() {
    when(schemaReader.getLatestValueSchemaId()).thenReturn(1);
    when(schemaReader.getValueSchema(1)).thenReturn(valueSchema);
    when(valueSchema.getType()).thenReturn(Schema.Type.RECORD);

    // Setup field1 mocks
    when(valueSchema.getField(FIELD_1)).thenReturn(field1);
    when(field1.schema()).thenReturn(field1Schema);
    when(field1Schema.getType()).thenReturn(Schema.Type.ARRAY);
    when(field1Schema.getElementType()).thenReturn(arrayItemSchema);

    // Setup field2 mocks
    when(valueSchema.getField(FIELD_2)).thenReturn(field2);
    when(field2.schema()).thenReturn(field2Schema);
    when(field2Schema.getType()).thenReturn(Schema.Type.ARRAY);
    when(field2Schema.getElementType()).thenReturn(arrayItemSchema);
  }

  private void setupTestData() {
    keys = Arrays.asList("key1", "key2", "key3");
    fieldTopKMap = new HashMap<>();
    fieldTopKMap.put(FIELD_1, TOP_K);
    fieldTopKMap.put(FIELD_2, TOP_K);
  }

  @Test(description = "Should build request with valid parameters")
  public void testCountGroupByValue_ValidParameters() {
    builder.countGroupByValue(TOP_K, FIELD_1);
    verify(computeRequestBuilder).count(eq(FIELD_1), eq(FIELD_1 + "_count"));
  }

  @Test(description = "Should handle multiple fields")
  public void testCountGroupByValue_ManyFields() {
    builder.countGroupByValue(TOP_K, FIELD_1, FIELD_2);

    verify(computeRequestBuilder).count(eq(FIELD_1), eq(FIELD_1 + "_count"));
    verify(computeRequestBuilder).count(eq(FIELD_2), eq(FIELD_2 + "_count"));
  }

  @DataProvider(name = "invalidTopK")
  public Object[][] getInvalidTopK() {
    return new Object[][] { { 0, "TopK must be positive" }, { -1, "TopK must be positive" },
        { Integer.MIN_VALUE, "TopK must be positive" } };
  }

  @Test(dataProvider = "invalidTopK", description = "Should reject invalid topK values")
  public void testCountGroupByValue_InvalidTopK(int topK, String expectedMessage) {
    VeniceClientException exception =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(topK, FIELD_1));
    assertTrue(exception.getMessage().contains(expectedMessage));
  }

  @DataProvider(name = "invalidFieldNames")
  public Object[][] getInvalidFieldNames() {
    return new Object[][] { { null, "Field name cannot be null" }, { "", "Field name cannot be empty" },
        { "nonexistent", "Field not found in schema" } };
  }

  @Test(dataProvider = "invalidFieldNames", description = "Should reject invalid field names")
  public void testCountGroupByValue_InvalidFieldNames(String fieldName, String expectedMessage) {
    VeniceClientException exception =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(TOP_K, fieldName));
    assertTrue(exception.getMessage().contains(expectedMessage));
  }

  @Test(description = "Should handle empty field name")
  public void testCountGroupByValue_EmptyFieldName() {
    assertThrows(VeniceClientException.class, () -> builder.countGroupByValue(TOP_K, ""));
  }

  @Test(description = "Should execute with valid parameters")
  public void testExecute_ValidParameters() {
    Set<String> keySet = new HashSet<>(keys);

    builder.countGroupByValue(TOP_K, FIELD_1);
    builder.execute(keySet);

    verify(computeRequestBuilder).count(eq(FIELD_1), eq(FIELD_1 + "_count"));
    verify(computeRequestBuilder).execute(eq(keySet));
  }

  @Test(description = "Should handle null keys")
  public void testExecute_NullKeys() {
    builder.countGroupByValue(TOP_K, FIELD_1);
    assertThrows(VeniceClientException.class, () -> builder.execute(null));
  }

  @Test(description = "Should handle empty keys")
  public void testExecute_EmptyKeys() {
    builder.countGroupByValue(TOP_K, FIELD_1);
    assertThrows(VeniceClientException.class, () -> builder.execute(new HashSet<>()));
  }

  @Test(description = "Should handle concurrent requests")
  public void testCountGroupByValue_Concurrent() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(10);
    CountDownLatch latch = new CountDownLatch(MAX_CONCURRENT_REQUESTS);
    Set<String> keySet = new HashSet<>(keys);

    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < MAX_CONCURRENT_REQUESTS; i++) {
      futures.add(executor.submit(() -> {
        try {
          builder.countGroupByValue(TOP_K, FIELD_1).execute(keySet);
        } finally {
          latch.countDown();
        }
      }));
    }

    assertTrue(latch.await(30, TimeUnit.SECONDS), "Concurrent requests timed out");
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor shutdown timed out");

    verify(computeRequestBuilder, times(MAX_CONCURRENT_REQUESTS)).count(eq(FIELD_1), eq(FIELD_1 + "_count"));
    verify(computeRequestBuilder, times(MAX_CONCURRENT_REQUESTS)).execute(eq(keySet));
  }

  @Test(description = "Should handle performance requirements")
  public void testCountGroupByValue_Performance() {
    Set<String> keySet = new HashSet<>(keys);
    int iterations = 1000;

    // Create a new builder for each iteration to avoid reuse issues
    long startTime = System.nanoTime();
    for (int i = 0; i < iterations; i++) {
      AvroComputeRequestBuilderV3<String> newComputeRequestBuilder =
          spy(new AvroComputeRequestBuilderV3<>(storeClient, schemaReader));
      when(storeClient.compute()).thenReturn(newComputeRequestBuilder);
      AvroComputeAggregationRequestBuilder<String> newBuilder =
          new AvroComputeAggregationRequestBuilder<>(storeClient, schemaReader);

      newBuilder.countGroupByValue(TOP_K, FIELD_1).execute(keySet);
    }
    long endTime = System.nanoTime();
    long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

    assertTrue(duration < 1000, "Performance test should complete within 1 second");
  }
}
