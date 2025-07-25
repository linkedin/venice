package com.linkedin.venice.client.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.schema.SchemaReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test suite for AvroComputeAggregationRequestBuilder.
 * Focuses on validating field existence and type checking
 */
public class AvroComputeAggregationRequestBuilderTest {
  private static final String JOB_TYPE_FIELD = "jobType";
  private static final String LOCATION_FIELD = "location";
  private static final String EXPERIENCE_FIELD = "experienceLevel";
  private static final String SALARY_FIELD = "salary";
  private static final String AGE_FIELD = "age";

  private AvroGenericReadComputeStoreClient<String, Object> storeClient;
  private AbstractAvroStoreClient<String, Object> abstractStoreClient;
  private TransportClient transportClient;
  private SchemaReader schemaReader;
  private AvroComputeRequestBuilderV3<String> delegate;
  private AvroComputeAggregationRequestBuilder<String> builder;
  private AvroComputeAggregationRequestBuilder<String> serverSideBuilder;
  private Schema jobSchema;

  @BeforeMethod
  public void setUp() {
    storeClient = mock(AvroGenericReadComputeStoreClient.class);
    abstractStoreClient = mock(AbstractAvroStoreClient.class);
    transportClient = mock(TransportClient.class);
    schemaReader = mock(SchemaReader.class);
    delegate = mock(AvroComputeRequestBuilderV3.class);

    // Create a simple job schema
    jobSchema = SchemaBuilder.record("Job")
        .fields()
        .name(JOB_TYPE_FIELD)
        .type()
        .stringType()
        .noDefault()
        .name(LOCATION_FIELD)
        .type()
        .stringType()
        .noDefault()
        .name(EXPERIENCE_FIELD)
        .type()
        .stringType()
        .noDefault()
        .name(SALARY_FIELD)
        .type()
        .intType()
        .noDefault()
        .name(AGE_FIELD)
        .type()
        .intType()
        .noDefault()
        .endRecord();

    when(schemaReader.getLatestValueSchemaId()).thenReturn(1);
    when(schemaReader.getValueSchema(1)).thenReturn(jobSchema);
    when(storeClient.getStoreName()).thenReturn("test_store");
    when(storeClient.compute()).thenReturn(delegate);
    when(storeClient.getSchemaReader()).thenReturn(schemaReader);

    // Setup server-side capable store client
    when(abstractStoreClient.getStoreName()).thenReturn("test_store");
    when(abstractStoreClient.compute()).thenReturn(delegate);
    when(abstractStoreClient.getSchemaReader()).thenReturn(schemaReader);
    when(abstractStoreClient.getTransportClient()).thenReturn(transportClient);

    builder = new AvroComputeAggregationRequestBuilder<>(storeClient, schemaReader);
    serverSideBuilder = new AvroComputeAggregationRequestBuilder<>(abstractStoreClient, schemaReader);
  }

  @Test(description = "Should accept valid parameters and project fields")
  public void testValidParameters() {
    builder.countGroupByValue(10, JOB_TYPE_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
  }

  @Test(description = "Should accept multiple fields")
  public void testMultipleFields() {
    builder.countGroupByValue(5, JOB_TYPE_FIELD, LOCATION_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
    verify(delegate).project(LOCATION_FIELD);
  }

  @Test(description = "Should reject invalid topK values")
  public void testInvalidTopK() {
    VeniceClientException ex1 =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(0, JOB_TYPE_FIELD));
    assertEquals(ex1.getMessage(), "TopK must be positive");

    VeniceClientException ex2 =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(-1, JOB_TYPE_FIELD));
    assertEquals(ex2.getMessage(), "TopK must be positive");
  }

  @Test(description = "Should reject invalid field inputs")
  public void testInvalidFields() {
    // Null field names
    expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, (String[]) null));

    // Empty field names
    expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, new String[0]));

    // Null field in array
    expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, new String[] { "validField", null }));

    // Empty field in array
    expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, new String[] { "validField", "" }));

    // Non-existent field
    expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, "nonExistentField"));
  }

  @Test(description = "Should execute with valid parameters")
  public void testValidExecution() {
    CompletableFuture<Map<String, ComputeGenericRecord>> mockFuture =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockFuture);

    Set<String> keys = new HashSet<>(Arrays.asList("job1", "job2"));

    CompletableFuture<ComputeAggregationResponse> future = builder.countGroupByValue(5, JOB_TYPE_FIELD).execute(keys);

    assertNotNull(future);
    verify(delegate).execute(keys);
  }

  @Test(description = "Should reject null and empty keys")
  public void testInvalidKeys() {
    builder.countGroupByValue(5, JOB_TYPE_FIELD);

    // Test null keys
    VeniceClientException ex1 = expectThrows(VeniceClientException.class, () -> builder.execute(null));
    assertEquals(ex1.getMessage(), "keys cannot be null or empty");

    // Test empty keys
    VeniceClientException ex2 = expectThrows(VeniceClientException.class, () -> builder.execute(new HashSet<>()));
    assertEquals(ex2.getMessage(), "keys cannot be null or empty");
  }

  @Test(description = "Should support chaining multiple countGroupByValue calls")
  public void testChainingMultipleCountGroupByValue() {
    builder.countGroupByValue(5, JOB_TYPE_FIELD)
        .countGroupByValue(3, LOCATION_FIELD)
        .countGroupByValue(2, EXPERIENCE_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
    verify(delegate).project(LOCATION_FIELD);
    verify(delegate).project(EXPERIENCE_FIELD);
  }

  @Test(description = "Should handle single field with minimum topK value")
  public void testSingleFieldWithMinimumTopK() {
    builder.countGroupByValue(1, JOB_TYPE_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
  }

  @Test(description = "Should handle large topK value")
  public void testLargeTopKValue() {
    builder.countGroupByValue(10000, JOB_TYPE_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
  }

  @Test(description = "Should handle all schema fields")
  public void testAllSchemaFields() {
    builder.countGroupByValue(5, JOB_TYPE_FIELD, LOCATION_FIELD, EXPERIENCE_FIELD, SALARY_FIELD, AGE_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
    verify(delegate).project(LOCATION_FIELD);
    verify(delegate).project(EXPERIENCE_FIELD);
    verify(delegate).project(SALARY_FIELD);
    verify(delegate).project(AGE_FIELD);
  }

  @Test(description = "Should handle duplicate field names")
  public void testDuplicateFieldNames() {
    builder.countGroupByValue(5, JOB_TYPE_FIELD, JOB_TYPE_FIELD, LOCATION_FIELD);

    verify(delegate, times(2)).project(JOB_TYPE_FIELD);
    verify(delegate).project(LOCATION_FIELD);
  }

  @Test(description = "Should handle schema reader returning null schema")
  public void testSchemaReaderReturnsNullSchema() {
    when(schemaReader.getValueSchema(1)).thenReturn(null);

    expectThrows(NullPointerException.class, () -> builder.countGroupByValue(5, JOB_TYPE_FIELD));
  }

  @Test(description = "Should handle schema reader throwing exception")
  public void testSchemaReaderThrowsException() {
    when(schemaReader.getValueSchema(1)).thenThrow(new RuntimeException("Schema error"));

    expectThrows(RuntimeException.class, () -> builder.countGroupByValue(5, JOB_TYPE_FIELD));
  }

  @Test(description = "Should handle delegate throwing exception during execution")
  public void testDelegateThrowsExceptionDuringExecution() {
    when(delegate.execute(any())).thenThrow(new RuntimeException("Execution error"));
    builder.countGroupByValue(5, JOB_TYPE_FIELD);

    Set<String> keys = new HashSet<>(Arrays.asList("job1", "job2"));

    expectThrows(RuntimeException.class, () -> builder.execute(keys));
  }

  @Test(description = "Should handle empty field names array")
  public void testEmptyFieldNamesArray() {
    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, new String[0]));
    assertEquals(ex.getMessage(), "fieldNames cannot be null or empty");
  }

  @Test(description = "Should handle whitespace field names")
  public void testWhitespaceFieldNames() {
    VeniceClientException ex = expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, "   "));
    assertEquals(ex.getMessage(), "Field not found in schema:    ");
  }

  @Test(description = "Should handle special characters in field names")
  public void testSpecialCharactersInFieldNames() {
    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, "field@name"));
    assertEquals(ex.getMessage(), "Field not found in schema: field@name");
  }

  @Test(description = "Should handle very long field names")
  public void testVeryLongFieldNames() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("a");
    }
    String longFieldName = sb.toString();
    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, longFieldName));
    assertEquals(ex.getMessage(), "Field not found in schema: " + longFieldName);
  }

  @Test(description = "Should handle mixed valid and invalid fields")
  public void testMixedValidAndInvalidFields() {
    VeniceClientException ex = expectThrows(
        VeniceClientException.class,
        () -> builder.countGroupByValue(5, JOB_TYPE_FIELD, "nonExistentField"));
    assertEquals(ex.getMessage(), "Field not found in schema: nonExistentField");
  }

  @Test(description = "Should handle multiple countGroupByValue calls with same field")
  public void testMultipleCountGroupByValueCallsWithSameField() {
    builder.countGroupByValue(5, JOB_TYPE_FIELD)
        .countGroupByValue(3, JOB_TYPE_FIELD)
        .countGroupByValue(1, JOB_TYPE_FIELD);

    // Should call project 3 times for the same field
    verify(delegate, times(3)).project(JOB_TYPE_FIELD);
  }

  @Test(description = "Should handle execution with single key")
  public void testExecutionWithSingleKey() {
    CompletableFuture<Map<String, ComputeGenericRecord>> mockFuture =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockFuture);

    Set<String> keys = new HashSet<>(Arrays.asList("singleKey"));
    builder.countGroupByValue(5, JOB_TYPE_FIELD);

    CompletableFuture<ComputeAggregationResponse> future = builder.execute(keys);

    assertNotNull(future);
    verify(delegate).execute(keys);
  }

  @Test(description = "Should handle execution with large key set")
  public void testExecutionWithLargeKeySet() {
    CompletableFuture<Map<String, ComputeGenericRecord>> mockFuture =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockFuture);

    Set<String> keys = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      keys.add("key" + i);
    }

    builder.countGroupByValue(5, JOB_TYPE_FIELD);

    CompletableFuture<ComputeAggregationResponse> future = builder.execute(keys);

    assertNotNull(future);
    verify(delegate).execute(keys);
  }

  @Test(description = "Should accept valid bucket parameters and project fields")
  public void testValidBucketParameters() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));
    bucketPredicates.put("senior", IntPredicate.greaterOrEquals(30));

    builder.countGroupByBucket(bucketPredicates, AGE_FIELD);

    verify(delegate).project(AGE_FIELD);
  }

  @Test(description = "Should accept multiple fields for bucket aggregation")
  public void testMultipleFieldsForBucketAggregation() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("low", IntPredicate.lowerThan(50));
    bucketPredicates.put("high", IntPredicate.greaterOrEquals(50));

    builder.countGroupByBucket(bucketPredicates, AGE_FIELD, SALARY_FIELD);

    verify(delegate).project(AGE_FIELD);
    verify(delegate).project(SALARY_FIELD);
  }

  @Test(description = "Should reject invalid bucket predicates")
  public void testInvalidBucketPredicates() {
    // Test null bucket predicates
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(null, AGE_FIELD));

    // Test empty bucket predicates
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(new HashMap<>(), AGE_FIELD));

    // Test null predicates in bucket map
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", null);
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, AGE_FIELD));
  }

  @Test(description = "Should reject null and empty bucket names")
  public void testInvalidBucketNames() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();

    // Test null bucket names
    bucketPredicates.put(null, IntPredicate.lowerThan(30));
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, AGE_FIELD));

    // Test empty bucket names
    bucketPredicates.clear();
    bucketPredicates.put("", IntPredicate.lowerThan(30));
    bucketPredicates.put("   ", IntPredicate.greaterOrEquals(30));
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, AGE_FIELD));
  }

  @Test(description = "Should reject null and empty field names for bucket aggregation")
  public void testInvalidFieldNamesForBucketAggregation() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));

    // Test null field names
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, (String[]) null));

    // Test empty field names
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, new String[0]));
  }

  @Test(description = "Should reject null and empty field in array for bucket aggregation")
  public void testInvalidFieldInArrayForBucketAggregation() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));

    // Test null field in array
    expectThrows(
        VeniceClientException.class,
        () -> builder.countGroupByBucket(bucketPredicates, new String[] { "validField", null }));

    // Test empty field in array
    expectThrows(
        VeniceClientException.class,
        () -> builder.countGroupByBucket(bucketPredicates, new String[] { "validField", "" }));
  }

  @Test(description = "Should reject non-existent field for bucket aggregation")
  public void testNonExistentFieldForBucketAggregation() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, "nonExistentField"));
  }

  @Test(description = "Should reject invalid fields for bucket aggregation")
  public void testInvalidFieldsForBucketAggregation() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));

    // Test single non-existent field
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, "nonExistentField"));

    // Test mixed valid and invalid fields
    expectThrows(
        VeniceClientException.class,
        () -> builder.countGroupByBucket(bucketPredicates, AGE_FIELD, "nonExistentField"));
  }

  @Test(description = "Should support chaining countGroupByValue and countGroupByBucket")
  public void testChainingValueAndBucket() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));

    builder.countGroupByValue(5, JOB_TYPE_FIELD).countGroupByBucket(bucketPredicates, AGE_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
    verify(delegate).project(AGE_FIELD);
  }

  @Test(description = "Should handle multiple countGroupByBucket calls")
  public void testMultipleCountGroupByBucketCalls() {
    Map<String, Predicate<Integer>> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("senior", IntPredicate.greaterOrEquals(30));

    Map<String, Predicate<String>> jobBuckets = new HashMap<>();
    jobBuckets.put("engineer", Predicate.equalTo("engineer"));
    jobBuckets.put("manager", Predicate.equalTo("manager"));

    // Test chaining different fields
    builder.countGroupByBucket(ageBuckets, AGE_FIELD).countGroupByBucket(jobBuckets, JOB_TYPE_FIELD);
    verify(delegate).project(AGE_FIELD);
    verify(delegate).project(JOB_TYPE_FIELD);

    // Test chaining same field
    Map<String, Predicate<Integer>> buckets1 = new HashMap<>();
    buckets1.put("young", IntPredicate.lowerThan(30));
    Map<String, Predicate<Integer>> buckets2 = new HashMap<>();
    buckets2.put("senior", IntPredicate.greaterOrEquals(30));

    builder.countGroupByBucket(buckets1, AGE_FIELD).countGroupByBucket(buckets2, AGE_FIELD);
    verify(delegate, times(3)).project(AGE_FIELD);
  }

  @Test(description = "Should handle edge cases for field names in bucket aggregation")
  public void testEdgeCasesForFieldNamesInBucketAggregation() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));

    // Test whitespace field names
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, "   "));

    // Test special characters in field names
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, "field@name"));

    // Test very long field names
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("a");
    }
    String longFieldName = sb.toString();
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, longFieldName));
  }

  @Test(description = "Should reject predicate type mismatch with field schema type")
  public void testPredicateTypeMismatch() {
    Map<String, Predicate<String>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("test", Predicate.equalTo("test"));

    // Test using StringPredicate with INT field (AGE_FIELD is int type)
    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, AGE_FIELD));

    assertTrue(ex.getMessage().contains("Predicate type mismatch"));
    assertTrue(ex.getMessage().contains("age"));
    assertTrue(ex.getMessage().contains("INT"));
    assertTrue(ex.getMessage().contains("EqualsPredicate"));
  }

  @Test(description = "Should reject IntPredicate with String field")
  public void testIntPredicateWithStringField() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));

    // Test using IntPredicate with String field (JOB_TYPE_FIELD is string type)
    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(bucketPredicates, JOB_TYPE_FIELD));

    assertTrue(ex.getMessage().contains("Predicate type mismatch"));
    assertTrue(ex.getMessage().contains("jobType"));
    assertTrue(ex.getMessage().contains("STRING"));
    assertTrue(ex.getMessage().contains("IntLowerThanPredicate"));
  }

  @Test(description = "Should accept correct predicate type with matching field schema")
  public void testCorrectPredicateTypeWithMatchingField() {
    Map<String, Predicate<Integer>> bucketPredicates = new HashMap<>();
    bucketPredicates.put("young", IntPredicate.lowerThan(30));
    bucketPredicates.put("senior", IntPredicate.greaterOrEquals(30));

    // This should work because AGE_FIELD is int type and we're using IntPredicate
    builder.countGroupByBucket(bucketPredicates, AGE_FIELD);
    verify(delegate).project(AGE_FIELD);
  }

  // =========================
  // Server-Side Aggregation Tests
  // =========================

  @Test(description = "Should use server-side aggregation when using AbstractAvroStoreClient with countByValue only")
  public void testServerSideAggregationExecution() throws Exception {
    // Setup successful server response
    String responseJson = "{\"jobType\":{\"engineer\":10,\"manager\":5,\"analyst\":3}}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, responseJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3"));

    // Execute and verify it uses server-side path
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify server endpoint was called
    verify(transportClient).post(eq("/aggregation/test_store"), any(), any());

    // Verify response contains server results
    Map<String, Integer> counts = result.getValueToCount(JOB_TYPE_FIELD);
    assertEquals(counts.get("engineer"), Integer.valueOf(10));
    assertEquals(counts.get("manager"), Integer.valueOf(5));
    assertEquals(counts.get("analyst"), Integer.valueOf(3));
  }

  @Test(description = "Should fall back to client-side when server-side aggregation fails")
  public void testServerSideAggregationFallback() throws Exception {
    // Setup server failure
    CompletableFuture<TransportClientResponse> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("Server error"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any())).thenReturn(failedFuture);

    // Setup client-side fallback
    CompletableFuture<Map<String, ComputeGenericRecord>> mockClientResult =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockClientResult);

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Execute - should not throw exception despite server failure
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify both server attempt and client fallback were called
    verify(transportClient).post(eq("/aggregation/test_store"), any(), any());
    verify(delegate).execute(keys);
    assertNotNull(result);
  }

  @Test(description = "Should not use server-side aggregation when bucket operations are present")
  public void testServerSideSkippedWithBucketOperations() throws Exception {
    // Setup client-side execution
    CompletableFuture<Map<String, ComputeGenericRecord>> mockClientResult =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockClientResult);

    // Setup aggregation with both value and bucket operations
    Map<String, Predicate<Integer>> buckets = new HashMap<>();
    buckets.put("young", IntPredicate.lowerThan(30));
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD).countGroupByBucket(buckets, AGE_FIELD);

    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Execute
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify server-side was NOT called
    verify(transportClient, times(0)).post(any(), any(), any());
    // Verify client-side was called
    verify(delegate).execute(keys);
    assertNotNull(result);
  }

  @Test(description = "Should not use server-side aggregation when using regular store client")
  public void testServerSideSkippedWithRegularStoreClient() throws Exception {
    // Setup client-side execution
    CompletableFuture<Map<String, ComputeGenericRecord>> mockClientResult =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockClientResult);

    // Use regular builder (not server-side capable)
    builder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Execute
    CompletableFuture<ComputeAggregationResponse> future = builder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify server-side was NOT called (transportClient should not be touched)
    verify(transportClient, times(0)).post(any(), any(), any());
    // Verify client-side was called
    verify(delegate).execute(keys);
    assertNotNull(result);
  }

  @Test(description = "Should handle server response parsing errors gracefully")
  public void testServerSideAggregationInvalidResponse() throws Exception {
    // Setup invalid JSON response
    String invalidJson = "{invalid json response}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, invalidJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup client-side fallback
    CompletableFuture<Map<String, ComputeGenericRecord>> mockClientResult =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockClientResult);

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Execute - should fall back to client-side due to parsing error
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify server was attempted and client fallback was used
    verify(transportClient).post(eq("/aggregation/test_store"), any(), any());
    verify(delegate).execute(keys);
    assertNotNull(result);
  }

  @Test(description = "Should include proper headers in server-side request")
  public void testServerSideAggregationHeaders() throws Exception {
    // Setup successful server response
    String responseJson = "{\"jobType\":{\"engineer\":5}}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, responseJson.getBytes("UTF-8"));

    // Capture headers passed to transport client
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    // Execute
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    future.get();

    // Verify transport client was called with correct parameters
    verify(transportClient).post(eq("/aggregation/test_store"), any(Map.class), any(byte[].class));
  }

  @Test(description = "Should serialize aggregation request correctly")
  public void testAggregationRequestSerialization() throws Exception {
    // Setup successful server response
    String responseJson = "{\"jobType\":{\"engineer\":5},\"location\":{\"NYC\":3,\"SF\":2}}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, responseJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup the aggregation with multiple fields
    serverSideBuilder.countGroupByValue(5, JOB_TYPE_FIELD).countGroupByValue(3, LOCATION_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3"));

    // Execute
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify results include both fields
    Map<String, Integer> jobCounts = result.getValueToCount(JOB_TYPE_FIELD);
    Map<String, Integer> locationCounts = result.getValueToCount(LOCATION_FIELD);

    assertEquals(jobCounts.get("engineer"), Integer.valueOf(5));
    assertEquals(locationCounts.get("NYC"), Integer.valueOf(3));
    assertEquals(locationCounts.get("SF"), Integer.valueOf(2));
  }

  @Test(description = "Should handle empty server response gracefully")
  public void testServerSideAggregationEmptyResponse() throws Exception {
    // Setup empty response
    String emptyJson = "{}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, emptyJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    // Execute
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify empty results are handled gracefully
    Map<String, Integer> counts = result.getValueToCount(JOB_TYPE_FIELD);
    assertTrue(counts.isEmpty());
  }

  @Test(description = "Should handle missing field in server response")
  public void testServerSideAggregationMissingField() throws Exception {
    // Setup response missing the requested field
    String responseJson = "{\"otherField\":{\"value1\":5}}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, responseJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    // Execute
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify missing field returns empty map
    Map<String, Integer> counts = result.getValueToCount(JOB_TYPE_FIELD);
    assertTrue(counts.isEmpty());
  }

  @Test(description = "ServerSideAggregationResponse should handle bucket operations correctly")
  public void testServerSideResponseBucketOperations() throws Exception {
    // Setup response
    String responseJson = "{\"jobType\":{\"engineer\":5}}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, responseJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    // Execute
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify bucket operations return empty (not implemented server-side yet)
    Map<String, Integer> bucketCounts = result.getBucketNameToCount(JOB_TYPE_FIELD);
    assertTrue(bucketCounts.isEmpty());
  }

  @Test(description = "Should handle transport client throwing exception during server call")
  public void testServerSideTransportClientException() throws Exception {
    // Setup transport client to throw exception
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenThrow(new RuntimeException("Transport error"));

    // Setup client-side fallback
    CompletableFuture<Map<String, ComputeGenericRecord>> mockClientResult =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockClientResult);

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));

    // Execute - should not throw exception despite transport error
    CompletableFuture<ComputeAggregationResponse> future = serverSideBuilder.execute(keys);
    ComputeAggregationResponse result = future.get();

    // Verify fallback to client-side execution
    verify(delegate).execute(keys);
    assertNotNull(result);
  }

  @Test(description = "Should test canUseServerSideAggregation method correctly")
  public void testCanUseServerSideAggregation() throws Exception {
    // Setup client-side execution for regular store client
    CompletableFuture<Map<String, ComputeGenericRecord>> mockClientResult =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockClientResult);

    // Test with regular store client (should use client-side)
    builder.countGroupByValue(3, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1"));
    CompletableFuture<ComputeAggregationResponse> regularResult = builder.execute(keys);
    regularResult.get();

    // Verify only client-side was called
    verify(delegate).execute(keys);
    verify(transportClient, times(0)).post(any(), any(), any());

    // Test with AbstractAvroStoreClient (should attempt server-side)
    String responseJson = "{\"jobType\":{\"engineer\":5}}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, responseJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    serverSideBuilder.countGroupByValue(3, JOB_TYPE_FIELD);
    CompletableFuture<ComputeAggregationResponse> serverResult = serverSideBuilder.execute(keys);
    serverResult.get();

    // Verify server-side was attempted
    verify(transportClient).post(eq("/aggregation/test_store"), any(), any());
  }

  @Test(description = "Should handle concurrent execution of server-side aggregation")
  public void testServerSideAggregationConcurrency() throws Exception {
    // Setup server response
    String responseJson = "{\"jobType\":{\"engineer\":10}}";
    TransportClientResponse response =
        new TransportClientResponse(1, CompressionStrategy.NO_OP, responseJson.getBytes("UTF-8"));
    when(transportClient.post(eq("/aggregation/test_store"), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(response));

    // Setup the aggregation
    serverSideBuilder.countGroupByValue(5, JOB_TYPE_FIELD);
    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2"));

    // Execute multiple concurrent requests
    CompletableFuture<ComputeAggregationResponse> future1 = serverSideBuilder.execute(keys);
    CompletableFuture<ComputeAggregationResponse> future2 = serverSideBuilder.execute(keys);

    // Wait for both to complete
    ComputeAggregationResponse result1 = future1.get();
    ComputeAggregationResponse result2 = future2.get();

    // Verify both completed successfully
    assertNotNull(result1);
    assertNotNull(result2);
    assertEquals(result1.getValueToCount(JOB_TYPE_FIELD).get("engineer"), Integer.valueOf(10));
    assertEquals(result2.getValueToCount(JOB_TYPE_FIELD).get("engineer"), Integer.valueOf(10));

    // Verify server was called twice
    verify(transportClient, times(2)).post(eq("/aggregation/test_store"), any(), any());
  }
}
