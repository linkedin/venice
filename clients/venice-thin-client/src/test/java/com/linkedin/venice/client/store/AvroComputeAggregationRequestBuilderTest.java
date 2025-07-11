package com.linkedin.venice.client.store;

import static org.mockito.ArgumentMatchers.any;
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
  private SchemaReader schemaReader;
  private AvroComputeRequestBuilderV3<String> delegate;
  private AvroComputeAggregationRequestBuilder<String> builder;
  private Schema jobSchema;

  @BeforeMethod
  public void setUp() {
    storeClient = mock(AvroGenericReadComputeStoreClient.class);
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

    builder = new AvroComputeAggregationRequestBuilder<>(storeClient, schemaReader);
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
}
