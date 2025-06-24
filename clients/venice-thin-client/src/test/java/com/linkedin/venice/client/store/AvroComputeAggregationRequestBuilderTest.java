package com.linkedin.venice.client.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
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
 * Test suite for {@link AvroComputeAggregationRequestBuilder}.
 * 
 * Tests the aggregation request builder functionality for constructing compute requests
 * that perform value counting and bucket aggregation operations on Avro records.
 * 
 * Key Areas:
 * - Value counting (countGroupByValue) with topK limits and field validation
 * - Bucket aggregation (countGroupByBucket) with various predicate types
 * - Input validation for fields, predicates, and parameters
 * - Error handling for invalid schemas and missing fields
 * - Edge cases including special characters and boundary conditions
 * 
 * @see AvroComputeAggregationRequestBuilder
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

    Set<String> keys = createSimpleKeySet();

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

    Set<String> keys = createSimpleKeySet();

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
    String longFieldName = createVeryLongFieldName();
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

    Set<String> keys = createSingleKeySet();
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

    Set<String> keys = createLargeKeySet();

    builder.countGroupByValue(5, JOB_TYPE_FIELD);

    CompletableFuture<ComputeAggregationResponse> future = builder.execute(keys);

    assertNotNull(future);
    verify(delegate).execute(keys);
  }

  @Test(description = "Should accept valid bucket parameters and project fields")
  public void testValidBucketParameters() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    builder.countGroupByBucket(ageBuckets, AGE_FIELD);

    verify(delegate).project(AGE_FIELD);
  }

  @Test(description = "Should accept multiple fields for bucket aggregation")
  public void testMultipleFieldsForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    builder.countGroupByBucket(ageBuckets, AGE_FIELD, SALARY_FIELD);

    verify(delegate).project(AGE_FIELD);
    verify(delegate).project(SALARY_FIELD);
  }

  @Test(description = "Should reject null bucket predicates")
  public void testNullBucketPredicates() {
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(null, AGE_FIELD));
  }

  @Test(description = "Should reject empty bucket predicates")
  public void testEmptyBucketPredicates() {
    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(new HashMap<>(), AGE_FIELD));
  }

  @Test(description = "Should reject null bucket names")
  public void testNullBucketNames() {
    Map<String, Predicate<Integer>> buckets = createBucketsWithNullName();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, AGE_FIELD));
  }

  @Test(description = "Should reject empty bucket names")
  public void testEmptyBucketNames() {
    Map<String, Predicate<Integer>> buckets = createBucketsWithEmptyName();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, AGE_FIELD));
  }

  @Test(description = "Should reject null predicates")
  public void testNullPredicates() {
    Map<String, Predicate<Integer>> buckets = createBucketsWithNullPredicate();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, AGE_FIELD));
  }

  @Test(description = "Should reject null field names for bucket aggregation")
  public void testNullFieldNamesForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(ageBuckets, (String[]) null));
  }

  @Test(description = "Should reject empty field names for bucket aggregation")
  public void testEmptyFieldNamesForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(ageBuckets, new String[0]));
  }

  @Test(description = "Should reject null field in array for bucket aggregation")
  public void testNullFieldInArrayForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(
        VeniceClientException.class,
        () -> builder.countGroupByBucket(ageBuckets, new String[] { "validField", null }));
  }

  @Test(description = "Should reject empty field in array for bucket aggregation")
  public void testEmptyFieldInArrayForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(
        VeniceClientException.class,
        () -> builder.countGroupByBucket(ageBuckets, new String[] { "validField", "" }));
  }

  @Test(description = "Should reject non-existent field for bucket aggregation")
  public void testNonExistentFieldForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(ageBuckets, "nonExistentField"));
  }

  @Test(description = "Should support chaining countGroupByValue and countGroupByBucket")
  public void testChainingValueAndBucket() {
    Map<String, Predicate<Integer>> ageBuckets = createComplexAgeBuckets();

    builder.countGroupByValue(5, JOB_TYPE_FIELD).countGroupByBucket(ageBuckets, AGE_FIELD);

    verify(delegate).project(JOB_TYPE_FIELD);
    verify(delegate).project(AGE_FIELD);
  }

  @Test(description = "Should support chaining multiple countGroupByBucket calls")
  public void testChainingMultipleCountGroupByBucket() {
    Map<String, Predicate<Integer>> ageBuckets = createComplexAgeBuckets();
    Map<String, Predicate<Integer>> salaryBuckets = createSalaryBuckets();

    builder.countGroupByBucket(ageBuckets, AGE_FIELD).countGroupByBucket(salaryBuckets, SALARY_FIELD);

    verify(delegate).project(AGE_FIELD);
    verify(delegate).project(SALARY_FIELD);
  }

  @Test(description = "Should handle multiple countGroupByBucket calls with same field")
  public void testMultipleCountGroupByBucketCallsWithSameField() {
    Map<String, Predicate<Integer>> ageBuckets1 = createSimpleAgeBuckets();
    Map<String, Predicate<Integer>> ageBuckets2 = createSeniorAgeBuckets();

    builder.countGroupByBucket(ageBuckets1, AGE_FIELD).countGroupByBucket(ageBuckets2, AGE_FIELD);

    // Should call project 2 times for the same field
    verify(delegate, times(2)).project(AGE_FIELD);
  }

  @Test(description = "Should execute with valid bucket parameters")
  public void testValidBucketExecution() {
    CompletableFuture<Map<String, ComputeGenericRecord>> mockFuture =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockFuture);

    Map<String, Predicate<Integer>> ageBuckets = createComplexAgeBuckets();

    Set<String> keys = createSimpleKeySet();

    CompletableFuture<ComputeAggregationResponse> future =
        builder.countGroupByBucket(ageBuckets, AGE_FIELD).execute(keys);

    assertNotNull(future);
    verify(delegate).execute(keys);
  }

  @Test(description = "Should handle complex bucket predicates")
  public void testComplexBucketPredicates() {
    Map<String, Predicate<Integer>> complexBuckets = createComplexBucketPredicates();

    builder.countGroupByBucket(complexBuckets, AGE_FIELD);

    verify(delegate).project(AGE_FIELD);
  }

  @Test(description = "Should handle mixed valid and invalid fields for bucket aggregation")
  public void testMixedValidAndInvalidFieldsForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(
        VeniceClientException.class,
        () -> builder.countGroupByBucket(ageBuckets, AGE_FIELD, "nonExistentField"));
  }

  @Test(description = "Should handle whitespace field names for bucket aggregation")
  public void testWhitespaceFieldNamesForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(ageBuckets, "   "));
  }

  @Test(description = "Should handle special characters in field names for bucket aggregation")
  public void testSpecialCharactersInFieldNamesForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(ageBuckets, "field@name"));
  }

  @Test(description = "Should handle very long field names for bucket aggregation")
  public void testVeryLongFieldNamesForBucketAggregation() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();
    String longFieldName = createVeryLongFieldName();

    expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(ageBuckets, longFieldName));
  }

  @Test(description = "Should handle LongPredicate correctly")
  public void testLongPredicateHandling() {
    Map<String, Predicate<Long>> salaryBuckets = createLongSalaryBuckets();
    builder.countGroupByBucket(salaryBuckets, SALARY_FIELD);
    verify(delegate).project(SALARY_FIELD);
  }

  @Test(description = "Should handle FloatPredicate correctly")
  public void testFloatPredicateHandling() {
    Map<String, Predicate<Float>> salaryBuckets = createFloatSalaryBuckets();
    builder.countGroupByBucket(salaryBuckets, SALARY_FIELD);
    verify(delegate).project(SALARY_FIELD);
  }

  @Test(description = "Should handle DoublePredicate correctly")
  public void testDoublePredicateHandling() {
    Map<String, Predicate<Double>> salaryBuckets = createDoubleSalaryBuckets();
    builder.countGroupByBucket(salaryBuckets, SALARY_FIELD);
    verify(delegate).project(SALARY_FIELD);
  }

  @Test(description = "Should handle StringPredicate correctly")
  public void testStringPredicateHandling() {
    Map<String, Predicate<String>> jobTypeBuckets = createStringJobTypeBuckets();
    builder.countGroupByBucket(jobTypeBuckets, JOB_TYPE_FIELD);
    verify(delegate).project(JOB_TYPE_FIELD);
  }

  @Test(description = "Should handle mixed predicate types")
  public void testMixedPredicateTypes() {
    Map<String, Predicate<Integer>> ageBuckets = createSimpleAgeBuckets();
    Map<String, Predicate<Float>> salaryBuckets = createFloatSalaryBuckets();
    builder.countGroupByBucket(ageBuckets, AGE_FIELD).countGroupByBucket(salaryBuckets, SALARY_FIELD);
    verify(delegate).project(AGE_FIELD);
    verify(delegate).project(SALARY_FIELD);
  }

  @Test(description = "Should handle complex predicate combinations")
  public void testComplexPredicateCombinations() {
    Map<String, Predicate<Integer>> ageBuckets = createComplexAgeBuckets();
    Map<String, Predicate<String>> jobTypeBuckets = createStringJobTypeBuckets();
    builder.countGroupByBucket(ageBuckets, AGE_FIELD).countGroupByBucket(jobTypeBuckets, JOB_TYPE_FIELD);
    verify(delegate).project(AGE_FIELD);
    verify(delegate).project(JOB_TYPE_FIELD);
  }

  @Test(description = "Should handle edge cases for different predicate types")
  public void testEdgeCasesForDifferentPredicateTypes() {
    Map<String, Predicate<Integer>> intBuckets = createEdgeCaseIntBuckets();
    Map<String, Predicate<Long>> longBuckets = createEdgeCaseLongBuckets();
    Map<String, Predicate<Float>> floatBuckets = createEdgeCaseFloatBuckets();
    builder.countGroupByBucket(intBuckets, AGE_FIELD)
        .countGroupByBucket(longBuckets, SALARY_FIELD)
        .countGroupByBucket(floatBuckets, SALARY_FIELD);
    verify(delegate).project(AGE_FIELD);
    verify(delegate, times(2)).project(SALARY_FIELD);
  }

  // Helper methods for creating test data
  private Set<String> createSimpleKeySet() {
    return new HashSet<>(Arrays.asList("job1", "job2"));
  }

  private Set<String> createSingleKeySet() {
    return new HashSet<>(Arrays.asList("singleKey"));
  }

  private Set<String> createLargeKeySet() {
    Set<String> keys = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      keys.add("key" + i);
    }
    return keys;
  }

  private String createVeryLongFieldName() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      sb.append("a");
    }
    return sb.toString();
  }

  private Map<String, Predicate<Integer>> createSimpleAgeBuckets() {
    Map<String, Predicate<Integer>> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("senior", IntPredicate.greaterThan(50));
    return ageBuckets;
  }

  private Map<String, Predicate<Integer>> createComplexAgeBuckets() {
    Map<String, Predicate<Integer>> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("senior", IntPredicate.greaterThan(50));
    return ageBuckets;
  }

  private Map<String, Predicate<Integer>> createSeniorAgeBuckets() {
    Map<String, Predicate<Integer>> ageBuckets = new HashMap<>();
    ageBuckets.put("senior", IntPredicate.greaterThan(50));
    return ageBuckets;
  }

  private Map<String, Predicate<Integer>> createSalaryBuckets() {
    Map<String, Predicate<Integer>> salaryBuckets = new HashMap<>();
    salaryBuckets.put("low", IntPredicate.lowerThan(50000));
    salaryBuckets.put("high", IntPredicate.greaterThan(80000));
    return salaryBuckets;
  }

  private Map<String, Predicate<Integer>> createComplexBucketPredicates() {
    Map<String, Predicate<Integer>> complexBuckets = new HashMap<>();
    complexBuckets.put("junior", IntPredicate.lowerThan(25));
    complexBuckets.put("mid_level", IntPredicate.greaterOrEquals(25));
    complexBuckets.put("senior", IntPredicate.greaterThan(40));
    complexBuckets.put("executive", IntPredicate.greaterThan(50));
    return complexBuckets;
  }

  private Map<String, Predicate<Integer>> createBucketsWithNullName() {
    Map<String, Predicate<Integer>> buckets = new HashMap<>();
    buckets.put(null, IntPredicate.greaterThan(30));
    return buckets;
  }

  private Map<String, Predicate<Integer>> createBucketsWithEmptyName() {
    Map<String, Predicate<Integer>> buckets = new HashMap<>();
    buckets.put("", IntPredicate.greaterThan(30));
    return buckets;
  }

  private Map<String, Predicate<Integer>> createBucketsWithNullPredicate() {
    Map<String, Predicate<Integer>> buckets = new HashMap<>();
    buckets.put("valid", null);
    return buckets;
  }

  private Map<String, Predicate<Long>> createLongSalaryBuckets() {
    Map<String, Predicate<Long>> salaryBuckets = new HashMap<>();
    salaryBuckets.put("low", LongPredicate.lowerThan(50000L));
    salaryBuckets.put("high", LongPredicate.greaterOrEquals(50000L));
    return salaryBuckets;
  }

  private Map<String, Predicate<Float>> createFloatSalaryBuckets() {
    Map<String, Predicate<Float>> salaryBuckets = new HashMap<>();
    salaryBuckets.put("cheap", FloatPredicate.lowerThan(10000.0f));
    salaryBuckets.put("expensive", FloatPredicate.greaterOrEquals(10000.0f));
    return salaryBuckets;
  }

  private Map<String, Predicate<Double>> createDoubleSalaryBuckets() {
    Map<String, Predicate<Double>> salaryBuckets = new HashMap<>();
    salaryBuckets.put("low", DoublePredicate.lowerThan(10000.0));
    salaryBuckets.put("high", DoublePredicate.greaterOrEquals(10000.0));
    return salaryBuckets;
  }

  private Map<String, Predicate<String>> createStringJobTypeBuckets() {
    Map<String, Predicate<String>> jobTypeBuckets = new HashMap<>();
    jobTypeBuckets.put("engineer", Predicate.equalTo("engineer"));
    jobTypeBuckets.put("manager", Predicate.equalTo("manager"));
    return jobTypeBuckets;
  }

  private Map<String, Predicate<Integer>> createEdgeCaseIntBuckets() {
    Map<String, Predicate<Integer>> intBuckets = new HashMap<>();
    intBuckets.put("zero", IntPredicate.equalTo(0));
    intBuckets.put("negative", IntPredicate.lowerThan(0));
    return intBuckets;
  }

  private Map<String, Predicate<Long>> createEdgeCaseLongBuckets() {
    Map<String, Predicate<Long>> longBuckets = new HashMap<>();
    longBuckets.put("max", LongPredicate.equalTo(Long.MAX_VALUE));
    longBuckets.put("min", LongPredicate.equalTo(Long.MIN_VALUE));
    return longBuckets;
  }

  private Map<String, Predicate<Float>> createEdgeCaseFloatBuckets() {
    Map<String, Predicate<Float>> floatBuckets = new HashMap<>();
    floatBuckets.put("infinity", FloatPredicate.greaterThan(Float.MAX_VALUE / 2));
    floatBuckets.put("small", FloatPredicate.lowerThan(1.0f));
    return floatBuckets;
  }
}
