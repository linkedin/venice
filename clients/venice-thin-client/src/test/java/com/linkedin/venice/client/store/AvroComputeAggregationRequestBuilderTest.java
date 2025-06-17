package com.linkedin.venice.client.store;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Core test suite for AvroComputeAggregationRequestBuilder countGroupByValue functionality.
 * Focuses on essential validation, aggregation logic, and topK ordering tests.
 */
public class AvroComputeAggregationRequestBuilderTest {
  private static final String COMPANY_FIELD = "companies";
  private static final String SKILLS_FIELD = "skills";
  private static final String PREFERENCES_FIELD = "preferences";

  private static final String COMPANY_GOOGLE = "Google";
  private static final String COMPANY_META = "Meta";
  private static final String COMPANY_AMAZON = "Amazon";
  private static final String COMPANY_MICROSOFT = "Microsoft";

  private static final String SKILL_JAVA = "Java";
  private static final String SKILL_PYTHON = "Python";
  private static final String SKILL_JAVASCRIPT = "JavaScript";
  private static final String SKILL_GO = "Go";

  private static final String TIMESTAMP_FIELD = "reportTimestamp";
  private static final String AGE_FIELD = "age";
  private static final String SCORE_FIELD = "score";

  private AvroGenericReadComputeStoreClient<String, Object> storeClient;
  private SchemaReader schemaReader;
  private AvroComputeRequestBuilderV3<String> delegate;
  private AvroComputeAggregationRequestBuilder<String> builder;
  private Schema valueSchema;

  @BeforeMethod
  public void setUp() {
    storeClient = mock(AvroGenericReadComputeStoreClient.class);
    schemaReader = mock(SchemaReader.class);
    delegate = mock(AvroComputeRequestBuilderV3.class);

    setupTestSchema();
    setupMockBehaviors();

    builder = new AvroComputeAggregationRequestBuilder<>(storeClient, schemaReader);
  }

  private void setupTestSchema() {
    valueSchema = SchemaBuilder.record("UserProfile")
        .fields()
        .name(COMPANY_FIELD)
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .name(SKILLS_FIELD)
        .type()
        .array()
        .items()
        .stringType()
        .noDefault()
        .name(PREFERENCES_FIELD)
        .type()
        .map()
        .values()
        .stringType()
        .noDefault()
        .name("age")
        .type()
        .intType()
        .noDefault() // Non-collection field for validation test
        .name(TIMESTAMP_FIELD)
        .type()
        .longType()
        .noDefault()
        .name(SCORE_FIELD)
        .type()
        .doubleType()
        .noDefault()
        .endRecord();
  }

  private void setupMockBehaviors() {
    when(schemaReader.getLatestValueSchemaId()).thenReturn(1);
    when(schemaReader.getValueSchema(1)).thenReturn(valueSchema);
    when(storeClient.getStoreName()).thenReturn("test_store");
    when(storeClient.compute()).thenReturn(delegate);
    when(storeClient.getSchemaReader()).thenReturn(schemaReader);
  }

  // --- Core Validation Tests ---
  @Test(description = "Should accept valid parameters and project fields")
  public void testValidParameters() {
    ComputeAggregationRequestBuilder<String> result = builder.countGroupByValue(10, COMPANY_FIELD);

    assertNotNull(result);
    verify(delegate).project(COMPANY_FIELD);
  }

  @Test(description = "Should accept multiple fields")
  public void testMultipleFields() {
    builder.countGroupByValue(5, COMPANY_FIELD, SKILLS_FIELD);

    verify(delegate).project(COMPANY_FIELD);
    verify(delegate).project(SKILLS_FIELD);
  }

  @Test(description = "Should reject invalid topK values")
  public void testInvalidTopK() {
    VeniceClientException ex1 =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(0, COMPANY_FIELD));
    assertTrue(ex1.getMessage().contains("TopK must be positive"));

    VeniceClientException ex2 =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(-1, COMPANY_FIELD));
    assertTrue(ex2.getMessage().contains("TopK must be positive"));
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

    // Non-collection field
    expectThrows(VeniceClientException.class, () -> builder.countGroupByValue(5, "age"));
  }

  // --- Value Counting Tests ---
  @Test(description = "Should correctly count values in array fields")
  public void testCountValuesInArrayField() {
    Map<String, ComputeGenericRecord> computeResults = createArrayTestData();

    Map<String, Integer> fieldTopKMap = Collections.singletonMap(COMPANY_FIELD, 10);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(COMPANY_FIELD);

    // Expected counts: Google=4, Meta=2, Amazon=2, Microsoft=1
    assertEquals(result.get(COMPANY_GOOGLE), Integer.valueOf(4));
    assertEquals(result.get(COMPANY_META), Integer.valueOf(2));
    assertEquals(result.get(COMPANY_AMAZON), Integer.valueOf(2));
    assertEquals(result.get(COMPANY_MICROSOFT), Integer.valueOf(1));
    assertEquals(result.size(), 4);
  }

  @Test(description = "Should correctly count values in map fields")
  public void testCountValuesInMapField() {
    Map<String, ComputeGenericRecord> computeResults = createMapTestData();

    Map<String, Integer> fieldTopKMap = Collections.singletonMap(PREFERENCES_FIELD, 10);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(PREFERENCES_FIELD);

    // Expected counts: dark_mode=3, english=2, spanish=2, light_mode=1
    assertEquals(result.get("dark_mode"), Integer.valueOf(3));
    assertEquals(result.get("english"), Integer.valueOf(2));
    assertEquals(result.get("spanish"), Integer.valueOf(2));
    assertEquals(result.get("light_mode"), Integer.valueOf(1));
  }

  @Test(description = "Should respect topK limit and return values in descending order")
  public void testTopKLimitAndDescendingOrder() {
    Map<String, ComputeGenericRecord> computeResults = createTopKTestData();

    // Test topK=3 limit
    Map<String, Integer> fieldTopKMap = Collections.singletonMap(SKILLS_FIELD, 3);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(SKILLS_FIELD);

    // Should only return top 3 results
    assertEquals(result.size(), 3);

    // Verify descending order: Java=5, Python=3, JavaScript=2
    List<Map.Entry<String, Integer>> entries = new ArrayList<>(result.entrySet());
    assertEquals(entries.get(0).getKey(), SKILL_JAVA);
    assertEquals(entries.get(0).getValue(), Integer.valueOf(5));
    assertEquals(entries.get(1).getKey(), SKILL_PYTHON);
    assertEquals(entries.get(1).getValue(), Integer.valueOf(3));
    assertEquals(entries.get(2).getKey(), SKILL_JAVASCRIPT);
    assertEquals(entries.get(2).getValue(), Integer.valueOf(2));

    // Go (count=1) should not be included
    assertFalse(result.containsKey(SKILL_GO));
  }

  @Test(description = "Should handle null values in value counting")
  public void testNullValueHandlingInValueCounting() {
    Map<String, ComputeGenericRecord> computeResults = createNullTestData();

    Map<String, Integer> fieldTopKMap = Collections.singletonMap(COMPANY_FIELD, 10);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(COMPANY_FIELD);

    // Expected counts: null=3, Google=2, Meta=1
    assertEquals(result.get(null), Integer.valueOf(3));
    assertEquals(result.get(COMPANY_GOOGLE), Integer.valueOf(2));
    assertEquals(result.get(COMPANY_META), Integer.valueOf(1));
  }

  // --- Execution Tests ---
  @Test(description = "Should execute with valid parameters")
  public void testValidExecution() {
    CompletableFuture<Map<String, ComputeGenericRecord>> mockFuture =
        CompletableFuture.completedFuture(new HashMap<>());
    when(delegate.execute(any())).thenReturn(mockFuture);

    Set<String> keys = new HashSet<>(Arrays.asList("user1", "user2"));

    CompletableFuture<ComputeAggregationResponse> future = builder.countGroupByValue(5, COMPANY_FIELD).execute(keys);

    assertNotNull(future);
    verify(delegate).execute(keys);
  }

  @Test(description = "Should reject null and empty keys")
  public void testInvalidKeys() {
    builder.countGroupByValue(5, COMPANY_FIELD);

    // Test null keys
    VeniceClientException ex1 = expectThrows(VeniceClientException.class, () -> builder.execute(null));
    assertTrue(ex1.getMessage().contains("keys cannot be null or empty"));

    // Test empty keys
    VeniceClientException ex2 = expectThrows(VeniceClientException.class, () -> builder.execute(new HashSet<>()));
    assertTrue(ex2.getMessage().contains("keys cannot be null or empty"));
  }

  // --- Test Data Creation Methods ---
  /**
   * Creates test data: Google=4, Meta=2, Amazon=2, Microsoft=1
   */
  private Map<String, ComputeGenericRecord> createArrayTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Record 1: [Google, Meta, Google] -> Google:2, Meta:1
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(COMPANY_FIELD)).thenReturn(Arrays.asList(COMPANY_GOOGLE, COMPANY_META, COMPANY_GOOGLE));
    data.put("user1", record1);

    // Record 2: [Meta, Amazon, Google] -> Meta:1, Amazon:1, Google:1
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(COMPANY_FIELD)).thenReturn(Arrays.asList(COMPANY_META, COMPANY_AMAZON, COMPANY_GOOGLE));
    data.put("user2", record2);

    // Record 3: [Amazon, Microsoft] -> Amazon:1, Microsoft:1
    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(COMPANY_FIELD)).thenReturn(Arrays.asList(COMPANY_AMAZON, COMPANY_MICROSOFT));
    data.put("user3", record3);

    // Record 4: [Google] -> Google:1
    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(COMPANY_FIELD)).thenReturn(Arrays.asList(COMPANY_GOOGLE));
    data.put("user4", record4);

    // Total: Google=4, Meta=2, Amazon=2, Microsoft=1
    return data;
  }

  /**
   * Creates map test data: dark_mode=3, english=2, spanish=2, light_mode=1
   */
  private Map<String, ComputeGenericRecord> createMapTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Record 1: {theme: "dark_mode", language: "english"}
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    Map<String, String> prefs1 = new HashMap<>();
    prefs1.put("theme", "dark_mode");
    prefs1.put("language", "english");
    when(record1.get(PREFERENCES_FIELD)).thenReturn(prefs1);
    data.put("user1", record1);

    // Record 2: {theme: "dark_mode", language: "spanish"}
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    Map<String, String> prefs2 = new HashMap<>();
    prefs2.put("theme", "dark_mode");
    prefs2.put("language", "spanish");
    when(record2.get(PREFERENCES_FIELD)).thenReturn(prefs2);
    data.put("user2", record2);

    // Record 3: {theme: "light_mode", language: "english"}
    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    Map<String, String> prefs3 = new HashMap<>();
    prefs3.put("theme", "light_mode");
    prefs3.put("language", "english");
    when(record3.get(PREFERENCES_FIELD)).thenReturn(prefs3);
    data.put("user3", record3);

    // Record 4: {theme: "dark_mode", language: "spanish"}
    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    Map<String, String> prefs4 = new HashMap<>();
    prefs4.put("theme", "dark_mode");
    prefs4.put("language", "spanish");
    when(record4.get(PREFERENCES_FIELD)).thenReturn(prefs4);
    data.put("user4", record4);

    // Total: dark_mode=3, english=2, spanish=2, light_mode=1
    return data;
  }

  /**
   * Creates topK test data: Java=5, Python=3, JavaScript=2, Go=1
   */
  private Map<String, ComputeGenericRecord> createTopKTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Record 1: [Java, Java, Python] -> Java:2, Python:1
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(SKILLS_FIELD)).thenReturn(Arrays.asList(SKILL_JAVA, SKILL_JAVA, SKILL_PYTHON));
    data.put("user1", record1);

    // Record 2: [Java, Python, JavaScript] -> Java:1, Python:1, JavaScript:1
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(SKILLS_FIELD)).thenReturn(Arrays.asList(SKILL_JAVA, SKILL_PYTHON, SKILL_JAVASCRIPT));
    data.put("user2", record2);

    // Record 3: [Java, Python, JavaScript] -> Java:1, Python:1, JavaScript:1
    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(SKILLS_FIELD)).thenReturn(Arrays.asList(SKILL_JAVA, SKILL_PYTHON, SKILL_JAVASCRIPT));
    data.put("user3", record3);

    // Record 4: [Java, Go] -> Java:1, Go:1
    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(SKILLS_FIELD)).thenReturn(Arrays.asList(SKILL_JAVA, SKILL_GO));
    data.put("user4", record4);

    // Total: Java=5, Python=3, JavaScript=2, Go=1
    return data;
  }

  /**
   * Creates null test data: null=3, Google=2, Meta=1
   */
  private Map<String, ComputeGenericRecord> createNullTestData() {
    Map<String, ComputeGenericRecord> data = new HashMap<>();

    // Record 1: null field value
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(COMPANY_FIELD)).thenReturn(null);
    data.put("user1", record1);

    // Record 2: array with null elements [Google, null, Google, null]
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    List<String> companiesWithNull = new ArrayList<>();
    companiesWithNull.add(COMPANY_GOOGLE);
    companiesWithNull.add(null);
    companiesWithNull.add(COMPANY_GOOGLE);
    companiesWithNull.add(null);
    when(record2.get(COMPANY_FIELD)).thenReturn(companiesWithNull);
    data.put("user2", record2);

    // Record 3: [Meta]
    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(COMPANY_FIELD)).thenReturn(Arrays.asList(COMPANY_META));
    data.put("user3", record3);

    // Total: null=3 (1 from null field + 2 from array), Google=2, Meta=1
    return data;
  }

  // --- Bucket Counting Tests ---
  @Test(description = "Should accept valid bucket parameters")
  public void testValidBucketParameters() {
    Map<String, Predicate<Long>> buckets = new HashMap<>();
    buckets.put("recent", LongPredicate.greaterThan(1000L));
    buckets.put("old", LongPredicate.lowerOrEquals(1000L));

    ComputeAggregationRequestBuilder<String> result = builder.countGroupByBucket(buckets, TIMESTAMP_FIELD);

    assertNotNull(result);
    verify(delegate).project(TIMESTAMP_FIELD);
  }

  @Test(description = "Should accept multiple bucket predicates")
  public void testMultipleBucketPredicates() {
    Map<String, Predicate<Long>> timeBuckets = new HashMap<>();
    long currentTime = System.currentTimeMillis();
    timeBuckets.put("last_24h", LongPredicate.greaterThan(currentTime - Time.MS_PER_DAY));
    timeBuckets.put("last_week", LongPredicate.greaterThan(currentTime - 7 * Time.MS_PER_DAY));
    timeBuckets.put("last_month", LongPredicate.greaterThan(currentTime - 30 * Time.MS_PER_DAY));

    builder.countGroupByBucket(timeBuckets, TIMESTAMP_FIELD);
    verify(delegate).project(TIMESTAMP_FIELD);
  }

  @Test(description = "Should support buckets on multiple fields")
  public void testBucketsOnMultipleFields() {
    Map<String, Predicate<Integer>> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("middle", Predicate.and(IntPredicate.greaterOrEquals(30), IntPredicate.lowerThan(60)));
    ageBuckets.put("senior", IntPredicate.greaterOrEquals(60));

    builder.countGroupByBucket(ageBuckets, AGE_FIELD, TIMESTAMP_FIELD);

    verify(delegate).project(AGE_FIELD);
    verify(delegate).project(TIMESTAMP_FIELD);
  }

  @Test(description = "Should reject null bucket map")
  public void testNullBucketMap() {
    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(null, TIMESTAMP_FIELD));
    assertTrue(ex.getMessage().contains("bucketNameToPredicate cannot be null or empty"));
  }

  @Test(description = "Should reject empty bucket map")
  public void testEmptyBucketMap() {
    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(new HashMap<>(), TIMESTAMP_FIELD));
    assertTrue(ex.getMessage().contains("bucketNameToPredicate cannot be null or empty"));
  }

  @Test(description = "Should reject null bucket name")
  public void testNullBucketName() {
    Map<String, Predicate<Long>> buckets = new HashMap<>();
    buckets.put(null, LongPredicate.greaterThan(1000L));

    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, TIMESTAMP_FIELD));
    assertTrue(ex.getMessage().contains("Bucket name cannot be null or empty"));
  }

  @Test(description = "Should reject empty bucket name")
  public void testEmptyBucketName() {
    Map<String, Predicate<Long>> buckets = new HashMap<>();
    buckets.put("", LongPredicate.greaterThan(1000L));

    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, TIMESTAMP_FIELD));
    assertTrue(ex.getMessage().contains("Bucket name cannot be null or empty"));
  }

  @Test(description = "Should reject null predicate")
  public void testNullPredicate() {
    Map<String, Predicate<Long>> buckets = new HashMap<>();
    buckets.put("bucket1", null);

    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, TIMESTAMP_FIELD));
    assertTrue(ex.getMessage().contains("Predicate for bucket 'bucket1' cannot be null"));
  }

  @Test(description = "Should reject null field names for buckets")
  public void testNullFieldNamesForBuckets() {
    Map<String, Predicate<Long>> buckets = new HashMap<>();
    buckets.put("bucket1", LongPredicate.greaterThan(1000L));

    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, (String[]) null));
    assertTrue(ex.getMessage().contains("fieldNames cannot be null or empty"));
  }

  @Test(description = "Should reject non-existent field for buckets")
  public void testNonExistentFieldForBuckets() {
    Map<String, Predicate<Long>> buckets = new HashMap<>();
    buckets.put("bucket1", LongPredicate.greaterThan(1000L));

    VeniceClientException ex =
        expectThrows(VeniceClientException.class, () -> builder.countGroupByBucket(buckets, "nonExistentField"));
    assertTrue(ex.getMessage().contains("Field not found in schema: nonExistentField"));
  }

  @Test(description = "Should execute bucket aggregation successfully")
  public void testBucketAggregationExecution() {
    // Setup bucket predicates
    Map<String, Predicate<Long>> buckets = new HashMap<>();
    long threshold = 1000L;
    buckets.put("high", LongPredicate.greaterThan(threshold));
    buckets.put("low", LongPredicate.lowerOrEquals(threshold));

    // Mock compute results
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();

    // Record 1: timestamp = 1500 (high bucket)
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(TIMESTAMP_FIELD)).thenReturn(1500L);
    computeResults.put("key1", record1);

    // Record 2: timestamp = 500 (low bucket)
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(TIMESTAMP_FIELD)).thenReturn(500L);
    computeResults.put("key2", record2);

    // Record 3: timestamp = 2000 (high bucket)
    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(TIMESTAMP_FIELD)).thenReturn(2000L);
    computeResults.put("key3", record3);

    CompletableFuture<Map<String, ComputeGenericRecord>> mockFuture = CompletableFuture.completedFuture(computeResults);
    when(delegate.execute(any())).thenReturn(mockFuture);

    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3"));

    CompletableFuture<ComputeAggregationResponse> future =
        builder.countGroupByBucket(buckets, TIMESTAMP_FIELD).execute(keys);

    assertNotNull(future);
    ComputeAggregationResponse response = future.join();

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(TIMESTAMP_FIELD);
    // Expected counts: high=2, low=1
    assertEquals(bucketCounts.get("high"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("low"), Integer.valueOf(1));
  }

  @Test(description = "Should handle complex time range buckets")
  public void testComplexTimeRangeBuckets() {
    // Use a fixed currentTime to ensure deterministic test results
    long currentTime = 1_000_000_000_000L;
    Map<String, Predicate<Long>> timeBuckets = new LinkedHashMap<>();
    timeBuckets.put("last_24h", LongPredicate.greaterOrEquals(currentTime - 1L * Time.MS_PER_DAY));
    timeBuckets.put("last_week", LongPredicate.greaterOrEquals(currentTime - 7L * Time.MS_PER_DAY));
    timeBuckets.put("last_30_days", LongPredicate.greaterOrEquals(currentTime - 30L * Time.MS_PER_DAY));

    // Mock compute results with various timestamps
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    // Record from 12 hours ago (matches all buckets)
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(TIMESTAMP_FIELD)).thenReturn(currentTime - 12 * Time.MS_PER_HOUR);
    computeResults.put("key1", record1);
    // Record from 3 days ago (matches week and 30 days)
    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(TIMESTAMP_FIELD)).thenReturn(currentTime - 3L * Time.MS_PER_DAY);
    computeResults.put("key2", record2);
    // Record from 15 days ago (matches only 30 days)
    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(TIMESTAMP_FIELD)).thenReturn(currentTime - 15L * Time.MS_PER_DAY);
    computeResults.put("key3", record3);
    // Record from 45 days ago (matches none)
    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(TIMESTAMP_FIELD)).thenReturn(currentTime - 45L * Time.MS_PER_DAY);
    computeResults.put("key4", record4);

    CompletableFuture<Map<String, ComputeGenericRecord>> mockFuture = CompletableFuture.completedFuture(computeResults);
    when(delegate.execute(any())).thenReturn(mockFuture);

    Set<String> keys = new HashSet<>(Arrays.asList("key1", "key2", "key3", "key4"));

    CompletableFuture<ComputeAggregationResponse> future =
        builder.countGroupByBucket(timeBuckets, TIMESTAMP_FIELD).execute(keys);

    ComputeAggregationResponse response = future.join();
    Map<String, Integer> bucketCounts = response.getBucketNameToCount(TIMESTAMP_FIELD);

    // Expected counts: last_24h=1, last_week=2, last_30_days=3
    assertEquals(bucketCounts.get("last_24h"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("last_week"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("last_30_days"), Integer.valueOf(3));
  }

  @Test(description = "Should combine bucket and value counting")
  public void testCombinedBucketAndValueCounting() {
    // Setup bucket predicates
    Map<String, Predicate<Integer>> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(30));
    ageBuckets.put("adult", IntPredicate.greaterOrEquals(30));

    // First add value counting
    builder.countGroupByValue(5, "skills");

    // Then add bucket counting
    builder.countGroupByBucket(ageBuckets, AGE_FIELD);

    // Verify both projections were called
    verify(delegate).project("skills");
    verify(delegate).project(AGE_FIELD);
  }
}
