package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.client.store.predicate.DoublePredicate;
import com.linkedin.venice.client.store.predicate.FloatPredicate;
import com.linkedin.venice.client.store.predicate.IntPredicate;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.SchemaBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Core test suite for AvroComputeAggregationResponse value counting functionality.
 * Focuses on essential aggregation logic, topK ordering, and edge case handling.
 */
public class AvroComputeAggregationResponseTest {
  private static final String COMPANY_FIELD = "companies";
  private static final String SKILLS_FIELD = "skills";
  private static final String PREFERENCES_FIELD = "preferences";
  private static final String TIMESTAMP_FIELD = "reportTimestamp";
  private static final String AGE_FIELD = "age";
  private static final String SCORE_FIELD = "score";
  private static final String RATING_FIELD = "rating"; // For float values
  private static final String PRICE_FIELD = "price"; // For double values

  private static final String COMPANY_GOOGLE = "Google";
  private static final String COMPANY_META = "Meta";
  private static final String COMPANY_AMAZON = "Amazon";
  private static final String COMPANY_MICROSOFT = "Microsoft";

  private static final String SKILL_JAVA = "Java";
  private static final String SKILL_PYTHON = "Python";
  private static final String SKILL_JAVASCRIPT = "JavaScript";
  private static final String SKILL_GO = "Go";

  private Map<String, ComputeGenericRecord> computeResults;
  private Map<String, Integer> fieldTopKMap;

  @BeforeMethod
  public void setUp() {
    setupTestSchema();
    setupTestData();
  }

  private void setupTestSchema() {
    SchemaBuilder.record("UserProfile")
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
        .endRecord();
  }

  private void setupTestData() {
    computeResults = new HashMap<>();
    fieldTopKMap = new HashMap<>();
  }

  // --- Value Counting Tests ---
  @Test(description = "Should correctly count values in array fields")
  public void testCountValuesInArrayField() {
    computeResults = createArrayTestData();
    fieldTopKMap.put(COMPANY_FIELD, 10);

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
    computeResults = createMapTestData();
    fieldTopKMap.put(PREFERENCES_FIELD, 10);

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
    computeResults = createTopKTestData();

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
    computeResults = createNullTestData();

    Map<String, Integer> fieldTopKMap = Collections.singletonMap(COMPANY_FIELD, 10);
    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(COMPANY_FIELD);

    // Expected counts: null=3, Google=2, Meta=1
    assertEquals(result.get(null), Integer.valueOf(3));
    assertEquals(result.get(COMPANY_GOOGLE), Integer.valueOf(2));
    assertEquals(result.get(COMPANY_META), Integer.valueOf(1));
  }

  // --- Bucket Counting Tests ---
  @Test(description = "Should correctly count values in buckets")
  public void testCountValuesInBuckets() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates
    Map<String, Predicate> timestampBuckets = new HashMap<>();
    timestampBuckets.put("recent", LongPredicate.greaterThan(1000L));
    timestampBuckets.put("old", LongPredicate.lowerOrEquals(1000L));
    fieldBucketMap.put(TIMESTAMP_FIELD, timestampBuckets);

    // Create test records
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(TIMESTAMP_FIELD)).thenReturn(1500L); // recent
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(TIMESTAMP_FIELD)).thenReturn(500L); // old
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(TIMESTAMP_FIELD)).thenReturn(2000L); // recent
    computeResults.put("key3", record3);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(TIMESTAMP_FIELD);

    // Expected counts: recent=2, old=1
    assertEquals(bucketCounts.get("recent"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("old"), Integer.valueOf(1));
  }

  @Test(description = "Should handle overlapping buckets correctly")
  public void testOverlappingBuckets() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup overlapping bucket predicates
    Map<String, Predicate> ageBuckets = new HashMap<>();
    ageBuckets.put("young", IntPredicate.lowerThan(40));
    ageBuckets.put("adult", IntPredicate.greaterOrEquals(20));
    ageBuckets.put("middle_age", Predicate.and(IntPredicate.greaterOrEquals(30), IntPredicate.lowerThan(50)));
    fieldBucketMap.put(AGE_FIELD, ageBuckets);

    // Create test records
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(AGE_FIELD)).thenReturn(25); // young and adult
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(AGE_FIELD)).thenReturn(35); // young, adult, and middle_age
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(AGE_FIELD)).thenReturn(45); // adult and middle_age
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(AGE_FIELD)).thenReturn(15); // young only
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(AGE_FIELD);

    // Expected counts: young=3, adult=3, middle_age=2
    assertEquals(bucketCounts.get("young"), Integer.valueOf(3)); // records 1, 2, 4
    assertEquals(bucketCounts.get("adult"), Integer.valueOf(3)); // records 1, 2, 3
    assertEquals(bucketCounts.get("middle_age"), Integer.valueOf(2)); // records 2, 3
  }

  @Test(description = "Should handle null values in bucket counting")
  public void testNullValueHandlingInBucketCounting() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates
    Map<String, Predicate> timestampBuckets = new HashMap<>();
    timestampBuckets.put("recent", LongPredicate.greaterThan(1000L));
    timestampBuckets.put("old", LongPredicate.lowerOrEquals(1000L));
    fieldBucketMap.put(TIMESTAMP_FIELD, timestampBuckets);

    // Create test records with null values
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(TIMESTAMP_FIELD)).thenReturn(null);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(TIMESTAMP_FIELD)).thenReturn(1500L);
    computeResults.put("key2", record2);

    // Null record
    computeResults.put("key3", null);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(TIMESTAMP_FIELD);

    // Expected counts: recent=1, old=0 (null values should not match any bucket)
    assertEquals(bucketCounts.get("recent"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("old"), Integer.valueOf(0));
  }

  @Test(description = "Should throw exception when no bucket aggregation was requested")
  public void testGetBucketNameToCountWithoutRequest() {
    computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    assertThrows(IllegalArgumentException.class, () -> response.getBucketNameToCount(TIMESTAMP_FIELD));
  }

  @Test(description = "Should handle empty bucket results")
  public void testEmptyBucketResults() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates that won't match anything
    Map<String, Predicate> timestampBuckets = new HashMap<>();
    timestampBuckets.put("future", LongPredicate.greaterThan(10000L));
    timestampBuckets.put("past", LongPredicate.lowerOrEquals(0L));
    fieldBucketMap.put(TIMESTAMP_FIELD, timestampBuckets);

    // Create records with timestamps that don't match any bucket
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(TIMESTAMP_FIELD)).thenReturn(500L);
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(TIMESTAMP_FIELD)).thenReturn(1000L);
    computeResults.put("key2", record2);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(TIMESTAMP_FIELD);

    // Expected counts: future=0, past=0 (no matches)
    assertEquals(bucketCounts.get("future"), Integer.valueOf(0));
    assertEquals(bucketCounts.get("past"), Integer.valueOf(0));
  }

  @Test(description = "Should handle type mismatches in bucket counting")
  public void testTypeMismatchHandlingInBucketCounting() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates expecting Long
    Map<String, Predicate> timestampBuckets = new HashMap<>();
    timestampBuckets.put("recent", LongPredicate.greaterThan(1000L));
    fieldBucketMap.put(TIMESTAMP_FIELD, timestampBuckets);

    // Create records with wrong type (String instead of Long)
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(TIMESTAMP_FIELD)).thenReturn("not a long");
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(TIMESTAMP_FIELD)).thenReturn(1500L); // Correct type
    computeResults.put("key2", record2);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(TIMESTAMP_FIELD);

    // Expected count: recent=1 (only valid record should count)
    assertEquals(bucketCounts.get("recent"), Integer.valueOf(1));
  }

  @Test(description = "Should correctly count values in float buckets")
  public void testCountValuesInFloatBuckets() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates for float values
    Map<String, Predicate> ratingBuckets = new HashMap<>();
    ratingBuckets.put("high", FloatPredicate.greaterThan(4.0f));
    ratingBuckets
        .put("medium", Predicate.and(FloatPredicate.greaterOrEquals(2.0f), FloatPredicate.lowerOrEquals(4.0f)));
    ratingBuckets.put("low", FloatPredicate.lowerThan(2.0f));
    fieldBucketMap.put(RATING_FIELD, ratingBuckets);

    // Create test records
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(RATING_FIELD)).thenReturn(4.5f); // high
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(RATING_FIELD)).thenReturn(3.0f); // medium
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(RATING_FIELD)).thenReturn(1.5f); // low
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(RATING_FIELD)).thenReturn(4.0f); // medium
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(RATING_FIELD);

    // Expected counts: high=1, medium=2, low=1
    assertEquals(bucketCounts.get("high"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("medium"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("low"), Integer.valueOf(1));
  }

  @Test(description = "Should correctly count values in double buckets")
  public void testCountValuesInDoubleBuckets() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates for double values
    Map<String, Predicate> priceBuckets = new HashMap<>();
    priceBuckets.put("expensive", DoublePredicate.greaterThan(100.0));
    priceBuckets
        .put("moderate", Predicate.and(DoublePredicate.greaterOrEquals(50.0), DoublePredicate.lowerOrEquals(100.0)));
    priceBuckets.put("cheap", DoublePredicate.lowerThan(50.0));
    fieldBucketMap.put(PRICE_FIELD, priceBuckets);

    // Create test records
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(PRICE_FIELD)).thenReturn(150.0); // expensive
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(PRICE_FIELD)).thenReturn(75.0); // moderate
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(PRICE_FIELD)).thenReturn(25.0); // cheap
    computeResults.put("key3", record3);

    ComputeGenericRecord record4 = mock(ComputeGenericRecord.class);
    when(record4.get(PRICE_FIELD)).thenReturn(100.0); // moderate
    computeResults.put("key4", record4);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(PRICE_FIELD);

    // Expected counts: expensive=1, moderate=2, cheap=1
    assertEquals(bucketCounts.get("expensive"), Integer.valueOf(1));
    assertEquals(bucketCounts.get("moderate"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("cheap"), Integer.valueOf(1));
  }

  @Test(description = "Should handle float anyOf predicate")
  public void testFloatAnyOfPredicate() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates for float values
    Map<String, Predicate> ratingBuckets = new HashMap<>();
    ratingBuckets.put("top_ratings", FloatPredicate.anyOf(4.5f, 5.0f));
    ratingBuckets.put("average_ratings", FloatPredicate.anyOf(3.0f, 3.5f, 4.0f));
    fieldBucketMap.put(RATING_FIELD, ratingBuckets);

    // Create test records
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(RATING_FIELD)).thenReturn(4.5f); // top_ratings
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(RATING_FIELD)).thenReturn(3.5f); // average_ratings
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(RATING_FIELD)).thenReturn(5.0f); // top_ratings
    computeResults.put("key3", record3);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(RATING_FIELD);

    // Expected counts: top_ratings=2, average_ratings=1
    assertEquals(bucketCounts.get("top_ratings"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("average_ratings"), Integer.valueOf(1));
  }

  @Test(description = "Should handle double anyOf predicate")
  public void testDoubleAnyOfPredicate() {
    Map<String, ComputeGenericRecord> computeResults = new HashMap<>();
    Map<String, Map<String, Predicate>> fieldBucketMap = new HashMap<>();

    // Setup bucket predicates for double values
    Map<String, Predicate> priceBuckets = new HashMap<>();
    priceBuckets.put("premium", DoublePredicate.anyOf(199.99, 299.99, 399.99));
    priceBuckets.put("standard", DoublePredicate.anyOf(99.99, 149.99, 199.99));
    fieldBucketMap.put(PRICE_FIELD, priceBuckets);

    // Create test records
    ComputeGenericRecord record1 = mock(ComputeGenericRecord.class);
    when(record1.get(PRICE_FIELD)).thenReturn(199.99); // premium and standard
    computeResults.put("key1", record1);

    ComputeGenericRecord record2 = mock(ComputeGenericRecord.class);
    when(record2.get(PRICE_FIELD)).thenReturn(149.99); // standard
    computeResults.put("key2", record2);

    ComputeGenericRecord record3 = mock(ComputeGenericRecord.class);
    when(record3.get(PRICE_FIELD)).thenReturn(299.99); // premium
    computeResults.put("key3", record3);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, new HashMap<>(), fieldBucketMap);

    Map<String, Integer> bucketCounts = response.getBucketNameToCount(PRICE_FIELD);

    // Expected counts: premium=2, standard=2
    assertEquals(bucketCounts.get("premium"), Integer.valueOf(2));
    assertEquals(bucketCounts.get("standard"), Integer.valueOf(2));
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
}
