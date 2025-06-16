package com.linkedin.venice.client.store;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
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

  // --- Core Aggregation Tests ---
  @Test(description = "Should correctly count array field values")
  public void testArrayFieldCounting() {
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

  @Test(description = "Should correctly count map field values")
  public void testMapFieldCounting() {
    computeResults = createMapTestData();
    fieldTopKMap.put(PREFERENCES_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(PREFERENCES_FIELD);

    // Verify counts: dark_mode=3, english=2, spanish=2, light_mode=1
    assertEquals(result.get("dark_mode"), Integer.valueOf(3));
    assertEquals(result.get("english"), Integer.valueOf(2));
    assertEquals(result.get("spanish"), Integer.valueOf(2));
    assertEquals(result.get("light_mode"), Integer.valueOf(1));
  }

  @Test(description = "Should respect topK limit and return descending order")
  public void testTopKAndOrdering() {
    computeResults = createTopKTestData();
    fieldTopKMap.put(SKILLS_FIELD, 3);

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

  @Test(description = "Should handle null values correctly")
  public void testNullValueHandling() {
    computeResults = createNullTestData();
    fieldTopKMap.put(COMPANY_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(COMPANY_FIELD);

    // Verify null handling: null=3, Google=2, Meta=1
    assertEquals(result.get(null), Integer.valueOf(3));
    assertEquals(result.get(COMPANY_GOOGLE), Integer.valueOf(2));
    assertEquals(result.get(COMPANY_META), Integer.valueOf(1));
  }

  // --- Edge Case Tests ---
  @Test(description = "Should handle empty compute results")
  public void testEmptyComputeResults() {
    computeResults = new HashMap<>();
    fieldTopKMap.put(COMPANY_FIELD, 10);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(COMPANY_FIELD);
    assertTrue(result.isEmpty());
  }

  @Test(description = "Should handle null field values")
  public void testNullFieldValues() {
    computeResults = new HashMap<>();
    fieldTopKMap.put(COMPANY_FIELD, 10);

    // Record with null field value
    ComputeGenericRecord record = mock(ComputeGenericRecord.class);
    when(record.get(COMPANY_FIELD)).thenReturn(null);
    computeResults.put("user1", record);

    AvroComputeAggregationResponse<String> response =
        new AvroComputeAggregationResponse<>(computeResults, fieldTopKMap);

    Map<String, Integer> result = response.getValueToCount(COMPANY_FIELD);
    assertEquals(result.get(null), Integer.valueOf(1));
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
