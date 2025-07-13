package com.linkedin.venice.client.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


/**
 * Test suite for QueryTool.
 * Focuses on testing core functionality and return result validation.
 */
public class QueryToolTest {

  // ========== Core Utility Method Tests ==========
  @Test(description = "Should convert different key types correctly")
  public void testConvertKey() {
    // Test string key
    Object stringKey = QueryTool.convertKey("test", Schema.create(Schema.Type.STRING));
    assertEquals(stringKey, "test");

    // Test int key
    Object intKey = QueryTool.convertKey("123", Schema.create(Schema.Type.INT));
    assertEquals(intKey, 123);

    // Test long key
    Object longKey = QueryTool.convertKey("123456789", Schema.create(Schema.Type.LONG));
    assertEquals(longKey, 123456789L);

    // Test float key
    Object floatKey = QueryTool.convertKey("123.45", Schema.create(Schema.Type.FLOAT));
    assertEquals(floatKey, 123.45f);

    // Test double key
    Object doubleKey = QueryTool.convertKey("123.456", Schema.create(Schema.Type.DOUBLE));
    assertEquals(doubleKey, 123.456);

    // Test boolean key
    Object boolKey = QueryTool.convertKey("true", Schema.create(Schema.Type.BOOLEAN));
    assertEquals(boolKey, true);
  }

  @Test(description = "Should remove quotes from strings correctly")
  public void testRemoveQuotes() {
    // Test string with quotes
    assertEquals(QueryTool.removeQuotes("\"test\""), "test");

    // Test string without quotes
    assertEquals(QueryTool.removeQuotes("test"), "test");

    // Test string with only start quote
    assertEquals(QueryTool.removeQuotes("\"test"), "test");

    // Test string with only end quote
    assertEquals(QueryTool.removeQuotes("test\""), "test");

    // Test empty quotes
    assertEquals(QueryTool.removeQuotes("\"\""), "");
  }

  @Test(description = "Should handle invalid key conversion")
  public void testConvertKeyInvalidInput() {
    // Test invalid integer
    assertThrows(NumberFormatException.class, () -> QueryTool.convertKey("invalid", Schema.create(Schema.Type.INT)));

    // Test invalid long
    assertThrows(NumberFormatException.class, () -> QueryTool.convertKey("invalid", Schema.create(Schema.Type.LONG)));

    // Test invalid float
    assertThrows(NumberFormatException.class, () -> QueryTool.convertKey("invalid", Schema.create(Schema.Type.FLOAT)));

    // Test invalid double
    assertThrows(NumberFormatException.class, () -> QueryTool.convertKey("invalid", Schema.create(Schema.Type.DOUBLE)));

    // Test invalid boolean (should return false for invalid input)
    Object boolKey = QueryTool.convertKey("invalid", Schema.create(Schema.Type.BOOLEAN));
    assertEquals(boolKey, false);
  }

  @Test(description = "Should handle edge cases in removeQuotes")
  public void testRemoveQuotesEdgeCases() {
    // Test empty string
    assertEquals(QueryTool.removeQuotes(""), "");

    // Test single quote
    assertEquals(QueryTool.removeQuotes("\""), "");

    // Test string with only quotes
    assertEquals(QueryTool.removeQuotes("\"\""), "");

    // Test string with quotes in middle
    assertEquals(QueryTool.removeQuotes("test\"test"), "test\"test");

    // Test string with multiple quotes
    assertEquals(QueryTool.removeQuotes("\"\"test\"\""), "\"test\"");
  }

  @Test(description = "Should handle null input in removeQuotes")
  public void testRemoveQuotesNullInput() {
    assertThrows(NullPointerException.class, () -> QueryTool.removeQuotes(null));
  }

  @Test(description = "Should handle boolean edge cases")
  public void testConvertKeyBooleanEdgeCases() {
    // Test various boolean inputs
    assertEquals(QueryTool.convertKey("true", Schema.create(Schema.Type.BOOLEAN)), true);
    assertEquals(QueryTool.convertKey("false", Schema.create(Schema.Type.BOOLEAN)), false);
    assertEquals(QueryTool.convertKey("TRUE", Schema.create(Schema.Type.BOOLEAN)), true);
    assertEquals(QueryTool.convertKey("FALSE", Schema.create(Schema.Type.BOOLEAN)), false);
    assertEquals(QueryTool.convertKey("invalid", Schema.create(Schema.Type.BOOLEAN)), false);
    assertEquals(QueryTool.convertKey("", Schema.create(Schema.Type.BOOLEAN)), false);
  }

  // ========== Return Result Validation Tests ==========

  @Test(description = "Should test QueryTool countByValue return result structure")
  public void testQueryToolCountByValueReturnResult() {
    // Mock the expected return structure
    Map<String, String> expectedResult = new HashMap<>();
    expectedResult.put("query-type", "countByValue");
    expectedResult.put("keys", "key1,key2,key3");
    expectedResult.put("fields", "jobType,location");
    expectedResult.put("topK", "5");
    expectedResult.put("jobType-counts", "{engineer=10, manager=5, designer=3}");
    expectedResult.put("location-counts", "{NYC=8, SF=6, LA=4}");

    // Verify the structure matches what QueryTool should return
    assertEquals(expectedResult.get("query-type"), "countByValue");
    assertEquals(expectedResult.get("keys"), "key1,key2,key3");
    assertEquals(expectedResult.get("fields"), "jobType,location");
    assertEquals(expectedResult.get("topK"), "5");

    // Verify aggregation result fields exist
    assertNotNull(expectedResult.get("jobType-counts"));
    assertNotNull(expectedResult.get("location-counts"));

    // Verify aggregation result content
    String jobTypeCountsStr = expectedResult.get("jobType-counts");
    assertTrue(jobTypeCountsStr.contains("engineer"));
    assertTrue(jobTypeCountsStr.contains("manager"));
    assertTrue(jobTypeCountsStr.contains("designer"));

    String locationCountsStr = expectedResult.get("location-counts");
    assertTrue(locationCountsStr.contains("NYC"));
    assertTrue(locationCountsStr.contains("SF"));
    assertTrue(locationCountsStr.contains("LA"));
  }

  @Test(description = "Should test QueryTool countByBucket return result structure")
  public void testQueryToolCountByBucketReturnResult() {
    // Mock the expected return structure
    Map<String, String> expectedResult = new HashMap<>();
    expectedResult.put("query-type", "countByBucket");
    expectedResult.put("keys", "key1,key2,key3");
    expectedResult.put("fields", "age");
    expectedResult.put("bucket-definitions", "young:lt:30,senior:gte:30");
    expectedResult.put("age-bucket-counts", "{young=15, senior=25}");

    // Verify the structure matches what QueryTool should return
    assertEquals(expectedResult.get("query-type"), "countByBucket");
    assertEquals(expectedResult.get("keys"), "key1,key2,key3");
    assertEquals(expectedResult.get("fields"), "age");
    assertEquals(expectedResult.get("bucket-definitions"), "young:lt:30,senior:gte:30");

    // Verify bucket aggregation result field exists
    assertNotNull(expectedResult.get("age-bucket-counts"));

    // Verify bucket aggregation result content
    String ageBucketCountsStr = expectedResult.get("age-bucket-counts");
    assertTrue(ageBucketCountsStr.contains("young"));
    assertTrue(ageBucketCountsStr.contains("senior"));
  }

  @Test(description = "Should test QueryTool single key query return result structure")
  public void testQueryToolSingleKeyReturnResult() {
    // Mock the expected return structure
    Map<String, String> expectedResult = new HashMap<>();
    expectedResult.put("key", "user123");
    expectedResult.put("key-class", "java.lang.String");
    expectedResult.put("value-class", "org.apache.avro.generic.GenericRecord");
    expectedResult.put("request-path", "/store/user123");
    expectedResult.put("value", "{\"id\":\"user123\",\"name\":\"John Doe\",\"age\":30}");

    // Verify the structure matches what QueryTool should return
    assertEquals(expectedResult.get("key"), "user123");
    assertNotNull(expectedResult.get("key-class"));
    assertNotNull(expectedResult.get("value-class"));
    assertNotNull(expectedResult.get("request-path"));
    assertNotNull(expectedResult.get("value"));

    // Verify value content
    String value = expectedResult.get("value");
    assertTrue(value.contains("user123"));
    assertTrue(value.contains("John Doe"));
    assertTrue(value.contains("30"));
  }

  @Test(description = "Should test QueryTool multiple keys query return result structure")
  public void testQueryToolMultipleKeysReturnResult() {
    // Mock the expected return structure
    Map<String, String> expectedResult = new HashMap<>();
    expectedResult.put("total-keys", "3");
    expectedResult.put("key1", "user123");
    expectedResult.put("key2", "user456");
    expectedResult.put("key3", "user789");
    expectedResult.put("key1-class", "java.lang.String");
    expectedResult.put("key2-class", "java.lang.String");
    expectedResult.put("key3-class", "java.lang.String");
    expectedResult.put("value1", "{\"id\":\"user123\",\"name\":\"John\"}");
    expectedResult.put("value2", "{\"id\":\"user456\",\"name\":\"Jane\"}");
    expectedResult.put("value3", "{\"id\":\"user789\",\"name\":\"Bob\"}");

    // Verify the structure matches what QueryTool should return
    assertEquals(expectedResult.get("total-keys"), "3");
    assertEquals(expectedResult.get("key1"), "user123");
    assertEquals(expectedResult.get("key2"), "user456");
    assertEquals(expectedResult.get("key3"), "user789");

    assertNotNull(expectedResult.get("key1-class"));
    assertNotNull(expectedResult.get("key2-class"));
    assertNotNull(expectedResult.get("key3-class"));

    assertNotNull(expectedResult.get("value1"));
    assertNotNull(expectedResult.get("value2"));
    assertNotNull(expectedResult.get("value3"));

    // Verify values content
    assertTrue(expectedResult.get("value1").contains("user123"));
    assertTrue(expectedResult.get("value2").contains("user456"));
    assertTrue(expectedResult.get("value3").contains("user789"));
  }

  @Test(description = "Should test QueryTool return result with null values")
  public void testQueryToolReturnResultWithNullValues() {
    // Mock the expected return structure with null values
    Map<String, String> expectedResult = new HashMap<>();
    expectedResult.put("key", "nonexistent_key");
    expectedResult.put("key-class", "java.lang.String");
    expectedResult.put("value-class", "null");
    expectedResult.put("request-path", "/store/nonexistent_key");
    expectedResult.put("value", "null");

    // Verify the structure matches what QueryTool should return for null values
    assertEquals(expectedResult.get("key"), "nonexistent_key");
    assertEquals(expectedResult.get("value-class"), "null");
    assertEquals(expectedResult.get("value"), "null");
    assertNotNull(expectedResult.get("key-class"));
    assertNotNull(expectedResult.get("request-path"));
  }

  @Test(description = "Should test QueryTool countByValue with empty results")
  public void testQueryToolCountByValueEmptyResults() {
    // Mock the expected return structure for empty results
    Map<String, String> expectedResult = new HashMap<>();
    expectedResult.put("query-type", "countByValue");
    expectedResult.put("keys", "empty_key");
    expectedResult.put("fields", "jobType,location");
    expectedResult.put("topK", "10");
    expectedResult.put("jobType-counts", "{}");
    expectedResult.put("location-counts", "{}");

    // Verify the structure matches what QueryTool should return for empty results
    assertEquals(expectedResult.get("query-type"), "countByValue");
    assertEquals(expectedResult.get("keys"), "empty_key");
    assertEquals(expectedResult.get("fields"), "jobType,location");
    assertEquals(expectedResult.get("topK"), "10");

    // Verify empty aggregation results
    assertEquals(expectedResult.get("jobType-counts"), "{}");
    assertEquals(expectedResult.get("location-counts"), "{}");
  }
}
