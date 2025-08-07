package com.linkedin.venice.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for the shared CountByValueUtils methods used by both thin-client and fast-client.
 */
public class CountByValueUtilsTest {
  private static class MockRecord {
    private final Map<String, Object> data = new HashMap<>();

    public MockRecord(Map<String, Object> testData) {
      this.data.putAll(testData);
    }

    public Object get(String fieldName) {
      return data.get(fieldName);
    }
  }

  @Test
  public void testNormalizeValue() {
    // Test Utf8 conversion
    Utf8 utf8Value = new Utf8("test_string");
    Object result = CountByValueUtils.normalizeValue(utf8Value);
    Assert.assertEquals(result, "test_string");
    Assert.assertTrue(result instanceof String);

    // Test non-Utf8 values are unchanged
    String stringValue = "already_string";
    result = CountByValueUtils.normalizeValue(stringValue);
    Assert.assertEquals(result, "already_string");
    Assert.assertTrue(result instanceof String);

    Integer intValue = 42;
    result = CountByValueUtils.normalizeValue(intValue);
    Assert.assertEquals(result, intValue);
    Assert.assertTrue(result instanceof Integer);

    // Test null
    result = CountByValueUtils.normalizeValue(null);
    Assert.assertNull(result);
  }

  @Test
  public void testAggregateValues() {
    List<String> values = Arrays.asList("user123", "member456", "user123", "record789", "member456", "user123");

    Map<String, Integer> result = CountByValueUtils.aggregateValues(values);

    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get("user123"), Integer.valueOf(3));
    Assert.assertEquals(result.get("member456"), Integer.valueOf(2));
    Assert.assertEquals(result.get("record789"), Integer.valueOf(1));
  }

  @Test
  public void testAggregateValuesEmpty() {
    List<String> emptyValues = Arrays.asList();

    Map<String, Integer> result = CountByValueUtils.aggregateValues(emptyValues);

    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testFilterTopKValuesGeneric() {
    Map<String, Integer> fieldCounts = new HashMap<>();
    fieldCounts.put("userId_1001", 5);
    fieldCounts.put("memberId_2002", 3);
    fieldCounts.put("recordId_3003", 8);
    fieldCounts.put("searchId_4004", 1);
    fieldCounts.put("sessionId_5005", 4);

    // Test normal TopK
    Map<String, Integer> result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 3);
    Assert.assertEquals(result.size(), 3);
    Assert.assertTrue(result.containsKey("recordId_3003")); // count 8
    Assert.assertTrue(result.containsKey("userId_1001")); // count 5
    Assert.assertTrue(result.containsKey("sessionId_5005")); // count 4

    // Test TopK larger than available items
    result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 10);
    Assert.assertEquals(result.size(), 5);
    Assert.assertEquals(result, fieldCounts);

    // Test TopK = 1
    result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 1);
    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(result.containsKey("recordId_3003"));
    Assert.assertEquals(result.get("recordId_3003"), Integer.valueOf(8));
  }

  @Test
  public void testFilterTopKValuesGenericWithTies() {
    Map<String, Integer> fieldCounts = new HashMap<>();
    fieldCounts.put("userId_1001", 3);
    fieldCounts.put("memberId_2002", 3);
    fieldCounts.put("recordId_3003", 5);

    Map<String, Integer> result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.containsKey("recordId_3003")); // highest count
    // Between userId and memberId (both count 3), should be deterministic based on key comparison
    Assert.assertTrue(result.containsKey("userId_1001") || result.containsKey("memberId_2002"));
  }

  @Test
  public void testFilterTopKValuesGenericWithNullKeys() {
    Map<String, Integer> fieldCounts = new HashMap<>();
    fieldCounts.put("userId_1001", 5);
    fieldCounts.put(null, 3);
    fieldCounts.put("recordId_3003", 8);

    Map<String, Integer> result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.containsKey("recordId_3003")); // count 8
    Assert.assertTrue(result.containsKey("userId_1001")); // count 5
    // null should come last due to null handling in comparator
  }

  @Test
  public void testValidateFieldNamesBasic() {
    // Test null array
    try {
      CountByValueUtils.validateFieldNames(null, createStringSchema());
      Assert.fail("Should throw exception for null field names");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
    }

    // Test empty array
    try {
      CountByValueUtils.validateFieldNames(new String[0], createStringSchema());
      Assert.fail("Should throw exception for empty field names");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
    }

    // Test null field name
    try {
      CountByValueUtils.validateFieldNames(new String[] { null }, createStringSchema());
      Assert.fail("Should throw exception for null field name");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
    }

    // Test empty field name
    try {
      CountByValueUtils.validateFieldNames(new String[] { "" }, createStringSchema());
      Assert.fail("Should throw exception for empty field name");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("cannot be null or empty"));
    }
  }

  @Test
  public void testValidateFieldNamesStringSchema() {
    Schema stringSchema = createStringSchema();

    // Test valid field names for string schema
    CountByValueUtils.validateFieldNames(new String[] { "value" }, stringSchema);
    CountByValueUtils.validateFieldNames(new String[] { "_value" }, stringSchema);

    // Test invalid field name for string schema
    try {
      CountByValueUtils.validateFieldNames(new String[] { "invalid_field" }, stringSchema);
      Assert.fail("Should throw exception for invalid field name in string schema");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("only 'value' or '_value' field names are supported"));
    }
  }

  @Test
  public void testValidateFieldNamesRecordSchema() {
    Schema recordSchema = createRecordSchema();

    // Test valid field names for record schema
    CountByValueUtils.validateFieldNames(new String[] { "name" }, recordSchema);
    CountByValueUtils.validateFieldNames(new String[] { "age" }, recordSchema);
    CountByValueUtils.validateFieldNames(new String[] { "name", "age" }, recordSchema);

    // Test invalid field name for record schema
    try {
      CountByValueUtils.validateFieldNames(new String[] { "nonexistent" }, recordSchema);
      Assert.fail("Should throw exception for nonexistent field in record schema");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("not found in schema"));
    }
  }

  @Test
  public void testValidateFieldNamesUnsupportedSchema() {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));

    try {
      CountByValueUtils.validateFieldNames(new String[] { "value" }, arraySchema);
      Assert.fail("Should throw exception for unsupported schema type");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("only supports STRING and RECORD value types"));
    }
  }

  @Test
  public void testExtractFieldToValueCounts() {
    // Test with GenericRecord objects
    List<GenericRecord> records = new ArrayList<>();
    Schema recordSchema = createRecordSchema();

    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("name", "John");
    record1.put("age", 25);
    records.add(record1);

    GenericRecord record2 = new GenericData.Record(recordSchema);
    record2.put("name", "Jane");
    record2.put("age", 25);
    records.add(record2);

    GenericRecord record3 = new GenericData.Record(recordSchema);
    record3.put("name", "John");
    record3.put("age", 30);
    records.add(record3);

    List<String> fieldNames = Arrays.asList("name", "age");
    CountByValueUtils.FieldValueExtractor<GenericRecord> extractor = CountByValueUtils::extractFieldValueGeneric;

    Map<String, Map<Object, Integer>> result =
        CountByValueUtils.extractFieldToValueCounts(records, fieldNames, extractor);

    // Verify name field counts
    Map<Object, Integer> nameCounts = result.get("name");
    Assert.assertEquals(Integer.valueOf(2), nameCounts.get("John"));
    Assert.assertEquals(Integer.valueOf(1), nameCounts.get("Jane"));

    // Verify age field counts
    Map<Object, Integer> ageCounts = result.get("age");
    Assert.assertEquals(Integer.valueOf(2), ageCounts.get(25));
    Assert.assertEquals(Integer.valueOf(1), ageCounts.get(30));
  }

  @Test
  public void testExtractFieldToValueCountsWithNulls() {
    // Test with null records and null field values
    List<GenericRecord> records = new ArrayList<>();
    Schema recordSchema = createRecordSchema();

    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("name", "John");
    record1.put("age", null); // null value
    records.add(record1);

    records.add(null); // null record

    GenericRecord record2 = new GenericData.Record(recordSchema);
    record2.put("name", null); // null value
    record2.put("age", 25);
    records.add(record2);

    List<String> fieldNames = Arrays.asList("name", "age");
    CountByValueUtils.FieldValueExtractor<GenericRecord> extractor = CountByValueUtils::extractFieldValueGeneric;

    Map<String, Map<Object, Integer>> result =
        CountByValueUtils.extractFieldToValueCounts(records, fieldNames, extractor);

    // Verify name field counts (including null)
    Map<Object, Integer> nameCounts = result.get("name");
    Assert.assertEquals(Integer.valueOf(1), nameCounts.get("John"));
    Assert.assertEquals(Integer.valueOf(1), nameCounts.get(null));

    // Verify age field counts (including null)
    Map<Object, Integer> ageCounts = result.get("age");
    Assert.assertEquals(Integer.valueOf(1), ageCounts.get(25));
    Assert.assertEquals(Integer.valueOf(1), ageCounts.get(null));
  }

  @Test
  public void testApplyTopKToFieldCounts() {
    // Create test data
    Map<String, Map<Object, Integer>> fieldToValueCounts = new HashMap<>();

    // Field 1: user IDs with different counts
    Map<Object, Integer> userIdCounts = new HashMap<>();
    userIdCounts.put("user_1001", 5);
    userIdCounts.put("user_2002", 3);
    userIdCounts.put("user_3003", 8);
    userIdCounts.put("user_4004", 2);
    fieldToValueCounts.put("userId", userIdCounts);

    // Field 2: member IDs with different counts
    Map<Object, Integer> memberIdCounts = new HashMap<>();
    memberIdCounts.put("member_5001", 10);
    memberIdCounts.put("member_5002", 5);
    memberIdCounts.put("member_5003", 3);
    memberIdCounts.put("member_5004", 1);
    fieldToValueCounts.put("memberId", memberIdCounts);

    // Apply TopK with k=2
    Map<String, Map<Object, Integer>> result = CountByValueUtils.applyTopKToFieldCounts(fieldToValueCounts, 2);

    // Verify userId field - should have top 2: user_3003(8), user_1001(5)
    Map<Object, Integer> userIdResult = result.get("userId");
    Assert.assertEquals(2, userIdResult.size());
    Assert.assertEquals(Integer.valueOf(8), userIdResult.get("user_3003"));
    Assert.assertEquals(Integer.valueOf(5), userIdResult.get("user_1001"));
    Assert.assertFalse(userIdResult.containsKey("user_2002"));
    Assert.assertFalse(userIdResult.containsKey("user_4004"));

    // Verify memberId field - should have top 2: member_5001(10), member_5002(5)
    Map<Object, Integer> memberIdResult = result.get("memberId");
    Assert.assertEquals(2, memberIdResult.size());
    Assert.assertEquals(Integer.valueOf(10), memberIdResult.get("member_5001"));
    Assert.assertEquals(Integer.valueOf(5), memberIdResult.get("member_5002"));
    Assert.assertFalse(memberIdResult.containsKey("member_5003"));
    Assert.assertFalse(memberIdResult.containsKey("member_5004"));
  }

  @Test
  public void testApplyTopKToFieldCountsWithEmptyField() {
    // Test with empty field
    Map<String, Map<Object, Integer>> fieldToValueCounts = new HashMap<>();
    fieldToValueCounts.put("empty_field", new HashMap<>());

    Map<String, Map<Object, Integer>> result = CountByValueUtils.applyTopKToFieldCounts(fieldToValueCounts, 5);

    Map<Object, Integer> emptyResult = result.get("empty_field");
    Assert.assertNotNull(emptyResult);
    Assert.assertTrue(emptyResult.isEmpty());
  }

  @Test
  public void testExtractFieldValueGenericWithString() {
    // Test with String value and "value" field
    String testValue = "test_string";
    Object result = CountByValueUtils.extractFieldValueGeneric(testValue, "value");
    Assert.assertEquals("test_string", result);

    // Test with "value" field
    result = CountByValueUtils.extractFieldValueGeneric(testValue, "_value");
    Assert.assertEquals("test_string", result);

    // Test with other field names - should return null
    result = CountByValueUtils.extractFieldValueGeneric(testValue, "other_field");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldValueGenericWithUtf8() {
    // Test with Utf8 value
    Utf8 testValue = new Utf8("test_utf8");
    Object result = CountByValueUtils.extractFieldValueGeneric(testValue, "value");
    Assert.assertEquals("test_utf8", result);

    result = CountByValueUtils.extractFieldValueGeneric(testValue, "_value");
    Assert.assertEquals("test_utf8", result);

    // Test with other field names - should return null
    result = CountByValueUtils.extractFieldValueGeneric(testValue, "other_field");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldValueGenericWithGenericRecord() {
    // Test with GenericRecord
    Schema recordSchema = createRecordSchema();
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("name", "John");
    record.put("age", 25);

    // Test extracting existing fields
    Object result = CountByValueUtils.extractFieldValueGeneric(record, "name");
    Assert.assertEquals("John", result);

    result = CountByValueUtils.extractFieldValueGeneric(record, "age");
    Assert.assertEquals(25, result);

    // Test extracting non-existent field
    result = CountByValueUtils.extractFieldValueGeneric(record, "non_existent");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldValueGenericWithNullValue() {
    // Test with null value
    Object result = CountByValueUtils.extractFieldValueGeneric(null, "any_field");
    Assert.assertNull(result);
  }

  @Test
  public void testExtractFieldValueGenericWithComputeGenericRecord() {
    // Create a mock object that has a get method (simulating ComputeGenericRecord)
    Map<String, Object> testData = new HashMap<>();
    testData.put("userId", "user_12345");
    MockRecord mockRecord = new MockRecord(testData);

    Object result = CountByValueUtils.extractFieldValueGeneric(mockRecord, "userId");
    Assert.assertEquals("user_12345", result);

    result = CountByValueUtils.extractFieldValueGeneric(mockRecord, "memberId");
    Assert.assertNull(result);
  }

  private Schema createStringSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  private Schema createRecordSchema() {
    String schemaStr = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"name\",\"type\":\"string\"}," + "{\"name\":\"age\",\"type\":\"int\"}" + "]}";
    return new Schema.Parser().parse(schemaStr);
  }

}
