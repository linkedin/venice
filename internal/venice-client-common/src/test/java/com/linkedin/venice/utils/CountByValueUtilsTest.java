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
    List<String> values = Arrays.asList("apple", "banana", "apple", "cherry", "banana", "apple");

    Map<String, Integer> result = CountByValueUtils.aggregateValues(values);

    Assert.assertEquals(result.size(), 3);
    Assert.assertEquals(result.get("apple"), Integer.valueOf(3));
    Assert.assertEquals(result.get("banana"), Integer.valueOf(2));
    Assert.assertEquals(result.get("cherry"), Integer.valueOf(1));
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
    fieldCounts.put("apple", 5);
    fieldCounts.put("banana", 3);
    fieldCounts.put("cherry", 8);
    fieldCounts.put("date", 1);
    fieldCounts.put("elderberry", 4);

    // Test normal TopK
    Map<String, Integer> result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 3);
    Assert.assertEquals(result.size(), 3);
    Assert.assertTrue(result.containsKey("cherry")); // count 8
    Assert.assertTrue(result.containsKey("apple")); // count 5
    Assert.assertTrue(result.containsKey("elderberry")); // count 4

    // Test TopK larger than available items
    result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 10);
    Assert.assertEquals(result.size(), 5);
    Assert.assertEquals(result, fieldCounts);

    // Test TopK = 1
    result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 1);
    Assert.assertEquals(result.size(), 1);
    Assert.assertTrue(result.containsKey("cherry"));
    Assert.assertEquals(result.get("cherry"), Integer.valueOf(8));
  }

  @Test
  public void testFilterTopKValuesGenericWithTies() {
    Map<String, Integer> fieldCounts = new HashMap<>();
    fieldCounts.put("apple", 3);
    fieldCounts.put("banana", 3);
    fieldCounts.put("cherry", 5);

    Map<String, Integer> result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.containsKey("cherry")); // highest count
    // Between apple and banana (both count 3), should be deterministic based on key comparison
    Assert.assertTrue(result.containsKey("apple") || result.containsKey("banana"));
  }

  @Test
  public void testFilterTopKValuesGenericWithNullKeys() {
    Map<String, Integer> fieldCounts = new HashMap<>();
    fieldCounts.put("apple", 5);
    fieldCounts.put(null, 3);
    fieldCounts.put("cherry", 8);

    Map<String, Integer> result = CountByValueUtils.filterTopKValuesGeneric(fieldCounts, 2);
    Assert.assertEquals(result.size(), 2);
    Assert.assertTrue(result.containsKey("cherry")); // count 8
    Assert.assertTrue(result.containsKey("apple")); // count 5
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

    // Field 1: names with different counts
    Map<Object, Integer> nameCounts = new HashMap<>();
    nameCounts.put("John", 5);
    nameCounts.put("Jane", 3);
    nameCounts.put("Bob", 8);
    nameCounts.put("Alice", 2);
    fieldToValueCounts.put("name", nameCounts);

    // Field 2: ages with different counts
    Map<Object, Integer> ageCounts = new HashMap<>();
    ageCounts.put(25, 10);
    ageCounts.put(30, 5);
    ageCounts.put(35, 3);
    ageCounts.put(40, 1);
    fieldToValueCounts.put("age", ageCounts);

    // Apply TopK with k=2
    Map<String, Map<Object, Integer>> result = CountByValueUtils.applyTopKToFieldCounts(fieldToValueCounts, 2);

    // Verify name field - should have top 2: Bob(8), John(5)
    Map<Object, Integer> nameResult = result.get("name");
    Assert.assertEquals(2, nameResult.size());
    Assert.assertEquals(Integer.valueOf(8), nameResult.get("Bob"));
    Assert.assertEquals(Integer.valueOf(5), nameResult.get("John"));
    Assert.assertFalse(nameResult.containsKey("Jane"));
    Assert.assertFalse(nameResult.containsKey("Alice"));

    // Verify age field - should have top 2: 25(10), 30(5)
    Map<Object, Integer> ageResult = result.get("age");
    Assert.assertEquals(2, ageResult.size());
    Assert.assertEquals(Integer.valueOf(10), ageResult.get(25));
    Assert.assertEquals(Integer.valueOf(5), ageResult.get(30));
    Assert.assertFalse(ageResult.containsKey(35));
    Assert.assertFalse(ageResult.containsKey(40));
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
    Object mockRecord = new Object() {
      public Object get(String fieldName) {
        if ("test_field".equals(fieldName)) {
          return "test_value";
        }
        return null;
      }
    };

    Object result = CountByValueUtils.extractFieldValueGeneric(mockRecord, "test_field");
    Assert.assertEquals("test_value", result);

    result = CountByValueUtils.extractFieldValueGeneric(mockRecord, "non_existent");
    Assert.assertNull(result);
  }

  private Schema createStringSchema() {
    return Schema.create(Schema.Type.STRING);
  }

  private Schema createRecordSchema() {
    Schema recordSchema = Schema.createRecord("TestRecord", null, null, false);
    recordSchema.setFields(
        Arrays.asList(
            new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("age", Schema.create(Schema.Type.INT), null, null)));
    return recordSchema;
  }
}
