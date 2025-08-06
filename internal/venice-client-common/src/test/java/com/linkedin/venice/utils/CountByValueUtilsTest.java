package com.linkedin.venice.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
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
