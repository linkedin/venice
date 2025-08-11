package com.linkedin.venice.client.store;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class FacetCountingUtilsTest {
  @Test(description = "Test validateFieldNames with RECORD schema and valid field")
  public void testValidateFieldNamesWithRecordSchemaValidField() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"}," + "{\"name\":\"field2\",\"type\":\"int\"}]}";
    Schema recordSchema = new Schema.Parser().parse(schemaJson);

    // Should not throw for existing fields
    FacetCountingUtils.validateFieldNames(new String[] { "field1" }, recordSchema);
    FacetCountingUtils.validateFieldNames(new String[] { "field2" }, recordSchema);
    FacetCountingUtils.validateFieldNames(new String[] { "field1", "field2" }, recordSchema);
  }

  @Test(description = "Test validateFieldNames with RECORD schema and non-existent field")
  public void testValidateFieldNamesWithRecordSchemaNonExistentField() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[" + "{\"name\":\"field1\",\"type\":\"string\"}]}";
    Schema recordSchema = new Schema.Parser().parse(schemaJson);

    IllegalArgumentException ex = expectThrows(
        IllegalArgumentException.class,
        () -> FacetCountingUtils.validateFieldNames(new String[] { "nonExistentField" }, recordSchema));
    assertEquals(ex.getMessage(), "Field not found in schema: nonExistentField");
  }

  @Test(description = "Test validateFieldNames with null field name")
  public void testValidateFieldNamesWithNullFieldName() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[" + "{\"name\":\"field1\",\"type\":\"string\"}]}";
    Schema recordSchema = new Schema.Parser().parse(schemaJson);

    IllegalArgumentException ex = expectThrows(
        IllegalArgumentException.class,
        () -> FacetCountingUtils.validateFieldNames(new String[] { null }, recordSchema));
    assertEquals(ex.getMessage(), "Field name cannot be null");
  }

  @Test(description = "Test validateFieldNames with empty field name")
  public void testValidateFieldNamesWithEmptyFieldName() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[" + "{\"name\":\"field1\",\"type\":\"string\"}]}";
    Schema recordSchema = new Schema.Parser().parse(schemaJson);

    IllegalArgumentException ex = expectThrows(
        IllegalArgumentException.class,
        () -> FacetCountingUtils.validateFieldNames(new String[] { "" }, recordSchema));
    assertEquals(ex.getMessage(), "Field name cannot be empty");
  }

  @Test(description = "Test validateFieldNames with multiple fields including invalid ones")
  public void testValidateFieldNamesWithMixedValidAndInvalidFields() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"}," + "{\"name\":\"field2\",\"type\":\"int\"}]}";
    Schema recordSchema = new Schema.Parser().parse(schemaJson);

    // First field is valid, second is invalid
    IllegalArgumentException ex = expectThrows(
        IllegalArgumentException.class,
        () -> FacetCountingUtils.validateFieldNames(new String[] { "field1", "invalidField" }, recordSchema));
    assertEquals(ex.getMessage(), "Field not found in schema: invalidField");
  }

  @Test(description = "Test validateFieldNames with null or empty array")
  public void testValidateFieldNamesWithNullOrEmptyArray() {
    String schemaJson =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[" + "{\"name\":\"field1\",\"type\":\"string\"}]}";
    Schema recordSchema = new Schema.Parser().parse(schemaJson);

    // Test null array
    IllegalArgumentException ex =
        expectThrows(IllegalArgumentException.class, () -> FacetCountingUtils.validateFieldNames(null, recordSchema));
    assertEquals(ex.getMessage(), "fieldNames cannot be null or empty");

    // Test empty array
    ex = expectThrows(
        IllegalArgumentException.class,
        () -> FacetCountingUtils.validateFieldNames(new String[0], recordSchema));
    assertEquals(ex.getMessage(), "fieldNames cannot be null or empty");
  }
}
