package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.testng.Assert.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controller.kafka.protocol.serializer.NewSemanticUsageValidator;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class NewSemanticUsageValidatorTest {
  @Test
  public void testIsNewEnumValue() {
    Schema currentSchema = AvroCompatibilityHelper.newEnumSchema(
        "currentEnum",
        "",
        "NewSemanticUsageValidatorTest",
        Arrays.asList("value1", "value2", "value3"),
        null);
    Schema.Field currentField = AvroCompatibilityHelper.createSchemaField("currentField", currentSchema, "", null);

    Schema targetSchema = AvroCompatibilityHelper.newEnumSchema(
        "targetEnum",
        "",
        "NewSemanticUsageValidatorTest",
        Arrays.asList("value1", "value2", "value3", "value4"),
        null);
    Schema.Field targetField = AvroCompatibilityHelper.createSchemaField("targetField", targetSchema, "", null);

    assertTrue(new NewSemanticUsageValidator().isNewEnumValue("value4", currentField, targetField));
  }

  @Test
  public void testIsNonDefaultValue() {
    // No default value
    String schemaJson = "{\"name\": \"fieldName\", \"type\": \"int\"}";
    Schema currentSchema = AvroCompatibilityHelper.parse(schemaJson);
    Schema.Field currentField = AvroCompatibilityHelper.createSchemaField("currentField", currentSchema, "", null);

    assertTrue(new NewSemanticUsageValidator().isNonDefaultValueField(1, currentField));

    // Default value is 10
    schemaJson = "{\"name\": \"fieldName\", \"type\": \"int\", \"default\": 10}";
    currentSchema = AvroCompatibilityHelper.parse(schemaJson);
    currentField = AvroCompatibilityHelper.createSchemaField("currentField", currentSchema, "", 10);
    // value equals to default of the field
    assertFalse(new NewSemanticUsageValidator().isNonDefaultValueField(10, currentField));
    // value equals to default of the type
    assertFalse(new NewSemanticUsageValidator().isNonDefaultValueField(0, currentField));

    // nested type
    schemaJson =
        "{\"type\": \"record\", \"name\": \"nestedRecord\", \"fields\": [{\"name\": \"nestedField\", \"type\": \"int\"}]}";
    currentSchema = AvroCompatibilityHelper.parse(schemaJson);
    currentField = AvroCompatibilityHelper.createSchemaField("currentField", currentSchema, "", null);
    assertFalse(new NewSemanticUsageValidator().isNonDefaultValueField(null, currentField));

    // even when the nested field has a default value, it is still considered non-default, the object should be null
    GenericRecord record = new GenericData.Record(currentSchema);
    record.put("nestedField", 0);
    assertTrue(new NewSemanticUsageValidator().isNonDefaultValueField(record, currentField));
  }

  @Test
  public void testIsNonDefaultValueUnion() {
    String intSchemaJson = "{\"name\": \"fieldName\", \"type\": \"int\", \"default\": 10}";
    Schema IntSchema = AvroCompatibilityHelper.parse(intSchemaJson);

    String longSchemaJson = "{\"name\": \"fieldName\", \"type\": \"long\"}";
    Schema LongSchema = AvroCompatibilityHelper.parse(longSchemaJson);

    Schema nullSchema = Schema.create(Schema.Type.NULL);
    Schema nullableUnionSchema = Schema.createUnion(Arrays.asList(IntSchema, nullSchema));
    Schema unionSchema = Schema.createUnion(Arrays.asList(IntSchema, LongSchema));

    // Case 1: When current field is nullable union
    Schema.Field currentField =
        AvroCompatibilityHelper.createSchemaField("currentField", nullableUnionSchema, "", null);
    // Even though the value is the same as the default value of target field, it is still considered as non-default
    // value for current field
    assertTrue(new NewSemanticUsageValidator().isNonDefaultValueUnion(10, currentField));

    // value equals to default of the type
    NewSemanticUsageValidator newSemanticUsageValidator = new NewSemanticUsageValidator();
    assertFalse(newSemanticUsageValidator.isNonDefaultValueUnion(0, currentField));

    // Case 2: when current field is non-nullable union
    Schema.Field currentField2 = AvroCompatibilityHelper.createSchemaField("currentField2", unionSchema, "", null);
    newSemanticUsageValidator = new NewSemanticUsageValidator();
    assertFalse(newSemanticUsageValidator.isNonDefaultValueUnion(0L, currentField2));

    // Case 3: current field is union, but value doesn't match any of the current field types
    Schema.Field currentField3 = AvroCompatibilityHelper.createSchemaField("currentField3", unionSchema, "", null);
    newSemanticUsageValidator = new NewSemanticUsageValidator();
    assertTrue(newSemanticUsageValidator.isNonDefaultValueUnion("123", currentField3));
    assertTrue(
        newSemanticUsageValidator.getErrorMessage()
            .contains("Field currentField3: Cannot find the match schema for value 123 from schema union"));
  }

  @Test

  public void testFindObjectSchemaInsideUnion() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
    String schemaJson =
        "{\"type\": \"record\", \"name\": \"nestedRecord\", \"fields\": [{\"name\": \"nestedField\", \"type\": \"int\"}]}";
    Schema recordSchema = AvroCompatibilityHelper.parse(schemaJson);
    Schema enumSchema = AvroCompatibilityHelper
        .newEnumSchema("enum", "", "NewSemanticUsageValidatorTest", Arrays.asList("value1", "value2", "value3"), null);

    Schema unionSchema = Schema.createUnion(
        Arrays.asList(
            stringSchema,
            intSchema,
            longSchema,
            floatSchema,
            doubleSchema,
            booleanSchema,
            bytesSchema,
            arraySchema,
            mapSchema,
            recordSchema,
            enumSchema));

    Schema.Field unionField = AvroCompatibilityHelper.createSchemaField("unionField", unionSchema, "", null);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion("10", unionField), stringSchema);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion(10, unionField), intSchema);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion(10L, unionField), longSchema);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion(10.0f, unionField), floatSchema);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion(10.0, unionField), doubleSchema);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion(true, unionField), booleanSchema);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion("bytes".getBytes(), unionField), bytesSchema);
    // Map
    assertEquals(
        NewSemanticUsageValidator.findObjectSchemaInsideUnion(Arrays.asList(1, 2, 3), unionField),
        arraySchema);
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("key", 1);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion(map, unionField), mapSchema);
    // Record
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("nestedField", 0);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion(record, unionField), recordSchema);
    // Enum
    Schema unionEnumSchema = Schema.createUnion(Arrays.asList(intSchema, enumSchema));
    Schema.Field enumField = AvroCompatibilityHelper.createSchemaField("enumField", unionEnumSchema, "", null);
    assertEquals(NewSemanticUsageValidator.findObjectSchemaInsideUnion("value1", enumField), enumSchema);

    // Invalid enum value
    assertNull(NewSemanticUsageValidator.findObjectSchemaInsideUnion("10", enumField), null);

    // Wrong types
    assertNull(NewSemanticUsageValidator.findObjectSchemaInsideUnion(10L, enumField));
  }
}
