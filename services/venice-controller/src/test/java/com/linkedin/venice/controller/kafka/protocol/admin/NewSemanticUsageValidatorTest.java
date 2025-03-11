package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controller.kafka.protocol.serializer.NewSemanticUsageValidator;
import com.linkedin.venice.utils.Pair;
import java.util.Arrays;
import java.util.HashMap;
import java.util.function.BiFunction;
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
  public void testHasSchemaTypeMismatch() {
    String schemaJson = "{\"name\": \"fieldName\", \"type\": \"int\"}";

    String targetSchemaJson = "{\"name\": \"fieldName\", \"type\": \"long\"}";

    Schema currentSchema = AvroCompatibilityHelper.parse(schemaJson);
    Schema.Field currentField = AvroCompatibilityHelper.createSchemaField("currentField", currentSchema, "", null);

    Schema targetSchema = AvroCompatibilityHelper.parse(targetSchemaJson);
    Schema.Field targetField = AvroCompatibilityHelper.createSchemaField("targetField", targetSchema, "", null);

    NewSemanticUsageValidator newSemanticUsageValidator = new NewSemanticUsageValidator();
    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator =
        newSemanticUsageValidator.getSemanticValidator();
    assertTrue(validator.apply(1, new Pair<>(currentField, targetField)));
    assertTrue(newSemanticUsageValidator.getErrorMessage().contains("Type mismatch INT vs LONG"));

    // nested type - nullable union
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    Schema unionSchema = Schema.createUnion(Arrays.asList(currentSchema, nullSchema));

    Schema.Field nullableField = AvroCompatibilityHelper.createSchemaField("nullableField", unionSchema, "", null);
    Schema.Field intField = AvroCompatibilityHelper.createSchemaField("intField", currentSchema, "", 10);

    newSemanticUsageValidator = new NewSemanticUsageValidator();
    validator = newSemanticUsageValidator.getSemanticValidator();
    assertFalse(validator.apply(0, new Pair<>(nullableField, intField)));
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
    // Case 1: When current field is union, target field is not union
    String intSchemaJson = "{\"name\": \"fieldName\", \"type\": \"int\", \"default\": 10}";
    Schema IntSchema = AvroCompatibilityHelper.parse(intSchemaJson);

    String longSchemaJson = "{\"name\": \"fieldName\", \"type\": \"long\"}";
    Schema LongSchema = AvroCompatibilityHelper.parse(longSchemaJson);

    Schema nullSchema = Schema.create(Schema.Type.NULL);
    Schema nullableUnionSchema = Schema.createUnion(Arrays.asList(IntSchema, nullSchema));

    Schema.Field currentField =
        AvroCompatibilityHelper.createSchemaField("currentField", nullableUnionSchema, "", null);
    Schema.Field targetField1 = AvroCompatibilityHelper.createSchemaField("targetField1", IntSchema, "", 10);
    // Even though the value is the same as the default value of target field, it is still considered as non-default
    // value for current field
    assertTrue(new NewSemanticUsageValidator().isNonDefaultValueUnion(10, currentField, targetField1));

    NewSemanticUsageValidator newSemanticUsageValidator = new NewSemanticUsageValidator();
    Schema.Field targetLongField = AvroCompatibilityHelper.createSchemaField("targetLongField", LongSchema, "", null);
    assertTrue(newSemanticUsageValidator.isNonDefaultValueUnion(10, currentField, targetLongField));
    assertTrue(newSemanticUsageValidator.getErrorMessage().contains("Type mismatch INT vs LONG"));

    // Case 2: when current field is union, target field is union
    Schema unionSchema = Schema.createUnion(Arrays.asList(IntSchema, LongSchema));
    Schema.Field targetField2 = AvroCompatibilityHelper.createSchemaField("targetField2", unionSchema, "", null);

    newSemanticUsageValidator = new NewSemanticUsageValidator();
    assertTrue(newSemanticUsageValidator.isNonDefaultValueUnion(0, currentField, targetField2));
    System.out.println(newSemanticUsageValidator.getErrorMessage());
    assertTrue(newSemanticUsageValidator.getErrorMessage().contains("Type mismatch INT vs UNION"));

    // Case 3: current field is union, target field is union, but value doesnt match any of the current field types
    Schema.Field currentField3 = AvroCompatibilityHelper.createSchemaField("currentField3", unionSchema, "", null);
    newSemanticUsageValidator = new NewSemanticUsageValidator();
    assertTrue(newSemanticUsageValidator.isNonDefaultValueUnion("123", currentField3, targetField2));
    assertTrue(
        newSemanticUsageValidator.getErrorMessage()
            .contains("Field currentField3: Cannot find the match schema for value 123 from schema union"));
  }

  @Test

  public void testCastValueToSchema() {
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

    // Cast to string
    assertEquals(NewSemanticUsageValidator.castValueToSchema("10", stringSchema), "10");
    // Cast to int
    assertEquals(NewSemanticUsageValidator.castValueToSchema(10, intSchema), 10);
    // Cast to long
    assertEquals(NewSemanticUsageValidator.castValueToSchema(10L, longSchema), 10L);
    // Cast to float
    assertEquals(NewSemanticUsageValidator.castValueToSchema(10.0f, floatSchema), 10.0f);
    // Cast to double
    assertEquals(NewSemanticUsageValidator.castValueToSchema(10.0, doubleSchema), 10.0);
    // Cast to boolean
    assertEquals(NewSemanticUsageValidator.castValueToSchema(true, booleanSchema), true);
    // Cast to bytes
    assertEquals(NewSemanticUsageValidator.castValueToSchema("bytes".getBytes(), bytesSchema), "bytes".getBytes());
    // Cast to array
    assertEquals(
        NewSemanticUsageValidator.castValueToSchema(Arrays.asList(1, 2, 3), arraySchema),
        Arrays.asList(1, 2, 3));
    // Cast to map
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("key", 1);
    assertEquals(NewSemanticUsageValidator.castValueToSchema(map, mapSchema), map);
    // Cast to record
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("nestedField", 0);
    assertEquals(NewSemanticUsageValidator.castValueToSchema(record, recordSchema), record);
    // Cast to enum
    assertEquals(NewSemanticUsageValidator.castValueToSchema("value1", enumSchema), "value1");

    try {
      NewSemanticUsageValidator.castValueToSchema("10", enumSchema);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Value 10 does not match schema type ENUM");
    }

    // Wrong types
    try {
      NewSemanticUsageValidator.castValueToSchema(10, stringSchema);
    } catch (IllegalArgumentException e) {
      assertEquals(e.getMessage(), "Value 10 does not match schema type STRING");
    }
  }

  @Test
  public void testHandleNullableUnion() {
    Schema nullSchema = Schema.create(Schema.Type.NULL);
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema unionSchema = Schema.createUnion(Arrays.asList(intSchema, nullSchema));
    Schema.Field currentField = AvroCompatibilityHelper.createSchemaField("currentField", unionSchema, "", null);
    Schema.Field targetField = AvroCompatibilityHelper.createSchemaField("targetField", intSchema, "", 10);

    NewSemanticUsageValidator newSemanticUsageValidator = new NewSemanticUsageValidator();
    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> validator =
        newSemanticUsageValidator.getSemanticValidator();
    // Non default value
    assertTrue(validator.apply(10, new Pair<>(currentField, targetField)));
    assertTrue(newSemanticUsageValidator.getErrorMessage().contains("non-default value"));

    // Default value
    assertFalse(validator.apply(null, new Pair<>(currentField, targetField)));

    // Different types
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema.Field longField = AvroCompatibilityHelper.createSchemaField("longField", longSchema, "", null);
    assertTrue(validator.apply(10, new Pair<>(currentField, longField)));
    assertTrue(newSemanticUsageValidator.getErrorMessage().contains("Type mismatch INT vs LONG"));

    // nullable union with null targetField
    newSemanticUsageValidator = new NewSemanticUsageValidator();
    assertTrue(newSemanticUsageValidator.isNonDefaultValueUnion(123, currentField, null));
    assertTrue(
        newSemanticUsageValidator.getErrorMessage()
            .contains("Field: currentField contains non-default value. Actual value: 123. Default value: null or 0"));
  }
}
