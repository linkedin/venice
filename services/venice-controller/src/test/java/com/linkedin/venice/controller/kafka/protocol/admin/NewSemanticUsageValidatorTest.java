package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controller.kafka.protocol.serializer.NewSemanticUsageValidator;
import java.util.Arrays;
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

    assertTrue(new NewSemanticUsageValidator().hasSchemaTypeMismatch(currentField, targetField));
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
    String schemaJson = "{\"name\": \"fieldName\", \"type\": \"int\", \"default\": 10}";
    Schema IntSchema = AvroCompatibilityHelper.parse(schemaJson);

    Schema nullSchema = Schema.create(Schema.Type.NULL);
    Schema unionSchema = Schema.createUnion(Arrays.asList(IntSchema, nullSchema));

    Schema.Field currentField = AvroCompatibilityHelper.createSchemaField("currentField", unionSchema, "", null);
    Schema.Field targetField1 = AvroCompatibilityHelper.createSchemaField("targetField1", IntSchema, "", 10);
    // Even though the value is the same as the default value of target field, it is still considered as non-default
    // value
    // for current field
    assertTrue(new NewSemanticUsageValidator().isNonDefaultValueUnion(10, currentField, targetField1));

    // Case 2: when current field is union, target field is union
    String longSchemaJson = "{\"name\": \"fieldName\", \"type\": \"long\"}";
    Schema LongSchema = AvroCompatibilityHelper.parse(longSchemaJson);
    Schema unionSchema2 = Schema.createUnion(Arrays.asList(IntSchema, LongSchema));
    Schema.Field targetField2 = AvroCompatibilityHelper.createSchemaField("targetField2", unionSchema2, "", null);

    assertFalse(new NewSemanticUsageValidator().isNonDefaultValueUnion(0, currentField, targetField2));
  }
}
