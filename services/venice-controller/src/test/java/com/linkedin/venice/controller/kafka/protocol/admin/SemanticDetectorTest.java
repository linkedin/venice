package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.testng.Assert.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.kafka.protocol.serializer.SemanticDetector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class SemanticDetectorTest {
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
  private final Schema currentLatestSchema =
      adminOperationSerializer.getSchema(AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
  private final Schema stringSchema = Schema.create(Schema.Type.STRING);
  private final Schema intSchema = Schema.create(Schema.Type.INT);
  private final Schema longSchema = Schema.create(Schema.Type.LONG);
  private final Schema floatSchema = Schema.create(Schema.Type.FLOAT);
  private final Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
  private final Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);
  private final Schema bytesSchema = Schema.create(Schema.Type.BYTES);
  private final Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
  private final Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
  private final Schema nullSchema = Schema.create(Schema.Type.NULL);

  @Test
  public void testTraverse() {
    // Create an AdminOperation object with latest version
    UpdateStore updateStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    updateStore.clusterName = "clusterName";
    updateStore.storeName = "storeName";
    updateStore.owner = "owner";
    updateStore.partitionNum = 20;
    updateStore.currentVersion = 1;
    updateStore.enableReads = true;
    updateStore.enableWrites = true;
    updateStore.replicateAllConfigs = true;
    updateStore.updatedConfigsList = Collections.emptyList();

    HybridStoreConfigRecord hybridStoreConfig = new HybridStoreConfigRecord();
    hybridStoreConfig.rewindTimeInSeconds = 123L;
    hybridStoreConfig.offsetLagThresholdToGoOnline = 1000L;
    hybridStoreConfig.producerTimestampLagThresholdToGoOnlineInSeconds = 300L;
    // Default value is empty string
    hybridStoreConfig.realTimeTopicName = "AAAA";
    updateStore.hybridStoreConfig = hybridStoreConfig;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;

    Schema targetSchema = adminOperationSerializer.getSchema(74);
    Schema currentSchema = currentLatestSchema;

    // Traverse the admin message
    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(adminMessage, currentSchema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertEquals(
        errorMessage,
        "Field payloadUnion.UpdateStore.hybridStoreConfig.HybridStoreConfigRecord.realTimeTopicName: String value AAAA is not the default value \"\" or ");
  }

  @Test
  public void testNestedArrayTraverse() {
    String schemaJson = "{" + "\"type\": \"array\"," + "\"items\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"int\"}" + "  ]" + "}" + "}";

    String targetSchemaJson =
        "{" + "\"type\": \"array\"," + "\"items\": {" + "  \"type\": \"record\"," + "  \"name\": \"ExampleRecord\","
            + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"}" + "  ]" + "}" + "}";

    Schema schema = AvroCompatibilityHelper.parse(schemaJson);
    Schema targetSchema = AvroCompatibilityHelper.parse(targetSchemaJson);

    // Create records for the schema
    Schema recordSchema = schema.getElementType();
    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("field1", "exampleString");
    record1.put("field2", 123);

    GenericRecord record2 = new GenericData.Record(recordSchema);
    record2.put("field1", "");
    record2.put("field2", 0);

    // Create an array and add the record to it
    GenericData.Array<GenericRecord> array = new GenericData.Array<>(1, schema);
    array.add(record1);
    array.add(record2);

    // Traverse the array
    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(array, schema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertEquals(
        errorMessage,
        "Field array.ExampleRecord.0.field2: Integer value 123 is not the default value 0 or null");
  }

  @Test
  public void testNestedMapTraverse() {
    String schemaJson = "{" + "\"type\": \"map\"," + "\"values\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"int\"}" + "  ]" + "}" + "}";

    String targetSchemaJson =
        "{" + "\"type\": \"map\"," + "\"values\": {" + "  \"type\": \"record\"," + "  \"name\": \"ExampleRecord\","
            + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"}" + "  ]" + "}" + "}";

    Schema schema = AvroCompatibilityHelper.parse(schemaJson);
    Schema targetSchema = AvroCompatibilityHelper.parse(targetSchemaJson);

    // Create records for the schema
    Schema recordSchema = schema.getValueType();
    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("field1", "exampleString");
    record1.put("field2", 123);

    GenericRecord record2 = new GenericData.Record(recordSchema);
    record2.put("field1", "exampleString");
    record2.put("field2", 0);

    // Create a map and add the record to it
    HashMap<String, Object> map = new HashMap<>();
    map.put("key0", record1);
    map.put("key1", record2);

    // collect the pair fields
    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(map, schema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertEquals(
        errorMessage,
        "Field map.ExampleRecord.key0.field2: Integer value 123 is not the default value 0 or null");
  }

  @Test
  public void testDefaultValueOfArray() {
    String schemaJson = "{" + "\"type\": \"map\"," + "\"values\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"int\"}," + "    {\"name\": \"owners\", \"type\": {"
        + "      \"type\": \"array\", \"items\": \"string\"}, \"default\": [\"venice\"]" + "    }" + "  ]" + "}" + "}";

    String targetSchemaJson = "{" + "\"type\": \"map\"," + "\"values\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"int\"}" + "  ]" + "}" + "}";

    Schema currentSchema = AvroCompatibilityHelper.parse(schemaJson);
    Schema targetSchema = AvroCompatibilityHelper.parse(targetSchemaJson);

    // Create records for the schema
    Schema recordSchema = currentSchema.getValueType();
    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("field1", "exampleString");
    record1.put("field2", 123);
    record1.put("owners", new ArrayList<>());

    GenericRecord record2 = new GenericData.Record(recordSchema);
    record2.put("field1", "exampleString");
    record2.put("field2", 0);
    record2.put("owners", new ArrayList<>(Collections.singletonList("owner")));

    // Create a map and add the record to it
    HashMap<String, Object> map = new HashMap<>();
    map.put("key0", record1);
    map.put("key1", record2);

    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(map, currentSchema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertEquals(
        errorMessage,
        "Field map.ExampleRecord.key1.owners: Value [owner] doesn't match default value [venice]");
  }

  @Test()
  public void testNewRecordFieldInCurrentSchema() {
    DeleteUnusedValueSchemas deleteUnusedValueSchemas = new DeleteUnusedValueSchemas();
    deleteUnusedValueSchemas.clusterName = "clusterName";
    deleteUnusedValueSchemas.storeName = "storeName";
    deleteUnusedValueSchemas.schemaIds = new ArrayList<>();

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA.getValue();
    adminMessage.payloadUnion = deleteUnusedValueSchemas;
    adminMessage.executionId = 1;

    Schema targetSchema = adminOperationSerializer.getSchema(74);
    Schema currentSchema = currentLatestSchema;

    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(adminMessage, currentSchema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertTrue(
        errorMessage.contains("Field payloadUnion.DeleteUnusedValueSchemas"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.contains("doesn't match default value null"),
        "New semantic is detected at RECORD level, expecting null");
  }

  @Test
  public void testDifferentTypesForField() {
    String schemaJson = "{" + "\"type\": \"array\"," + "\"items\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"long\"}" + "  ]" + "}" + "}";

    String targetSchemaJson = "{" + "\"type\": \"array\"," + "\"items\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"int\"}" + "  ]" + "}" + "}";

    Schema currentSchema = AvroCompatibilityHelper.parse(schemaJson);
    Schema targetSchema = AvroCompatibilityHelper.parse(targetSchemaJson);

    // Create records for the schema
    Schema recordSchema = currentSchema.getElementType();
    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("field1", "exampleString");
    record1.put("field2", 123);

    GenericRecord record2 = new GenericData.Record(recordSchema);
    record2.put("field1", "");
    record2.put("field2", 0);

    // Create an array and add the record to it
    GenericData.Array<GenericRecord> array = new GenericData.Array<>(2, currentSchema);
    array.add(record1);
    array.add(record2);

    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(array, currentSchema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertEquals(errorMessage, "Field array.ExampleRecord.0.field2: Type LONG is not the same as INT");
  }

  @Test
  public void testDefaultFieldValue() {
    // Create an AdminOperation object with latest version
    UpdateStore updateStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    updateStore.clusterName = "clusterName";
    updateStore.storeName = "storeName";
    updateStore.owner = "owner";
    updateStore.partitionNum = 20;
    updateStore.currentVersion = 1;
    updateStore.enableReads = true;
    updateStore.enableWrites = true;
    updateStore.replicateAllConfigs = true;
    updateStore.updatedConfigsList = Collections.emptyList();

    // Default value of this field is 60
    updateStore.targetSwapRegionWaitTime = 10;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;
    Schema targetSchema = adminOperationSerializer.getSchema(83);
    Schema currentSchema = currentLatestSchema;

    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(adminMessage, currentSchema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertEquals(
        errorMessage,
        "Field payloadUnion.UpdateStore.targetSwapRegionWaitTime: Integer value 10 is not the default value 0 or 60");

    // Test the case where the field is set as default value
    updateStore.targetSwapRegionWaitTime = 60;
    adminMessage.payloadUnion = updateStore;
    isNewSemanticUsage = semanticDetector.traverse(adminMessage, currentSchema, targetSchema, "", null);
    assertFalse(isNewSemanticUsage, "The flag should be set to false");
  }

  @Test
  public void testEnumNewValue() {
    String schemaJson = "{" + "\"type\": \"array\"," + "\"items\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"long\"}," + "    {\"name\": \"executionType\", \"type\": {"
        + "      \"type\": \"enum\", \"name\": \"ExecutionType\", \"symbols\": [\"START\", \"STOP\", \"PAUSE\"]}"
        + "    }" + "  ]" + "}" + "}";

    String targetSchemaJson = "{" + "\"type\": \"array\"," + "\"items\": {" + "  \"type\": \"record\","
        + "  \"name\": \"ExampleRecord\"," + "  \"fields\": [" + "    {\"name\": \"field1\", \"type\": \"string\"},"
        + "    {\"name\": \"field2\", \"type\": \"long\"}," + "    {\"name\": \"executionType\", \"type\": {"
        + "      \"type\": \"enum\", \"name\": \"ExecutionType\", \"symbols\": [\"START\", \"COMPLETED\"]}" + "    }"
        + "  ]" + "}" + "}";

    Schema currentSchema = AvroCompatibilityHelper.parse(schemaJson);
    Schema targetSchema = AvroCompatibilityHelper.parse(targetSchemaJson);

    // Create records for the schema
    Schema recordSchema = currentSchema.getElementType();
    GenericRecord record1 = new GenericData.Record(recordSchema);
    record1.put("field1", "exampleString");
    record1.put("field2", 123);
    record1.put("executionType", "START");

    GenericRecord record2 = new GenericData.Record(recordSchema);
    record2.put("field1", "");
    record2.put("field2", 0);
    record2.put("executionType", "COMPLETED");

    // Create an array and add the record to it
    GenericData.Array<GenericRecord> array = new GenericData.Array<>(2, currentSchema);
    array.add(record1);
    array.add(record2);

    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.traverse(array, currentSchema, targetSchema, "", null);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    String errorMessage = semanticDetector.getErrorMessage();
    assertEquals(
        errorMessage,
        "Field array.ExampleRecord.1.executionType: Enum value COMPLETED is not in the previous enum symbols but in the target enum symbols");
  }

  @Test
  public void testValidateEnum() {
    Schema currentSchema = AvroCompatibilityHelper.newEnumSchema(
        "currentEnum",
        "",
        "NewSemanticUsageValidatorTest",
        Arrays.asList("value1", "value2", "value3"),
        null);

    Schema targetSchema = AvroCompatibilityHelper.newEnumSchema(
        "targetEnum",
        "",
        "NewSemanticUsageValidatorTest",
        Arrays.asList("value1", "value2", "value3", "value4"),
        null);

    SemanticDetector semanticDetector = new SemanticDetector();
    boolean isNewSemanticUsage = semanticDetector.validateEnum("value4", currentSchema, targetSchema, "fieldA");
    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    assertEquals(
        semanticDetector.getErrorMessage(),
        "Field fieldA: Enum value value4 is not in the previous enum symbols but in the target enum symbols");

    isNewSemanticUsage = semanticDetector.validateEnum("value1", currentSchema, targetSchema, "fieldB");
    assertFalse(isNewSemanticUsage, "The flag should be set to false since value1 appears in both schemas");

    isNewSemanticUsage = semanticDetector.validateEnum(1, currentSchema, targetSchema, "fieldC");
    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    assertEquals(semanticDetector.getErrorMessage(), "Field fieldC: Enum value 1 is not a string");
  }

  @Test
  public void testFindObjectSchemaInsideUnion() {
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
            nullSchema,
            enumSchema));

    SemanticDetector semanticDetector = new SemanticDetector();

    assertEquals(semanticDetector.getObjectSchema("10", unionSchema), stringSchema);
    assertEquals(semanticDetector.getObjectSchema(10, unionSchema), intSchema);
    assertEquals(semanticDetector.getObjectSchema(10L, unionSchema), longSchema);
    assertEquals(semanticDetector.getObjectSchema(10.0f, unionSchema), floatSchema);
    assertEquals(semanticDetector.getObjectSchema(10.0, unionSchema), doubleSchema);
    assertEquals(semanticDetector.getObjectSchema(true, unionSchema), booleanSchema);
    assertEquals(semanticDetector.getObjectSchema("bytes".getBytes(), unionSchema), bytesSchema);
    assertEquals(semanticDetector.getObjectSchema(Arrays.asList(1, 2, 3), unionSchema), arraySchema);
    // Wrong element type
    assertEquals(semanticDetector.getObjectSchema(Arrays.asList("1", "2", "3"), unionSchema), null);
    // Map
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("key", 1);
    assertEquals(semanticDetector.getObjectSchema(map, unionSchema), mapSchema);
    // Map with wrong value type
    HashMap<String, String> mapString = new HashMap<String, String>();
    mapString.put("key", "value");
    assertEquals(semanticDetector.getObjectSchema(mapString, unionSchema), null);

    // Record
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("nestedField", 0);
    assertEquals(semanticDetector.getObjectSchema(record, unionSchema), recordSchema);

    // Null
    assertEquals(semanticDetector.getObjectSchema(null, unionSchema), nullSchema);

    // Enum
    Schema unionEnumSchema = Schema.createUnion(Arrays.asList(intSchema, enumSchema));
    assertEquals(semanticDetector.getObjectSchema("value1", unionEnumSchema), enumSchema);

    // Invalid enum value
    assertNull(semanticDetector.getObjectSchema("10", unionEnumSchema), null);

    // Wrong types
    assertNull(semanticDetector.getObjectSchema(10L, unionEnumSchema));
  }

  @Test
  public void testValidateDefaultValue() {
    SemanticDetector semanticDetector = new SemanticDetector();
    String fieldName = "testFieldName";

    // Integer
    assertTrue(semanticDetector.validate(10, intSchema, fieldName, null));
    assertFalse(semanticDetector.validate(0, intSchema, fieldName, null));

    // Cast error
    assertTrue(semanticDetector.validate(10L, intSchema, fieldName, null));

    // Long
    assertTrue(semanticDetector.validate(10L, longSchema, fieldName, null));
    assertFalse(semanticDetector.validate(10L, longSchema, fieldName, 10L));

    // Float
    assertTrue(semanticDetector.validate(10.0f, floatSchema, fieldName, null));
    assertFalse(semanticDetector.validate(10.0f, floatSchema, fieldName, 10.0f));

    // Double
    assertTrue(semanticDetector.validate(10.0, doubleSchema, fieldName, null));
    assertFalse(semanticDetector.validate(10.0, doubleSchema, fieldName, 10.0));

    // Boolean
    assertTrue(semanticDetector.validate(true, booleanSchema, fieldName, null));
    assertFalse(semanticDetector.validate(false, booleanSchema, fieldName, null));

    // String
    assertTrue(semanticDetector.validate("test", stringSchema, fieldName, null));
    assertFalse(semanticDetector.validate("test", stringSchema, fieldName, "test"));

    // Bytes
    assertTrue(semanticDetector.validate("test".getBytes(), bytesSchema, fieldName, null));

    // Array
    assertTrue(semanticDetector.validate(Arrays.asList(1, 2, 3), arraySchema, fieldName, null));

    // Map
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("key", 1);
    assertTrue(semanticDetector.validate(map, mapSchema, fieldName, null));

    // Record
    String schemaJson =
        "{\"type\": \"record\", \"name\": \"nestedRecord\", \"fields\": [{\"name\": \"nestedField\", \"type\": \"int\"}]}";
    Schema recordSchema = AvroCompatibilityHelper.parse(schemaJson);
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("nestedField", 0);
    assertTrue(semanticDetector.validate(record, recordSchema, fieldName, null));
  }

  @Test
  public void testFixedType() {
    Schema currentFixedSchema = Schema.createFixed("testFixed", null, null, 10);
    Schema targetFixedSchema = Schema.createFixed("testFixed", null, null, 8);

    Object fixedValue = new byte[10];

    SemanticDetector semanticDetector = new SemanticDetector();
    String fieldName = "fixedFieldName";

    // Fixed
    assertTrue(semanticDetector.traverse(fixedValue, currentFixedSchema, targetFixedSchema, fieldName, null));
    assertEquals(semanticDetector.getErrorMessage(), "Field fixedFieldName: Changing fixed size is not allowed.");
  }
}
