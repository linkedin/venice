package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.kafka.protocol.serializer.SemanticDetector;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceProtocolException;
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
    updateStore.blobDbEnabled = "NOT_SPECIFIED";

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
    testBadSemanticUsage(
        adminMessage,
        currentSchema,
        "",
        targetSchema,
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
    testBadSemanticUsage(
        array,
        schema,
        "",
        targetSchema,
        "Field array.0.ExampleRecord.field2: Integer value 123 is not the default value 0 or null");

    // Traverse the array with the correct value
    GenericData.Array<GenericRecord> array2 = new GenericData.Array<>(1, schema);
    array2.add(record2);

    SemanticDetector.traverseAndValidate(array2, schema, targetSchema, "", null);
  }

  @Test
  public void testTraverseUnion() {
    String recordSchemaString1 =
        "{\"type\": \"record\", \"name\": \"Record1\", \"fields\": [{\"name\": \"field1\", \"type\": \"string\"}, {\"name\": \"field2\", \"type\": \"int\"}]}";
    String recordSchemaString2 =
        "{\"type\": \"record\", \"name\": \"Record2\", \"fields\": [{\"name\": \"field1\", \"type\": \"string\"}, {\"name\": \"field2\", \"type\": \"int\"}]}";

    Schema recordSchema1 = AvroCompatibilityHelper.parse(recordSchemaString1);
    Schema recordSchema2 = AvroCompatibilityHelper.parse(recordSchemaString2);

    Schema currentSchema = Schema.createUnion(Arrays.asList(recordSchema1, recordSchema2));
    Schema targetSchema = Schema.createUnion(Arrays.asList(recordSchema1));

    GenericRecord record = new GenericData.Record(recordSchema2);
    record.put("field1", "exampleString");
    record.put("field2", 123);

    testBadSemanticUsage(
        record,
        currentSchema,
        "",
        targetSchema,
        "Field Record2: Value {\"field1\": \"exampleString\", \"field2\": 123} doesn't match default value null");
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
    testBadSemanticUsage(
        map,
        schema,
        "",
        targetSchema,
        "Field map.key0.ExampleRecord.field2: Integer value 123 is not the default value 0 or null");
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

    testBadSemanticUsage(
        map,
        currentSchema,
        "",
        targetSchema,
        "Field map.key1.ExampleRecord.owners: Value [owner] doesn't match default value [venice]");
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

    testBadSemanticUsage(
        adminMessage,
        currentSchema,
        "",
        targetSchema,
        "Field payloadUnion.DeleteUnusedValueSchemas: Value {\"clusterName\": \"clusterName\", \"storeName\": \"storeName\", \"schemaIds\": []} doesn't match default value null");
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

    testBadSemanticUsage(
        array,
        currentSchema,
        "",
        targetSchema,
        "Field array.0.ExampleRecord.field2: Type LONG is not the same as INT");
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
    updateStore.blobDbEnabled = "NOT_SPECIFIED";

    // Default value of this field is 60
    updateStore.targetSwapRegionWaitTime = 10;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;
    Schema targetSchema = adminOperationSerializer.getSchema(83);
    Schema currentSchema = currentLatestSchema;

    testBadSemanticUsage(
        adminMessage,
        currentSchema,
        "",
        targetSchema,
        "Field payloadUnion.UpdateStore.targetSwapRegionWaitTime: Integer value 10 is not the default value 0 or 60");

    // Test the case where the field is set as default value
    updateStore.targetSwapRegionWaitTime = 60;
    adminMessage.payloadUnion = updateStore;
    SemanticDetector.traverseAndValidate(adminMessage, currentSchema, targetSchema, "", null);
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
    record2.put("executionType", "STOP");

    GenericRecord record3 = new GenericData.Record(recordSchema);
    record3.put("field1", "");
    record3.put("field2", 0);
    record3.put("executionType", "COMPLETED");

    // Create an array and add the record to it
    GenericData.Array<GenericRecord> array1 = new GenericData.Array<>(2, currentSchema);
    array1.add(record1);
    array1.add(record2);

    testBadSemanticUsage(
        array1,
        currentSchema,
        "",
        targetSchema,
        "Field array.1.ExampleRecord.executionType: Enum value STOP is not accepted in the target schema [START, COMPLETED]");

    GenericData.Array<GenericRecord> array2 = new GenericData.Array<>(2, currentSchema);
    array2.add(record1);
    array2.add(record3);

    testBadSemanticUsage(
        array2,
        currentSchema,
        "",
        targetSchema,
        "Field array.1.ExampleRecord.executionType: Invalid enum value COMPLETED is not accepted in the current schema [START, STOP, PAUSE]");
  }

  @Test
  public void testValidateEnum() {
    Schema currentSchema = AvroCompatibilityHelper.newEnumSchema(
        "currentEnum",
        "",
        "NewSemanticUsageValidatorTest",
        Arrays.asList("value1", "value2", "value3", "value4"),
        null);

    Schema targetSchema = AvroCompatibilityHelper.newEnumSchema(
        "targetEnum",
        "",
        "NewSemanticUsageValidatorTest",
        Arrays.asList("value1", "value2", "value3"),
        null);

    SemanticDetector.validateEnum("value1", currentSchema, targetSchema, "fieldA");

    testBadSemanticUsage(
        "value4",
        currentSchema,
        "fieldA",
        targetSchema,
        "Field fieldA: Enum value value4 is not accepted in the target schema [value1, value2, value3]");

    testBadSemanticUsage(
        "value5",
        currentSchema,
        "fieldA",
        targetSchema,
        "Field fieldA: Invalid enum value value5 is not accepted in the current schema [value1, value2, value3, value4]");

    testBadSemanticUsage(1, currentSchema, "fieldB", targetSchema, "Field fieldB: Enum value 1 is not a string");
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

    assertEquals(SemanticDetector.getObjectSchema("10", unionSchema), stringSchema);
    assertEquals(SemanticDetector.getObjectSchema(10, unionSchema), intSchema);
    assertEquals(SemanticDetector.getObjectSchema(10L, unionSchema), longSchema);
    assertEquals(SemanticDetector.getObjectSchema(10.0f, unionSchema), floatSchema);
    assertEquals(SemanticDetector.getObjectSchema(10.0, unionSchema), doubleSchema);
    assertEquals(SemanticDetector.getObjectSchema(true, unionSchema), booleanSchema);
    assertEquals(SemanticDetector.getObjectSchema("bytes".getBytes(), unionSchema), bytesSchema);
    assertEquals(SemanticDetector.getObjectSchema(Arrays.asList(1, 2, 3), unionSchema), arraySchema);
    // Wrong element type
    assertNull(SemanticDetector.getObjectSchema(Arrays.asList("1", "2", "3"), unionSchema));
    // Map
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("key", 1);
    assertEquals(SemanticDetector.getObjectSchema(map, unionSchema), mapSchema);
    // Map with wrong value type
    HashMap<String, String> mapString = new HashMap<String, String>();
    mapString.put("key", "value");
    assertNull(SemanticDetector.getObjectSchema(mapString, unionSchema));

    // Record
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("nestedField", 0);
    assertEquals(SemanticDetector.getObjectSchema(record, unionSchema), recordSchema);

    // Null
    assertEquals(SemanticDetector.getObjectSchema(null, unionSchema), nullSchema);

    // Enum
    Schema unionEnumSchema = Schema.createUnion(Arrays.asList(intSchema, enumSchema));
    assertEquals(SemanticDetector.getObjectSchema("value1", unionEnumSchema), enumSchema);

    // Invalid enum value
    assertNull(SemanticDetector.getObjectSchema("10", unionEnumSchema));

    // Wrong types
    assertNull(SemanticDetector.getObjectSchema(10L, unionEnumSchema));
  }

  @Test
  public void testValidateDefaultValue() {
    // Integer
    testCompareObjectToDefaultValue(10, intSchema, 10, false);
    testCompareObjectToDefaultValue(0, intSchema, 10, false);

    // Cast error
    testCompareObjectToDefaultValue(10, intSchema, "10", true);

    // Long
    testCompareObjectToDefaultValue(10L, longSchema, 10L, false);
    testCompareObjectToDefaultValue(8L, longSchema, 10L, true);

    // Float
    testCompareObjectToDefaultValue(10.0f, floatSchema, 10.0f, false);
    testCompareObjectToDefaultValue(8.0f, floatSchema, 10.0f, true);

    // Double
    testCompareObjectToDefaultValue(10.0, doubleSchema, 10.0, false);
    testCompareObjectToDefaultValue(8, doubleSchema, 10.0, true);

    // Boolean
    testCompareObjectToDefaultValue(true, booleanSchema, null, true);
    testCompareObjectToDefaultValue(false, booleanSchema, null, false);

    // String
    testCompareObjectToDefaultValue("test", stringSchema, "test", false);
    testCompareObjectToDefaultValue(1, stringSchema, "test", true);

    // Bytes
    testCompareObjectToDefaultValue("test".getBytes(), bytesSchema, null, true);

    // Array
    testCompareObjectToDefaultValue(Arrays.asList(1, 2, 3), arraySchema, null, true);

    // Map
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("key", 1);
    testCompareObjectToDefaultValue(map, mapSchema, null, true);

    // Record
    String schemaJson =
        "{\"type\": \"record\", \"name\": \"nestedRecord\", \"fields\": [{\"name\": \"nestedField\", \"type\": \"int\"}]}";
    Schema recordSchema = AvroCompatibilityHelper.parse(schemaJson);
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("nestedField", 0);
    testCompareObjectToDefaultValue(record, recordSchema, null, true);
  }

  @Test
  public void testFixedType() {
    Schema currentFixedSchema = Schema.createFixed("testFixed", null, null, 10);
    Schema targetFixedSchema = Schema.createFixed("testFixed", null, null, 8);
    Object fixedValue = new byte[10];
    String fieldName = "fixedFieldName";

    // Fixed
    try {
      SemanticDetector.traverseAndValidate(fixedValue, currentFixedSchema, targetFixedSchema, fieldName, null);
    } catch (VeniceProtocolException e) {
      assertEquals(e.getMessage(), "Field fixedFieldName: Changing fixed size is not allowed.");
    }
  }

  private void testCompareObjectToDefaultValue(
      Object object,
      Schema schema,
      Object defaultValue,
      boolean shouldThrowException) {
    String fieldName = "testFieldName";
    if (shouldThrowException) {
      assertThrows(
          VeniceException.class,
          () -> SemanticDetector.compareObjectToDefaultValue(object, schema, fieldName, defaultValue));
    } else {
      SemanticDetector.compareObjectToDefaultValue(object, schema, fieldName, defaultValue);
    }
  }

  private void testBadSemanticUsage(
      Object object,
      Schema schema,
      String fieldName,
      Schema targetSchema,
      String expectedMessage) {
    try {
      SemanticDetector.traverseAndValidate(object, schema, targetSchema, fieldName, null);
      fail(); // Should not reach here
    } catch (VeniceProtocolException e) {
      assertEquals(e.getMessage(), expectedMessage);
    }
  }
}
