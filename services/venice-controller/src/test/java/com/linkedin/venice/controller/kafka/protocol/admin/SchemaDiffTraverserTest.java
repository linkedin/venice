package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.kafka.protocol.serializer.SchemaDiffTraverser;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class SchemaDiffTraverserTest {
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
  private final Schema currentLatestSchema =
      adminOperationSerializer.getSchema(AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);

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
    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();

    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    // Traverse the admin message
    boolean isNewSemanticUsage =
        schemaDiffTraverser.traverse(adminMessage, null, currentSchema, targetSchema, "", filter);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    assertTrue(
        errorMessage.get()
            .contains("payloadUnion.UpdateStore.hybridStoreConfig.HybridStoreConfigRecord.realTimeTopicName"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get().contains("non-default value"),
        "The error message should contain the reason for the failure");
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
    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();
    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    boolean isUsingNewSemantic = schemaDiffTraverser.traverse(array, null, schema, targetSchema, "", filter);

    // Check if the flag is set to true
    // Check if the field name is as expected
    assertTrue(isUsingNewSemantic, "The flag should be set to true");
    assertTrue(
        errorMessage.get().contains("array.ExampleRecord.0.field2"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get().contains("non-default value"),
        "The error message should contain the reason for the failure");
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

    // Traverse the map
    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();

    // collect the pair fields
    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    boolean isUsingNewSemantic = schemaDiffTraverser.traverse(map, null, schema, targetSchema, "", filter);

    // Check if the flag is set to true
    assertTrue(isUsingNewSemantic, "The traverse should return true");
    // Check if the field name is as expected
    assertTrue(
        errorMessage.get().contains("map.ExampleRecord.key0.field2"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get().contains("non-default value"),
        "The error message should contain the reason for the failure");
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

    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();
    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    boolean isUsingNewSemantic = schemaDiffTraverser.traverse(map, null, currentSchema, targetSchema, "", filter);

    assertTrue(isUsingNewSemantic, "The traverse should return true");
    assertTrue(
        errorMessage.get().contains("map.ExampleRecord.key1.owners"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get()
            .contains("contains non-default value. Actual value: [owner]. Default value: [venice] or null"),
        "The error message should contain the reason for the failure");
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

    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();
    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    boolean isUsingNewSemantic =
        schemaDiffTraverser.traverse(adminMessage, null, currentSchema, targetSchema, "", filter);

    assertTrue(isUsingNewSemantic, "The traverse should return true");
    assertTrue(
        errorMessage.get().contains("payloadUnion.DeleteUnusedValueSchemas"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get().contains("is not in the target schema, and object is non-null"),
        "The error message should contain the reason for the failure");
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

    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();
    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    boolean isUsingNewSemantic = schemaDiffTraverser.traverse(array, null, currentSchema, targetSchema, "", filter);

    assertTrue(isUsingNewSemantic, "The traverse should return true");
    assertTrue(
        errorMessage.get().contains("array.ExampleRecord.0.field2"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get().contains("different types"),
        "The error message should contain the reason for the failure");
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
    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();

    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    // Traverse the admin message
    boolean isNewSemanticUsage =
        schemaDiffTraverser.traverse(adminMessage, null, currentSchema, targetSchema, "", filter);

    assertTrue(isNewSemanticUsage, "The flag should be set to true");
    assertTrue(
        errorMessage.get().contains("payloadUnion.UpdateStore.targetSwapRegionWaitTime"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get().contains("non-default value"),
        "The error message should contain the reason for the failure");

    // Test the case where the field is set as default value
    updateStore.targetSwapRegionWaitTime = 60;
    adminMessage.payloadUnion = updateStore;
    isNewSemanticUsage = schemaDiffTraverser.traverse(adminMessage, null, currentSchema, targetSchema, "", filter);
    assertFalse(isNewSemanticUsage, "The value is equal to the default value, should return false");
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

    SchemaDiffTraverser schemaDiffTraverser = new SchemaDiffTraverser();
    AtomicReference<String> errorMessage = new AtomicReference<>();

    BiFunction<Object, Pair<Schema.Field, Schema.Field>, Boolean> filter =
        schemaDiffTraverser.createSemanticCheck(errorMessage);

    boolean isUsingNewSemantic = schemaDiffTraverser.traverse(array, null, currentSchema, targetSchema, "", filter);

    assertTrue(isUsingNewSemantic, "The traverse should return true");
    assertTrue(
        errorMessage.get().contains("array.ExampleRecord.1.executionType"),
        "The error message should contain the field name");
    assertTrue(
        errorMessage.get().contains("new enum value"),
        "The error message should contain the reason for the failure");
  }
}
