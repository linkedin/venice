package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteUnusedValueSchemas;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.MetadataSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaUsageResponse;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link ParentSchemaOrchestrator}, exercised through the public schema API of
 * {@link VeniceParentHelixAdmin} which delegates to the orchestrator. These tests were extracted from
 * {@code TestVeniceParentHelixAdmin} when the parent-controller schema orchestration moved into
 * {@link ParentSchemaOrchestrator}.
 */
public class TestParentSchemaOrchestrator extends AbstractTestVeniceParentHelixAdmin {
  @BeforeMethod
  public void setupTestCase() {
    setupInternalMocks();
    initializeParentAdmin(Optional.empty(), Optional.empty());
  }

  @AfterMethod
  public void cleanupTestCase() {
    super.cleanupTestCase();
  }

  @Test
  public void testAddValueSchema() {
    String storeName = "test-store";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    int valueSchemaId = 10;
    String valueSchemaStr = "\"string\"";
    doReturn(valueSchemaId).when(storeSchemaManager)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(
            clusterName,
            storeName,
            valueSchemaStr,
            DirectionalSchemaCompatibilityType.FULL);
    doReturn(valueSchemaId).when(internalAdmin).getValueSchemaId(clusterName, storeName, valueSchemaStr);

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);

    verify(storeSchemaManager).checkPreConditionForAddValueSchemaAndGetNewSchemaId(
        clusterName,
        storeName,
        valueSchemaStr,
        DirectionalSchemaCompatibilityType.FULL);
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());

    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter).put(
        keyCaptor.capture(),
        valueCaptor.capture(),
        schemaCaptor.capture(),
        any(),
        any(),
        anyLong(),
        any(),
        any(),
        any(),
        any());

    byte[] keyBytes = keyCaptor.getValue();
    byte[] valueBytes = valueCaptor.getValue();
    int schemaId = schemaCaptor.getValue();
    assertEquals(schemaId, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
    assertEquals(keyBytes.length, 0);

    AdminOperation adminMessage = adminOperationSerializer.deserialize(ByteBuffer.wrap(valueBytes), schemaId);
    assertEquals(adminMessage.operationType, AdminMessageType.VALUE_SCHEMA_CREATION.getValue());

    ValueSchemaCreation valueSchemaCreationMessage = (ValueSchemaCreation) adminMessage.payloadUnion;
    assertEquals(valueSchemaCreationMessage.clusterName.toString(), clusterName);
    assertEquals(valueSchemaCreationMessage.storeName.toString(), storeName);
    assertEquals(valueSchemaCreationMessage.schema.definition.toString(), valueSchemaStr);
    assertEquals(valueSchemaCreationMessage.schemaId, valueSchemaId);
  }

  /**
   * {@code deleteValueSchemas} broadcasts a {@link AdminMessageType#DELETE_UNUSED_VALUE_SCHEMA} admin message when none
   * of the requested schema ids are reported in-use by any child region. This orchestration moved verbatim from
   * {@code VeniceParentHelixAdmin} into {@link ParentSchemaOrchestrator}.
   */
  @Test
  public void testDeleteValueSchemasBroadcastsWhenRequestedIdsAreUnused() {
    String storeName = "test-store-delete-schemas";
    // A child region reports schema ids 2 and 3 as in-use; the deletion targets 4 and 5, which are not in use.
    ControllerClient regionClient = mock(ControllerClient.class);
    SchemaUsageResponse usageResponse = new SchemaUsageResponse();
    usageResponse.setInUseValueSchemaIds(new HashSet<>(Arrays.asList(2, 3)));
    doReturn(usageResponse).when(regionClient).getInUseSchemaIds(storeName);
    controllerClients.put(regionName, regionClient);

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.deleteValueSchemas(clusterName, storeName, new HashSet<>(Arrays.asList(4, 5)));

    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter)
        .put(any(), valueCaptor.capture(), schemaCaptor.capture(), any(), any(), anyLong(), any(), any(), any(), any());

    AdminOperation adminMessage =
        adminOperationSerializer.deserialize(ByteBuffer.wrap(valueCaptor.getValue()), schemaCaptor.getValue());
    assertEquals(adminMessage.operationType, AdminMessageType.DELETE_UNUSED_VALUE_SCHEMA.getValue());
    DeleteUnusedValueSchemas payload = (DeleteUnusedValueSchemas) adminMessage.payloadUnion;
    assertEquals(payload.clusterName.toString(), clusterName);
    assertEquals(payload.storeName.toString(), storeName);
    Set<Integer> broadcastSchemaIds = new HashSet<>(payload.schemaIds);
    assertEquals(broadcastSchemaIds.size(), 2);
    assertTrue(broadcastSchemaIds.contains(4));
    assertTrue(broadcastSchemaIds.contains(5));
  }

  /**
   * {@code deleteValueSchemas} skips the broadcast entirely when no schema ids are reported in-use: an empty in-use set
   * is treated as "nothing to act on", so the method returns without sending a delete message.
   */
  @Test
  public void testDeleteValueSchemasSkipsWhenNoSchemasInUse() {
    String storeName = "test-store-delete-none-in-use";
    ControllerClient regionClient = mock(ControllerClient.class);
    SchemaUsageResponse usageResponse = new SchemaUsageResponse();
    usageResponse.setInUseValueSchemaIds(Collections.emptySet());
    doReturn(usageResponse).when(regionClient).getInUseSchemaIds(storeName);
    controllerClients.put(regionName, regionClient);

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.deleteValueSchemas(clusterName, storeName, new HashSet<>(Arrays.asList(4, 5)));

    verify(veniceWriter, never()).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
  }

  /**
   * {@code deleteValueSchemas} skips the broadcast when any requested id is still in use in a child region, to avoid
   * deleting a value schema that a running store version depends on.
   */
  @Test
  public void testDeleteValueSchemasSkipsWhenRequestedIdIsInUse() {
    String storeName = "test-store-delete-in-use";
    ControllerClient regionClient = mock(ControllerClient.class);
    SchemaUsageResponse usageResponse = new SchemaUsageResponse();
    usageResponse.setInUseValueSchemaIds(new HashSet<>(Arrays.asList(3, 4)));
    doReturn(usageResponse).when(regionClient).getInUseSchemaIds(storeName);
    controllerClients.put(regionName, regionClient);

    parentAdmin.initStorageCluster(clusterName);
    // Request to delete ids 3 (in use) and 5 (unused); the overlap on id 3 must abort the whole deletion.
    parentAdmin.deleteValueSchemas(clusterName, storeName, new HashSet<>(Arrays.asList(3, 5)));

    verify(veniceWriter, never()).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
  }

  /**
   * Regression guard: when write computation is enabled, {@code addValueSchema} must also broadcast a
   * {@link AdminMessageType#DERIVED_SCHEMA_CREATION} admin message so the derived (write-compute) schema is replicated
   * to child fabrics. A local schema-repo write would not propagate, leaving child controllers without the derived
   * schema.
   */
  @Test
  public void testAddValueSchemaWithWriteComputePropagatesDerivedSchema() {
    String storeName = "test-store-wc";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setWriteComputationEnabled(true);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);
    // No existing superset/latest value schema, so the superset-generation branch is skipped.
    doReturn(null).when(storeSchemaManager).getSupersetOrLatestValueSchema(eq(clusterName), any(Store.class));

    int valueSchemaId = 1;
    String valueSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"int\",\"default\":0}]}";
    doReturn(valueSchemaId).when(storeSchemaManager)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(
            clusterName,
            storeName,
            valueSchemaStr,
            DirectionalSchemaCompatibilityType.FULL);
    doReturn(valueSchemaId).when(internalAdmin).getValueSchemaId(clusterName, storeName, valueSchemaStr);
    doReturn(1).when(storeSchemaManager)
        .checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(
            eq(clusterName),
            eq(storeName),
            eq(valueSchemaId),
            anyString());

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);

    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter, atLeast(2))
        .put(any(), valueCaptor.capture(), schemaCaptor.capture(), any(), any(), anyLong(), any(), any(), any(), any());

    boolean sawValueSchemaCreation = false;
    boolean sawDerivedSchemaCreation = false;
    List<byte[]> values = valueCaptor.getAllValues();
    List<Integer> schemas = schemaCaptor.getAllValues();
    for (int i = 0; i < values.size(); i++) {
      AdminOperation adminMessage =
          adminOperationSerializer.deserialize(ByteBuffer.wrap(values.get(i)), schemas.get(i));
      if (adminMessage.operationType == AdminMessageType.VALUE_SCHEMA_CREATION.getValue()) {
        sawValueSchemaCreation = true;
      } else if (adminMessage.operationType == AdminMessageType.DERIVED_SCHEMA_CREATION.getValue()) {
        sawDerivedSchemaCreation = true;
      }
    }
    assertTrue(sawValueSchemaCreation, "Expected a VALUE_SCHEMA_CREATION admin message");
    assertTrue(
        sawDerivedSchemaCreation,
        "addValueSchema with write computation enabled must broadcast a DERIVED_SCHEMA_CREATION admin message");
  }

  @Test
  public void testAddDerivedSchema() {
    String storeName = "test-store";
    String derivedSchemaStr = "\"string\"";
    int valueSchemaId = 10;
    int derivedSchemaId = 1;

    doReturn(derivedSchemaId).when(storeSchemaManager)
        .checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaId, derivedSchemaStr);

    doReturn(new GeneratedSchemaID(valueSchemaId, derivedSchemaId)).when(internalAdmin)
        .getDerivedSchemaId(clusterName, storeName, derivedSchemaStr);

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.addDerivedSchema(clusterName, storeName, valueSchemaId, derivedSchemaStr);

    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter)
        .put(any(), valueCaptor.capture(), schemaCaptor.capture(), any(), any(), anyLong(), any(), any(), any(), any());

    AdminOperation adminMessage =
        adminOperationSerializer.deserialize(ByteBuffer.wrap(valueCaptor.getValue()), schemaCaptor.getValue());
    DerivedSchemaCreation derivedSchemaCreation = (DerivedSchemaCreation) adminMessage.payloadUnion;

    assertEquals(derivedSchemaCreation.clusterName.toString(), clusterName);
    assertEquals(derivedSchemaCreation.storeName.toString(), storeName);
    assertEquals(derivedSchemaCreation.schema.definition.toString(), derivedSchemaStr);
    assertEquals(derivedSchemaCreation.valueSchemaId, valueSchemaId);
    assertEquals(derivedSchemaCreation.derivedSchemaId, derivedSchemaId);
  }

  @Test
  public void testAddValueSchemaDuplicateReturnsExistingIdWithoutAdminMessage() {
    String storeName = "dup-value-store";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    String valueSchemaStr = "\"string\"";
    doReturn(SchemaData.DUPLICATE_VALUE_SCHEMA_CODE).when(storeSchemaManager)
        .checkPreConditionForAddValueSchemaAndGetNewSchemaId(
            clusterName,
            storeName,
            valueSchemaStr,
            DirectionalSchemaCompatibilityType.FULL);
    doReturn(42).when(internalAdmin).getValueSchemaId(clusterName, storeName, valueSchemaStr);

    parentAdmin.initStorageCluster(clusterName);
    SchemaEntry result =
        parentAdmin.addValueSchema(clusterName, storeName, valueSchemaStr, DirectionalSchemaCompatibilityType.FULL);

    assertEquals(result.getId(), 42);
    // Duplicate schema short-circuits before any admin message is broadcast.
    verify(veniceWriter, never()).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testAddDerivedSchemaDuplicateReturnsExistingIdWithoutAdminMessage() {
    String storeName = "dup-derived-store";
    String derivedSchemaStr = "\"string\"";
    int valueSchemaId = 3;

    doReturn(SchemaData.DUPLICATE_VALUE_SCHEMA_CODE).when(storeSchemaManager)
        .checkPreConditionForAddDerivedSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaId, derivedSchemaStr);
    doReturn(new GeneratedSchemaID(valueSchemaId, 5)).when(internalAdmin)
        .getDerivedSchemaId(clusterName, storeName, derivedSchemaStr);

    parentAdmin.initStorageCluster(clusterName);
    DerivedSchemaEntry result = parentAdmin.addDerivedSchema(clusterName, storeName, valueSchemaId, derivedSchemaStr);

    assertEquals(result.getValueSchemaID(), valueSchemaId);
    assertEquals(result.getId(), 5);
    verify(veniceWriter, never()).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testAddReplicationMetadataSchemaBroadcastsAndValidates() {
    String storeName = "rmd-store";
    String rmdSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0}]}";
    int valueSchemaId = 1;
    int rmdVersionId = 1;

    doReturn(false).when(storeSchemaManager).checkIfMetadataSchemaAlreadyPresent(eq(clusterName), eq(storeName), any());
    Schema parsed = new Schema.Parser().parse(rmdSchemaStr);
    doReturn(Optional.of(parsed)).when(internalAdmin)
        .getReplicationMetadataSchema(clusterName, storeName, valueSchemaId, rmdVersionId);

    parentAdmin.initStorageCluster(clusterName);
    RmdSchemaEntry result =
        parentAdmin.addReplicationMetadataSchema(clusterName, storeName, valueSchemaId, rmdVersionId, rmdSchemaStr);

    assertEquals(result.getValueSchemaID(), valueSchemaId);
    assertEquals(result.getId(), rmdVersionId);
    // A REPLICATION_METADATA_SCHEMA_CREATION admin message is broadcast.
    verify(veniceWriter).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testAddReplicationMetadataSchemaAlreadyPresentSkipsAdminMessage() {
    String storeName = "rmd-present-store";
    String rmdSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0}]}";
    int valueSchemaId = 1;
    int rmdVersionId = 1;

    doReturn(true).when(storeSchemaManager).checkIfMetadataSchemaAlreadyPresent(eq(clusterName), eq(storeName), any());

    parentAdmin.initStorageCluster(clusterName);
    RmdSchemaEntry result =
        parentAdmin.addReplicationMetadataSchema(clusterName, storeName, valueSchemaId, rmdVersionId, rmdSchemaStr);

    assertEquals(result.getValueSchemaID(), valueSchemaId);
    verify(veniceWriter, never()).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testUpdateReplicationMetadataSchemaForAllValueSchemaSkipsWhenAlreadyPresent() {
    String storeName = "rmd-all-store";
    String valueSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0}]}";
    int valueSchemaId = 1;

    doReturn(Collections.singletonList(new SchemaEntry(valueSchemaId, valueSchemaStr))).when(internalAdmin)
        .getValueSchemas(clusterName, storeName);
    // store == null forces getRmdVersionID to fall back to the cluster config (which returns RMD version 1).
    doReturn(null).when(internalAdmin).getStore(clusterName, storeName);
    doReturn(true).when(storeSchemaManager)
        .checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, valueSchemaId, 1);

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.updateReplicationMetadataSchemaForAllValueSchema(clusterName, storeName);

    verify(storeSchemaManager).checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, valueSchemaId, 1);
    // Value schema already has an RMD schema, so nothing is broadcast.
    verify(veniceWriter, never()).put(any(), any(), anyInt(), any(), any(), anyLong(), any(), any(), any(), any());
  }

  @Test
  public void testActiveActiveAddValueSchemaUpdatesRmdForNewValueSchemaWhenSupersetUnchanged() {
    String storeName = "aa-superset-unchanged-store";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setActiveActiveReplicationEnabled(true);
    store.setLatestSuperSetValueSchemaId(10);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    int newValueSchemaId = 11;
    String newValueSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0}]}";
    String existingSupersetSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0},{\"name\":\"f2\",\"type\":\"string\",\"default\":\"\"}]}";
    Schema existingSupersetSchema = new Schema.Parser().parse(existingSupersetSchemaStr);

    doReturn(existingSupersetSchema).when(storeSchemaManager)
        .getSupersetOrLatestValueSchema(eq(clusterName), eq(store));
    doReturn(newValueSchemaId).when(internalAdmin).getValueSchemaId(clusterName, storeName, newValueSchemaStr);
    doReturn(true).when(storeSchemaManager)
        .checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, newValueSchemaId, 1);

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.addValueSchema(
        clusterName,
        storeName,
        newValueSchemaStr,
        newValueSchemaId,
        DirectionalSchemaCompatibilityType.FULL);

    verify(storeSchemaManager).checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, newValueSchemaId, 1);
    verify(storeSchemaManager, never()).checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, 10, 1);
  }

  /**
   * Content-correctness guard for the intentional A/A RMD fix (commits 49270e531 / ab2357598). The sibling test above
   * pins WHICH value-schema id drives RMD registration but short-circuits (valueSchemaAlreadyHasRmdSchema=true) before
   * any RMD is generated. This test drives the full registration path (valueSchemaAlreadyHasRmdSchema=false) and asserts
   * the broadcast RMD schema is generated from the JUST-ADDED value schema ({f1}) rather than the unchanged superset
   * ({f1, f2}) -- the actual semantic of the fix.
   */
  @Test
  public void testActiveActiveAddValueSchemaRegistersRmdGeneratedFromNewValueSchemaNotSuperset() {
    String storeName = "aa-rmd-content-store";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.setActiveActiveReplicationEnabled(true);
    store.setLatestSuperSetValueSchemaId(10);
    doReturn(store).when(internalAdmin).getStore(clusterName, storeName);

    int newValueSchemaId = 11;
    String newValueSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0}]}";
    String existingSupersetSchemaStr =
        "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\",\"default\":0},{\"name\":\"f2\",\"type\":\"string\",\"default\":\"\"}]}";
    Schema newValueSchema = new Schema.Parser().parse(newValueSchemaStr);
    Schema existingSupersetSchema = new Schema.Parser().parse(existingSupersetSchemaStr);

    doReturn(existingSupersetSchema).when(storeSchemaManager)
        .getSupersetOrLatestValueSchema(eq(clusterName), eq(store));
    doReturn(newValueSchemaId).when(internalAdmin).getValueSchemaId(clusterName, storeName, newValueSchemaStr);
    // RMD does not yet exist for the new value schema, so the orchestrator must generate and broadcast it.
    doReturn(false).when(storeSchemaManager)
        .checkIfValueSchemaAlreadyHasRmdSchema(clusterName, storeName, newValueSchemaId, 1);
    doReturn(false).when(storeSchemaManager).checkIfMetadataSchemaAlreadyPresent(eq(clusterName), eq(storeName), any());
    // The RMD generated from the NEW value schema is what the post-broadcast validation reads back.
    Schema expectedRmdSchema = RmdSchemaGenerator.generateMetadataSchema(newValueSchema, 1);
    doReturn(Optional.of(expectedRmdSchema)).when(internalAdmin)
        .getReplicationMetadataSchema(clusterName, storeName, newValueSchemaId, 1);

    parentAdmin.initStorageCluster(clusterName);
    parentAdmin.addValueSchema(
        clusterName,
        storeName,
        newValueSchemaStr,
        newValueSchemaId,
        DirectionalSchemaCompatibilityType.FULL);

    // Capture the broadcast REPLICATION_METADATA_SCHEMA_CREATION message.
    ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<Integer> schemaCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(veniceWriter, atLeast(1))
        .put(any(), valueCaptor.capture(), schemaCaptor.capture(), any(), any(), anyLong(), any(), any(), any(), any());

    MetadataSchemaCreation rmdCreation = null;
    List<byte[]> values = valueCaptor.getAllValues();
    List<Integer> schemas = schemaCaptor.getAllValues();
    for (int i = 0; i < values.size(); i++) {
      AdminOperation adminMessage =
          adminOperationSerializer.deserialize(ByteBuffer.wrap(values.get(i)), schemas.get(i));
      if (adminMessage.operationType == AdminMessageType.REPLICATION_METADATA_SCHEMA_CREATION.getValue()) {
        rmdCreation = (MetadataSchemaCreation) adminMessage.payloadUnion;
      }
    }

    assertNotNull(rmdCreation, "Expected a REPLICATION_METADATA_SCHEMA_CREATION admin message to be broadcast");
    // The RMD must be registered against the just-added value schema id, not the superset id.
    assertEquals(rmdCreation.valueSchemaId, newValueSchemaId);

    // The RMD schema content must derive from the new value schema {f1}, not the superset {f1, f2}.
    Schema broadcastRmdSchema = new Schema.Parser().parse(rmdCreation.metadataSchema.definition.toString());
    Schema supersetRmdSchema = RmdSchemaGenerator.generateMetadataSchema(existingSupersetSchema, 1);
    assertTrue(
        AvroSchemaUtils.compareSchemaIgnoreFieldOrder(broadcastRmdSchema, expectedRmdSchema),
        "Broadcast RMD schema must be generated from the just-added value schema, not the superset");
    assertFalse(
        AvroSchemaUtils.compareSchemaIgnoreFieldOrder(broadcastRmdSchema, supersetRmdSchema),
        "Broadcast RMD schema must NOT match the RMD generated from the superset schema");
  }
}
