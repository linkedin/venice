package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.exceptions.VeniceProtocolException;
import com.linkedin.venice.helix.HelixReadOnlyZKSharedSchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class AdminOperationSerializerTest {
  @Test
  public void testGetSchema() {
    AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
    expectThrows(VeniceProtocolException.class, () -> adminOperationSerializer.getSchema(0));
    expectThrows(
        VeniceProtocolException.class,
        () -> adminOperationSerializer.getSchema(AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION + 1));
  }

  @Test
  public void testAdminOperationSerializer() {
    AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

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
    updateStore.separateRealTimeTopicEnabled = false;
    updateStore.storeLifecycleHooks = Collections.emptyList();
    updateStore.blobTransferInServerEnabled = "NOT_SPECIFIED";
    updateStore.blobDbEnabled = "NOT_SPECIFIED";
    updateStore.keyUrnFields = Collections.emptyList();
    updateStore.blobDbEnabled = "NOT_SPECIFIED";
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;

    // Serialize the AdminOperation object with writer schema id v74, should not fail
    byte[] serializedBytes = adminOperationSerializer.serialize(adminMessage, 74);
    AdminOperation deserializedOperation = adminOperationSerializer.deserialize(ByteBuffer.wrap(serializedBytes), 74);
    UpdateStore deserializedOperationPayloadUnion = (UpdateStore) deserializedOperation.getPayloadUnion();
    assertEquals(deserializedOperationPayloadUnion.clusterName.toString(), "clusterName");
    assertEquals(deserializedOperationPayloadUnion.storeName.toString(), "storeName");
    assertEquals(deserializedOperationPayloadUnion.owner.toString(), "owner");
    assertEquals(deserializedOperationPayloadUnion.partitionNum, 20);
    assertEquals(deserializedOperationPayloadUnion.currentVersion, 1);
    assertTrue(deserializedOperationPayloadUnion.enableReads);
    assertTrue(deserializedOperationPayloadUnion.enableWrites);
    assertTrue(deserializedOperationPayloadUnion.replicateAllConfigs);
    assertEquals(deserializedOperationPayloadUnion.updatedConfigsList, Collections.emptyList());

    // Check value of new semantics are all set to default value
    assertFalse(deserializedOperationPayloadUnion.separateRealTimeTopicEnabled);
    assertEquals(deserializedOperationPayloadUnion.maxRecordSizeBytes, -1);
    assertEquals(deserializedOperationPayloadUnion.maxNearlineRecordSizeBytes, -1);
    assertFalse(deserializedOperationPayloadUnion.unusedSchemaDeletionEnabled);
    assertFalse(deserializedOperationPayloadUnion.blobTransferEnabled);
    assertTrue(deserializedOperationPayloadUnion.nearlineProducerCompressionEnabled);
    assertEquals(deserializedOperationPayloadUnion.nearlineProducerCountPerWriter, 1);
    assertNull(deserializedOperationPayloadUnion.targetSwapRegion);
    assertEquals(deserializedOperationPayloadUnion.targetSwapRegionWaitTime, 60);
    assertFalse(deserializedOperationPayloadUnion.isDaVinciHeartBeatReported);
    assertEquals(deserializedOperationPayloadUnion.blobDbEnabled.toString(), "NOT_SPECIFIED");
  }

  @Test
  public void testValidateAdminOperation() {
    AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

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
    // Purposely set to true. This field doesn't exist in v74, so it should throw an exception.
    // Default value of this field is False.
    updateStore.separateRealTimeTopicEnabled = true;
    updateStore.blobTransferInServerEnabled = "NOT_SPECIFIED";
    updateStore.blobDbEnabled = "NOT_SPECIFIED";
    updateStore.keyUrnFields = Collections.emptyList();
    updateStore.storeLifecycleHooks = Collections.emptyList();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;

    // Serialize the AdminOperation object with writer schema id v74
    try {
      adminOperationSerializer.validate(adminMessage, 74);
    } catch (VeniceProtocolException e) {
      String expectedMessage =
          "Current schema version: 74. New semantic is being used. Field AdminOperation.payloadUnion.UpdateStore.separateRealTimeTopicEnabled: Boolean value true is not the default value false or false";
      assertEquals(e.getMessage(), expectedMessage);
    }

    try {
      // Validate should fail when target schema id is greater than the latest schema id
      adminOperationSerializer
          .validate(adminMessage, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION + 1);
    } catch (VeniceProtocolException e) {
      assertTrue(e.getMessage().contains("We don't support serialization to future schema versions."));
    }
  }

  @Test
  public void testSerializeDeserializeWithDocChange() {
    AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();

    // Create an AdminOperation object with latest version
    AddVersion addVersion = (AddVersion) AdminMessageType.ADD_VERSION.getNewInstance();
    addVersion.clusterName = "clusterName";
    addVersion.storeName = "storeName";
    addVersion.pushJobId = "pushJobId";
    addVersion.versionNum = 1;
    addVersion.numberOfPartitions = 20;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.ADD_VERSION.getValue();
    adminMessage.payloadUnion = addVersion;
    adminMessage.executionId = 1;

    // Serialize the AdminOperation object with writer schema id v24
    byte[] serializedBytes = adminOperationSerializer.serialize(adminMessage, 24);

    // Deserialize the AdminOperation object with reader schema id v25
    AdminOperation deserializedOperation = adminOperationSerializer.deserialize(ByteBuffer.wrap(serializedBytes), 25);
    AddVersion deserializedOperationPayloadUnion = (AddVersion) deserializedOperation.getPayloadUnion();
    assertEquals(deserializedOperationPayloadUnion.clusterName.toString(), "clusterName");
    assertEquals(deserializedOperationPayloadUnion.storeName.toString(), "storeName");
    assertEquals(deserializedOperationPayloadUnion.pushJobId.toString(), "pushJobId");
    assertEquals(deserializedOperationPayloadUnion.versionNum, 1);
    assertEquals(deserializedOperationPayloadUnion.numberOfPartitions, 20);
  }

  /**
   * Test downloading and storing schema if it's absent in the protocol map.
   * 1st attempt: schema not found in system store schema repository, expect VeniceProtocolException.
   * 2nd attempt: schema found in system store schema repository, but error downloading schema, expect VeniceProtocolException.
   * 3rd attempt: schema found in system store schema repository, schema downloaded successfully and added to protocol map.
   */
  @Test
  public void testDownloadAndSchemaIfNecessary() {
    AdminOperationSerializer adminOperationSerializer = Mockito.spy(new AdminOperationSerializer());

    VeniceHelixAdmin mockAdmin = Mockito.mock(VeniceHelixAdmin.class);
    Schema mockSchema = Mockito.mock(Schema.class);
    SchemaEntry mockSchemaEntry = Mockito.mock(SchemaEntry.class);
    HelixReadOnlyZKSharedSchemaRepository mockSchemaRepo = Mockito.mock(HelixReadOnlyZKSharedSchemaRepository.class);
    when(mockAdmin.getReadOnlyZKSharedSchemaRepository()).thenReturn(mockSchemaRepo);
    when(mockSchemaRepo.getValueSchema(anyString(), anyInt())).thenReturn(mockSchemaEntry);

    int nonExistSchemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION + 1;
    doCallRealMethod().when(adminOperationSerializer).fetchAndStoreSchemaIfAbsent(any(), anyInt());

    // hasValueSchema: 1st call: schema not exists; 2nd call & 3rd call: schema exists
    when(mockSchemaRepo.hasValueSchema(anyString(), anyInt())).thenReturn(false).thenReturn(true).thenReturn(true);

    // getSchema First call returns null (for 2nd attempt), second call returns the mock schema (for 3rd attempt)
    when(mockSchemaEntry.getSchema()).thenReturn(null).thenReturn(mockSchema);

    // 1st attempt: schema not exist in system store schema repository
    try {
      adminOperationSerializer.fetchAndStoreSchemaIfAbsent(mockAdmin, nonExistSchemaId);
    } catch (VeniceProtocolException e) {
      String expectedMessage = "Failed to fetch schema id: " + nonExistSchemaId
          + " from system store schema repository as it does not exist.";
      assertEquals(e.getMessage(), expectedMessage);
    }

    // 2nd attempt: error downloading schema from system store schema repository
    try {
      adminOperationSerializer.fetchAndStoreSchemaIfAbsent(mockAdmin, nonExistSchemaId);
    } catch (VeniceProtocolException e) {
      String expectedMessage = "Failed to fetch schema id: " + nonExistSchemaId
          + " from system store schema repository even though it exists.";
      assertEquals(e.getMessage(), expectedMessage);
    }

    // 3rd attempt: schema found in system store schema repository
    adminOperationSerializer.fetchAndStoreSchemaIfAbsent(mockAdmin, nonExistSchemaId);
    // Verify schema is downloaded and added to the protocol map
    assertEquals(adminOperationSerializer.getSchema(nonExistSchemaId), mockSchema);

    // Clean up the protocol map by removing the added mock schema
    adminOperationSerializer.removeSchema(nonExistSchemaId);
  }
}
