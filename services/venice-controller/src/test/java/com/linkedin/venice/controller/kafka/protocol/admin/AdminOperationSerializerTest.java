package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.exceptions.VeniceProtocolException;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class AdminOperationSerializerTest {
  private AdminOperationSerializer adminOperationSerializer = Mockito.mock(AdminOperationSerializer.class);

  @Test
  public void testGetSchema() {
    expectThrows(VeniceProtocolException.class, () -> AdminOperationSerializer.getSchema(0));
    expectThrows(
        VeniceProtocolException.class,
        () -> AdminOperationSerializer.getSchema(AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION + 1));
  }

  @Test
  public void testAdminOperationSerializer() {
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
    updateStore.keyUrnFields = Collections.emptyList();
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;

    doCallRealMethod().when(adminOperationSerializer).serialize(any(), anyInt());
    doCallRealMethod().when(adminOperationSerializer).deserialize(any(), anyInt());

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
  }

  @Test
  public void testValidateAdminOperationWithNonExistedField() {
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

    // Initialize fields that might be null to avoid NPE during serialization
    updateStore.updatedConfigsList = Collections.emptyList();
    updateStore.storeLifecycleHooks = Collections.emptyList();
    updateStore.blobTransferInServerEnabled = "NOT_SPECIFIED";
    updateStore.keyUrnFields = Collections.emptyList();

    // Purposely set to true. This field doesn't exist in v74.
    // Default value of this field is False.
    updateStore.separateRealTimeTopicEnabled = true;

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;

    doCallRealMethod().when(adminOperationSerializer).serialize(any(), anyInt());
    doCallRealMethod().when(adminOperationSerializer).deserialize(any(), anyInt());

    // Validate with v74 schema - should PASS now because unknown fields [separateRealTimeTopicEnabled] are filtered
    // The separateRealTimeTopicEnabled field will be dropped during filtering then the validation should pass
    try {
      AdminOperationSerializer.validate(adminMessage, 74);
      assertTrue(true, "Validation should pass when unknown fields are filtered");
    } catch (VeniceProtocolException e) {
      // This should not happen
      fail("Validation should not fail when unknown fields are filtered, but got: " + e.getMessage());
    }
  }

  @Test
  public void testValidateAdminOperationWithExistedFields() {
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

    // Initialize fields that might be null to avoid NPE during serialization
    updateStore.updatedConfigsList = Collections.emptyList();
    updateStore.storeLifecycleHooks = Collections.emptyList();
    updateStore.blobTransferInServerEnabled = "NOT_SPECIFIED";
    updateStore.keyUrnFields = Collections.emptyList();

    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;

    doCallRealMethod().when(adminOperationSerializer).serialize(any(), anyInt());
    doCallRealMethod().when(adminOperationSerializer).deserialize(any(), anyInt());

    // Validate with v74 schema - should PASS because all fields exist in v74
    try {
      AdminOperationSerializer.validate(adminMessage, 74);
      assertTrue(true, "Validation should pass when all fields exist in target schema");
    } catch (VeniceProtocolException e) {
      fail("Validation should not fail when all fields exist in target schema, but got: " + e.getMessage());
    }
  }
}
