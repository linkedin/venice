package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

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
    // Purposely set to true. This field doesn't exist in v74, so it should throw an exception.
    // Default value of this field is False.
    updateStore.separateRealTimeTopicEnabled = true;
    AdminOperation adminMessage = new AdminOperation();
    adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
    adminMessage.payloadUnion = updateStore;
    adminMessage.executionId = 1;

    doCallRealMethod().when(adminOperationSerializer).serialize(any(), anyInt());
    doCallRealMethod().when(adminOperationSerializer).deserialize(any(), anyInt());

    // Serialize the AdminOperation object with writer schema id v74
    try {
      adminOperationSerializer.serialize(adminMessage, 74);
    } catch (VeniceProtocolException e) {
      String expectedMessage =
          "Field AdminOperation.payloadUnion.UpdateStore.separateRealTimeTopicEnabled: Boolean value true is not the default value false or false";
      assertEquals(e.getMessage(), expectedMessage);
    }

    // Set the separateRealTimeTopicEnabled to false
    updateStore.separateRealTimeTopicEnabled = false;
    adminMessage.payloadUnion = updateStore;

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
}
