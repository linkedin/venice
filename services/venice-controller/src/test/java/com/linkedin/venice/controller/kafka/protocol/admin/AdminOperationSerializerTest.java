package com.linkedin.venice.controller.kafka.protocol.admin;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.intThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.exceptions.VeniceMessageException;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.avro.Schema;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class AdminOperationSerializerTest {
  private AdminOperationSerializer adminOperationSerializer = Mockito.mock(AdminOperationSerializer.class);

  @Test
  public void testGetSchema() {
    expectThrows(VeniceMessageException.class, () -> AdminOperationSerializer.getSchema(0));
    expectThrows(
        VeniceMessageException.class,
        () -> AdminOperationSerializer.getSchema(AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION + 1));
  }

  @Test
  public void testAdminOperationSerializer() {
    try (MockedStatic<AdminOperationSerializer> mockedStatic = mockStatic(AdminOperationSerializer.class)) {
      // For schema IDs <= 0, throw exception.
      mockedStatic.when(() -> AdminOperationSerializer.getSchema(intThat(arg -> arg <= 0)))
          .thenThrow(new VeniceMessageException("Admin operation schema version: " + 0 + " doesn't exist"));

      // For schema IDs > 0 and <= 84, call the real method.
      mockedStatic.when(() -> AdminOperationSerializer.getSchema(intThat(arg -> arg > 0 && arg <= 84)))
          .thenCallRealMethod();

      // For schema IDs > 84, return a mock schema (for demonstration).
      Schema mockSchema = mock(Schema.class);
      mockedStatic.when(() -> AdminOperationSerializer.getSchema(intThat(arg -> arg > 84))).thenReturn(mockSchema);

      // Create an AdminOperation object with v84
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
      // Purposely set to true. This field doesn't exist in v74, will be dropped during serialization
      // Default value of this field is False.
      updateStore.separateRealTimeTopicEnabled = true;
      AdminOperation adminMessage = new AdminOperation();
      adminMessage.operationType = AdminMessageType.UPDATE_STORE.getValue();
      adminMessage.payloadUnion = updateStore;
      adminMessage.executionId = 1;

      doCallRealMethod().when(adminOperationSerializer).serialize(any(), anyInt());
      doCallRealMethod().when(adminOperationSerializer).deserialize(any(), anyInt());

      // Serialize the AdminOperation object with writer schema id v74
      byte[] serializedBytes = adminOperationSerializer.serialize(adminMessage, 74);

      // Deserialize the serialized bytes back into an AdminOperation object
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
      // The field separateRealTimeTopicEnabled should be set to false (default value) after deserialization
      assertFalse(deserializedOperationPayloadUnion.separateRealTimeTopicEnabled);
    }
  }
}
