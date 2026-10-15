package com.linkedin.venice;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.helix.HelixStoreGraveyard;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestRecoverStoreMetadata {
  @DataProvider(name = "writeQuotaEnabledValues")
  public Object[][] writeQuotaEnabledValues() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "writeQuotaEnabledValues")
  public void testRecoveryRestoresWriteQuotaThroughUpdate(boolean enabled) throws Exception {
    String cluster = "test-cluster";
    String storeName = "test-store";
    String url = "http://localhost:1234";
    String schema = "\"string\"";
    Store deletedStore = TestUtils.createTestStore(storeName, "owner", 1L);
    deletedStore.setWriteQuotaEnabled(enabled);
    ControllerClient controller = mock(ControllerClient.class);
    when(controller.createNewStore(storeName, "owner", schema, schema)).thenReturn(new NewStoreResponse());
    when(controller.updateStore(eq(storeName), any())).thenReturn(new ControllerResponse());

    StoreCreation creation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
    creation.clusterName = cluster;
    creation.storeName = storeName;
    creation.owner = "owner";
    creation.keySchema = new SchemaMeta(SchemaType.AVRO_1_4.getValue(), schema);
    creation.valueSchema = new SchemaMeta(SchemaType.AVRO_1_4.getValue(), schema);
    AdminOperation operation = new AdminOperation();
    operation.operationType = AdminMessageType.STORE_CREATION.getValue();
    operation.payloadUnion = creation;
    Put put = new Put();
    put.schemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;
    put.putValue = ByteBuffer.wrap(new AdminOperationSerializer().serialize(operation, put.schemaId));
    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.messageType = MessageType.PUT.getValue();
    envelope.payloadUnion = put;
    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    when(record.getValue()).thenReturn(envelope);
    PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
    when(consumer.poll(anyLong())).thenReturn(
        Collections.singletonMap(mock(PubSubTopicPartition.class), Collections.singletonList(record)),
        Collections.emptyMap());

    try (MockedStatic<ControllerClient> clients = mockStatic(ControllerClient.class);
        MockedStatic<ControllerClientFactory> factory = mockStatic(ControllerClientFactory.class);
        MockedConstruction<HelixStoreGraveyard> ignored =
            mockConstruction(HelixStoreGraveyard.class, (graveyard, context) -> {
              when(graveyard.listStoreNamesFromGraveyard(cluster)).thenReturn(Collections.singletonList(storeName));
              when(graveyard.getStoreFromGraveyard(cluster, storeName, null)).thenReturn(deletedStore);
            })) {
      clients.when(() -> ControllerClient.discoverCluster(url, storeName, Optional.empty(), 3))
          .thenReturn(new D2ServiceDiscoveryResponse());
      factory.when(() -> ControllerClientFactory.getControllerClient(cluster, url, Optional.empty()))
          .thenReturn(controller);

      RecoverStoreMetadata.recover(
          mock(ZkClient.class),
          consumer,
          Optional.empty(),
          url,
          storeName,
          false,
          true,
          Collections.singletonList(cluster),
          cluster);

      InOrder order = inOrder(controller);
      order.verify(controller).createNewStore(storeName, "owner", schema, schema);
      ArgumentCaptor<UpdateStoreQueryParams> params = ArgumentCaptor.forClass(UpdateStoreQueryParams.class);
      order.verify(controller).updateStore(eq(storeName), params.capture());
      assertEquals(params.getValue().getWriteQuotaEnabled(), Optional.of(enabled));
    }
  }
}
