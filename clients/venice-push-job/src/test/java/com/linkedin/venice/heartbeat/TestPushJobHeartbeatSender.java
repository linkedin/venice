package com.linkedin.venice.heartbeat;

import static org.mockito.Mockito.*;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPushJobHeartbeatSender {
  @Test
  public void testHeartbeatSenderCreation() {
    String kafkaUrl = "localhost:1234";
    String heartbeatStoreName = AvroProtocolDefinition.BATCH_JOB_HEARTBEAT.getSystemStoreName();
    VeniceProperties properties = VeniceProperties.empty();
    Optional<Properties> sslProperties = Optional.empty();
    DefaultPushJobHeartbeatSenderFactory pushJobHeartbeatSenderFactory = new DefaultPushJobHeartbeatSenderFactory();

    // Prepare controller client.
    ControllerClient controllerClient = mock(ControllerClient.class);
    StoreResponse storeResponse = mock(StoreResponse.class);
    StoreInfo storeInfo = mock(StoreInfo.class, RETURNS_DEEP_STUBS);
    PartitionerConfig partitionerConfig = new PartitionerConfigImpl();
    when(storeInfo.getHybridStoreConfig().getRealTimeTopicName())
        .thenReturn(Utils.composeRealTimeTopic(heartbeatStoreName));
    doReturn(1).when(storeInfo).getPartitionCount();
    doReturn(partitionerConfig).when(storeInfo).getPartitionerConfig();
    doReturn(storeInfo).when(storeResponse).getStore();
    doReturn(storeResponse).when(controllerClient).getStore(heartbeatStoreName);

    // Value Schema prepare.
    MultiSchemaResponse multiSchemaResponse = mock(MultiSchemaResponse.class);
    MultiSchemaResponse.Schema valueSchema = mock(MultiSchemaResponse.Schema.class);
    doReturn(BatchJobHeartbeatValue.SCHEMA$.toString()).when(valueSchema).getSchemaStr();
    MultiSchemaResponse.Schema[] valueSchemas = { valueSchema };
    doReturn(valueSchemas).when(multiSchemaResponse).getSchemas();

    // Key schema prepare.
    SchemaResponse keySchemaResponse = mock(SchemaResponse.class);
    doReturn(BatchJobHeartbeatKey.SCHEMA$.toString()).when(keySchemaResponse).getSchemaStr();
    doReturn(keySchemaResponse).when(controllerClient).getKeySchema(heartbeatStoreName);
    doReturn(multiSchemaResponse).when(controllerClient).getAllValueSchema(heartbeatStoreName);

    PushJobHeartbeatSender pushJobHeartbeatSender =
        pushJobHeartbeatSenderFactory.createHeartbeatSender(kafkaUrl, properties, controllerClient, sslProperties);
    Assert.assertNotNull(pushJobHeartbeatSender);
    Assert.assertTrue(pushJobHeartbeatSender instanceof DefaultPushJobHeartbeatSender);
    DefaultPushJobHeartbeatSender defaultPushJobHeartbeatSender =
        (DefaultPushJobHeartbeatSender) pushJobHeartbeatSender;
    Assert.assertEquals(
        defaultPushJobHeartbeatSender.getVeniceWriter().getTopicName(),
        Utils.composeRealTimeTopic(heartbeatStoreName));
  }
}
