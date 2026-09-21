package com.linkedin.venice.heartbeat;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestPushJobHeartbeatSender {
  @DataProvider
  public Object[][] encryptionKeys() {
    return new Object[][] { { "urn:test:key:1" }, { "" }, { null } };
  }

  @Test(dataProvider = "encryptionKeys")
  public void testHeartbeatSenderCreation(String keyUrn) {
    String kafkaUrl = "localhost:1234";
    String heartbeatStoreName = AvroProtocolDefinition.BATCH_JOB_HEARTBEAT.getSystemStoreName();
    VeniceProperties properties = VeniceProperties.empty();
    Optional<Properties> sslProperties = Optional.empty();
    DefaultPushJobHeartbeatSenderFactory pushJobHeartbeatSenderFactory =
        spy(new DefaultPushJobHeartbeatSenderFactory());
    VeniceWriter<byte[], byte[], byte[]> writer = mock(VeniceWriter.class);
    doReturn(Utils.composeRealTimeTopic(heartbeatStoreName)).when(writer).getTopicName();
    doAnswer(invocation -> {
      Function<String, String> lookup = invocation.getArgument(4);
      Assert.assertNotNull(lookup);
      Assert.assertEquals(lookup.apply(heartbeatStoreName), keyUrn);
      Assert.assertNull(lookup.apply("another-store"));
      return writer;
    }).when(pushJobHeartbeatSenderFactory).getVeniceWriter(anyString(), any(), any(), anyInt(), any());

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
    doReturn(heartbeatStoreName).when(storeInfo).getName();
    doReturn(keyUrn).when(storeInfo).getPubSubEncryptionKeyUrn();

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
    Assert.assertSame(defaultPushJobHeartbeatSender.getVeniceWriter(), writer);
    verify(controllerClient, times(1)).getStore(heartbeatStoreName);
  }
}
