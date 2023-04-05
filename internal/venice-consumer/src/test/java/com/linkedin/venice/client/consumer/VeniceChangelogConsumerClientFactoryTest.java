package com.linkedin.venice.client.consumer;

import static com.linkedin.venice.integration.utils.VeniceControllerWrapper.*;
import static org.testng.Assert.*;

import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.views.ChangeCaptureView;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceChangelogConsumerClientFactoryTest {
  private static final String STORE_NAME = "dybbuk_store";
  private static final String VIEW_NAME = "mazzikim_view";

  @Test
  public void testGetChangelogConsumer() {
    Properties consumerProperties = new Properties();
    String localKafkaUrl = "http://www.fooAddress.linkedin.com:16337";
    consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);

    SchemaReader mockSchemaReader = Mockito.mock(SchemaReader.class);
    Mockito.when(mockSchemaReader.getKeySchema()).thenReturn(TestKeyRecord.SCHEMA$);
    KafkaConsumer mockKafkaConsumer = Mockito.mock(KafkaConsumer.class);

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties).setSchemaReader(mockSchemaReader);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig);
    D2ControllerClient mockControllerClient = Mockito.mock(D2ControllerClient.class);

    veniceChangelogConsumerClientFactory.setD2ControllerClient(mockControllerClient);
    veniceChangelogConsumerClientFactory.setConsumer(mockKafkaConsumer);

    StoreResponse mockStoreResponse = Mockito.mock(StoreResponse.class);
    Mockito.when(mockStoreResponse.isError()).thenReturn(false);
    StoreInfo mockStoreInfo = new StoreInfo();
    mockStoreInfo.setPartitionCount(1);
    mockStoreInfo.setCurrentVersion(1);
    ViewConfig viewConfig = new ViewConfigImpl(ChangeCaptureView.class.getCanonicalName(), new HashMap<>());
    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    viewConfigMap.put(VIEW_NAME, viewConfig);
    mockStoreInfo.setViewConfigs(viewConfigMap);
    Mockito.when(mockStoreResponse.getStore()).thenReturn(mockStoreInfo);
    Mockito.when(mockControllerClient.getStore(STORE_NAME)).thenReturn(mockStoreResponse);
    VeniceChangelogConsumer consumer = veniceChangelogConsumerClientFactory.getChangelogConsumer(STORE_NAME);

    Assert.assertTrue(consumer instanceof VeniceAfterImageConsumerImpl);

    globalChangelogClientConfig.setViewName(VIEW_NAME);
    consumer = veniceChangelogConsumerClientFactory.getChangelogConsumer(STORE_NAME);
    Assert.assertTrue(consumer instanceof VeniceChangelogConsumerImpl);
  }

  @Test
  public void testGetChangelogConsumerThrowsException() {
    Properties consumerProperties = new Properties();
    String localKafkaUrl = "http://www.fooAddress.linkedin.com:16337";
    consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);

    SchemaReader mockSchemaReader = Mockito.mock(SchemaReader.class);
    Mockito.when(mockSchemaReader.getKeySchema()).thenReturn(TestKeyRecord.SCHEMA$);
    KafkaConsumer mockKafkaConsumer = Mockito.mock(KafkaConsumer.class);

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties).setSchemaReader(mockSchemaReader);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig);
    D2ControllerClient mockControllerClient = Mockito.mock(D2ControllerClient.class);

    veniceChangelogConsumerClientFactory.setD2ControllerClient(mockControllerClient);
    veniceChangelogConsumerClientFactory.setConsumer(mockKafkaConsumer);

    StoreResponse mockStoreResponse = Mockito.mock(StoreResponse.class);
    Mockito.when(mockStoreResponse.isError()).thenReturn(false);
    StoreInfo mockStoreInfo = new StoreInfo();
    mockStoreInfo.setPartitionCount(1);
    mockStoreInfo.setCurrentVersion(1);
    ViewConfig viewConfig = new ViewConfigImpl(ChangeCaptureView.class.getCanonicalName(), new HashMap<>());
    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    mockStoreInfo.setViewConfigs(viewConfigMap);
    Mockito.when(mockStoreResponse.getStore()).thenReturn(mockStoreInfo);
    Mockito.when(mockControllerClient.getStore(STORE_NAME)).thenReturn(mockStoreResponse);
    globalChangelogClientConfig.setViewName(VIEW_NAME);
    Assert.assertThrows(() -> veniceChangelogConsumerClientFactory.getChangelogConsumer(STORE_NAME));
  }
}
