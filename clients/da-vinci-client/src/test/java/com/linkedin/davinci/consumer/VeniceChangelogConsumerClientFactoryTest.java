package com.linkedin.davinci.consumer;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.views.ChangeCaptureView;
import io.tehuti.metrics.MetricsRepository;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceChangelogConsumerClientFactoryTest {
  private static final String STORE_NAME = "dybbuk_store";
  private static final String VIEW_NAME = "mazzikim_view";
  private static final String TEST_CLUSTER = "golem_cluster";

  @Test
  public void testGetChangelogConsumer() throws ExecutionException, InterruptedException, JsonProcessingException {
    Properties consumerProperties = new Properties();
    String localKafkaUrl = "http://www.fooAddress.linkedin.com:16337";
    consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);

    SchemaReader mockSchemaReader = Mockito.mock(SchemaReader.class);
    Mockito.when(mockSchemaReader.getKeySchema()).thenReturn(TestKeyRecord.SCHEMA$);
    PubSubConsumerAdapter mockKafkaConsumer = Mockito.mock(PubSubConsumerAdapter.class);

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties).setSchemaReader(mockSchemaReader);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, new MetricsRepository());
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

    D2ServiceDiscoveryResponse serviceDiscoveryResponse = new D2ServiceDiscoveryResponse();
    serviceDiscoveryResponse.setCluster(TEST_CLUSTER);
    serviceDiscoveryResponse.setD2Service("TEST_ROUTER_D2_SERVICE");
    serviceDiscoveryResponse.setServerD2Service("TEST_SERVER_D2_SERVICE");
    serviceDiscoveryResponse.setName(STORE_NAME);
    String discoverClusterResponse = ObjectMapperFactory.getInstance().writeValueAsString(serviceDiscoveryResponse);

    RestResponse discoverClusterRestResponse = mock(RestResponse.class);
    doReturn(ByteString.unsafeWrap(discoverClusterResponse.getBytes(StandardCharsets.UTF_8)))
        .when(discoverClusterRestResponse)
        .getEntity();

    // Verify branches which have a managed D2Client passed in for the controller client
    D2Client mockD2Client = Mockito.mock(D2Client.class);
    Future mockClusterDiscoveryFuture = Mockito.mock(Future.class);
    Mockito.when(mockClusterDiscoveryFuture.get()).thenReturn(discoverClusterRestResponse);
    Mockito.when(mockD2Client.restRequest(Mockito.any())).thenReturn(mockClusterDiscoveryFuture);

    globalChangelogClientConfig.setD2Client(mockD2Client).setD2ControllerClient(null).setControllerRequestRetryCount(1);

    VeniceChangelogConsumerClientFactory.ViewClassGetter mockViewGetter = (
        String storeName,
        String viewName,
        D2ControllerClient d2ControllerClient,
        int retries) -> ChangeCaptureView.class.getCanonicalName();

    veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, new MetricsRepository());
    veniceChangelogConsumerClientFactory.setViewClassGetter(mockViewGetter);
    veniceChangelogConsumerClientFactory.setD2ControllerClient(mockControllerClient);
    veniceChangelogConsumerClientFactory.setConsumer(mockKafkaConsumer);

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
    PubSubConsumerAdapter mockKafkaConsumer = Mockito.mock(PubSubConsumerAdapter.class);

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties).setSchemaReader(mockSchemaReader);
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        new VeniceChangelogConsumerClientFactory(globalChangelogClientConfig, new MetricsRepository());
    D2ControllerClient mockControllerClient = Mockito.mock(D2ControllerClient.class);

    veniceChangelogConsumerClientFactory.setConsumer(mockKafkaConsumer);

    StoreResponse mockStoreResponse = Mockito.mock(StoreResponse.class);
    Mockito.when(mockStoreResponse.isError()).thenReturn(false);
    StoreInfo mockStoreInfo = new StoreInfo();
    mockStoreInfo.setPartitionCount(1);
    mockStoreInfo.setCurrentVersion(1);
    Map<String, ViewConfig> viewConfigMap = new HashMap<>();
    mockStoreInfo.setViewConfigs(viewConfigMap);
    Mockito.when(mockStoreResponse.getStore()).thenReturn(mockStoreInfo);
    Mockito.when(mockControllerClient.getStore(STORE_NAME)).thenReturn(mockStoreResponse);
    globalChangelogClientConfig.setViewName(VIEW_NAME);
    Assert.assertThrows(() -> veniceChangelogConsumerClientFactory.getChangelogConsumer(STORE_NAME));
  }
}
