package com.linkedin.davinci.consumer;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.data.ByteString;
import com.linkedin.davinci.repository.NativeMetadataRepositoryViewAdapter;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.venice.client.store.schemas.TestKeyRecord;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
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
  private static final String TEST_CLUSTER_NAME = "test_cluster";
  private static final String TEST_ZOOKEEPER_ADDRESS = "test_zookeeper";
  private static final String TEST_BOOTSTRAP_FILE_SYSTEM_PATH = "/export/content/data/change-capture";

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
    Mockito.when(mockControllerClient.retryableRequest(Mockito.anyInt(), Mockito.any())).thenReturn(mockStoreResponse);
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
  public void testGetChangelogConsumerWithConsumerId()
      throws ExecutionException, InterruptedException, JsonProcessingException {
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

    setUpMockStoreResponse(mockControllerClient, STORE_NAME);
    setUpMockStoreResponse(mockControllerClient, STORE_NAME + "-" + "consumer1");
    setUpMockStoreResponse(mockControllerClient, STORE_NAME + "-" + "consumer2");

    VeniceChangelogConsumer consumer = veniceChangelogConsumerClientFactory.getChangelogConsumer(STORE_NAME);
    Assert.assertTrue(consumer instanceof VeniceAfterImageConsumerImpl);

    VeniceChangelogConsumer consumer1 =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(STORE_NAME, "consumer1");
    Assert.assertTrue(consumer1 instanceof VeniceAfterImageConsumerImpl);

    VeniceChangelogConsumer consumer2 =
        veniceChangelogConsumerClientFactory.getChangelogConsumer(STORE_NAME, "consumer2");
    Assert.assertTrue(consumer2 instanceof VeniceAfterImageConsumerImpl);

    Assert.assertNotSame(consumer, consumer1);
    Assert.assertNotSame(consumer, consumer2);
    Assert.assertNotSame(consumer1, consumer2);
  }

  private void setUpMockStoreResponse(D2ControllerClient mockControllerClient, String storeConsumer) {
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
    Mockito.when(mockControllerClient.getStore(storeConsumer)).thenReturn(mockStoreResponse);
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

  @Test
  public void testGetBootstrappingChangelogConsumer()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    Properties consumerProperties = new Properties();
    String localKafkaUrl = "http://www.fooAddress.linkedin.com:16337";
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    consumerProperties.put(CLUSTER_NAME, TEST_CLUSTER_NAME);
    consumerProperties.put(ZOOKEEPER_ADDRESS, TEST_ZOOKEEPER_ADDRESS);

    SchemaReader mockSchemaReader = Mockito.mock(SchemaReader.class);
    Mockito.when(mockSchemaReader.getKeySchema()).thenReturn(TestKeyRecord.SCHEMA$);
    PubSubConsumerAdapter mockKafkaConsumer = Mockito.mock(PubSubConsumerAdapter.class);

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setSchemaReader(mockSchemaReader)
            .setBootstrapFileSystemPath(TEST_BOOTSTRAP_FILE_SYSTEM_PATH)
            .setLocalD2ZkHosts(TEST_ZOOKEEPER_ADDRESS)
            .setIsBeforeImageView(true);
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
    BootstrappingVeniceChangelogConsumer consumer =
        veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(STORE_NAME);

    Assert.assertTrue(consumer instanceof LocalBootstrappingVeniceChangelogConsumer);

    globalChangelogClientConfig.setViewName(VIEW_NAME);

    NativeMetadataRepositoryViewAdapter mockRepository = mock(NativeMetadataRepositoryViewAdapter.class);
    Store store = mock(Store.class);
    Version mockVersion = new VersionImpl(STORE_NAME, 1, "foo");
    mockVersion.setPartitionCount(2);
    Mockito.when(store.getCurrentVersion()).thenReturn(1);
    Mockito.when(store.getCompressionStrategy()).thenReturn(CompressionStrategy.NO_OP);
    Mockito.when(store.getPartitionCount()).thenReturn(2);
    Mockito.when(store.getVersion(anyInt())).thenReturn(mockVersion);
    Mockito.when(mockRepository.getStore(anyString())).thenReturn(store);
    Mockito.when(store.getVersionOrThrow(Mockito.anyInt())).thenReturn(mockVersion);

    ((LocalBootstrappingVeniceChangelogConsumer) consumer).setStoreRepository(mockRepository);

    consumer = veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(STORE_NAME);
    Assert.assertTrue(consumer instanceof LocalBootstrappingVeniceChangelogConsumer);

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

    consumer = veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(STORE_NAME);
    Assert.assertTrue(consumer instanceof LocalBootstrappingVeniceChangelogConsumer);
  }

  @Test
  public void testGetBootstrappingChangelogConsumerThrowsException() {
    Properties consumerProperties = new Properties();
    String localKafkaUrl = "http://www.fooAddress.linkedin.com:16337";
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    consumerProperties.put(CLUSTER_NAME, TEST_CLUSTER_NAME);
    consumerProperties.put(ZOOKEEPER_ADDRESS, TEST_ZOOKEEPER_ADDRESS);

    SchemaReader mockSchemaReader = Mockito.mock(SchemaReader.class);
    Mockito.when(mockSchemaReader.getKeySchema()).thenReturn(TestKeyRecord.SCHEMA$);
    PubSubConsumerAdapter mockKafkaConsumer = Mockito.mock(PubSubConsumerAdapter.class);

    ChangelogClientConfig globalChangelogClientConfig =
        new ChangelogClientConfig().setConsumerProperties(consumerProperties)
            .setSchemaReader(mockSchemaReader)
            .setBootstrapFileSystemPath(TEST_BOOTSTRAP_FILE_SYSTEM_PATH)
            .setLocalD2ZkHosts(TEST_ZOOKEEPER_ADDRESS);
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
    Assert.assertThrows(() -> veniceChangelogConsumerClientFactory.getBootstrappingChangelogConsumer(STORE_NAME));
  }
}
