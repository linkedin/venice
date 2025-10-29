package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.venice.ConfigKeys.CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_INGESTION_ISOLATION_D2_CLIENT_ENABLED;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_VALUE_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.SeekableDaVinciClient;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.ConstantVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SeekableDaVinciClientTest {
  private static final Logger LOGGER = LogManager.getLogger(DaVinciClientIsolatedAndHybridStoreTest.class);
  private static final int KEY_COUNT = 10;
  private static final int TEST_TIMEOUT = 120_000;
  private static final PubSubTopicRepository PUB_SUB_TOPIC_REPOSITORY = new PubSubTopicRepository();
  private VeniceClusterWrapper cluster;
  private D2Client d2Client;
  private PubSubProducerAdapterFactory pubSubProducerAdapterFactory;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .numberOfRouters(1)
        .replicationFactor(2)
        .partitionSize(100)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(clusterConfig)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);
    d2Client = new D2ClientBuilder().setZkHosts(cluster.getZk().getAddress())
        .setZkSessionTimeout(3, TimeUnit.SECONDS)
        .setZkStartupTimeout(3, TimeUnit.SECONDS)
        .build();
    pubSubProducerAdapterFactory =
        cluster.getPubSubBrokerWrapper().getPubSubClientsFactory().getProducerAdapterFactory();
    D2ClientUtils.startClient(d2Client);
  }

  @AfterClass
  public void cleanUp() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testDVCSeeking() throws Exception {
    final int partition = 0;
    final int partitionCount = 1;
    String storeName = Utils.getUniqueString("store");
    Consumer<UpdateStoreQueryParams> paramsConsumer =
        params -> params.setPartitionerClass(ConstantVenicePartitioner.class.getName())
            .setPartitionCount(partitionCount)
            .setPartitionerParams(
                Collections.singletonMap(ConstantVenicePartitioner.CONSTANT_PARTITION, String.valueOf(partition)));
    // Create an empty hybrid store first
    setupHybridStore(storeName, paramsConsumer, KEY_COUNT);

    // Build the da-vinci client
    VeniceProperties backendConfig = new PropertyBuilder().put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
        .put(CLIENT_SYSTEM_STORE_REPOSITORY_REFRESH_INTERVAL_SECONDS, 1)
        .put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
        .put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(DAVINCI_PUSH_STATUS_CHECK_INTERVAL_IN_MS, 1000)
        .put(SERVER_INGESTION_ISOLATION_D2_CLIENT_ENABLED, true)
        .build();

    MetricsRepository metricsRepository = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {

      DaVinciConfig daVinciConfig = new DaVinciConfig().setIsolated(false);
      SeekableDaVinciClient<Integer, Integer> client =
          factory.getAndStartGenericSeekableAvroClient(storeName, daVinciConfig);
      List<DefaultPubSubMessage> messages = getDataMessages(storeName);
      DefaultPubSubMessage pubSubMessage = messages.get(1);
      VeniceChangeCoordinate changeCoordinate = new VeniceChangeCoordinate(
          pubSubMessage.getTopic().getName(),
          pubSubMessage.getPosition(),
          pubSubMessage.getPartition());

      // Seek to the checkpoint of the 5th message
      // client.seekToCheckpoint(Collections.singleton(changeCoordinate)).get();
      client.seekToTimestamp(pubSubMessage.getValue().getProducerMetadata().getMessageTimestamp()).get();
      // client.subscribeAll().get();
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        for (Integer i = 0; i < KEY_COUNT; i++) {
          Object o = client.get(i).get();
          // Assert.assertEquals(o, i < 1 ? null : i);
          System.out.println("Key: " + i + " Value: " + o);
        }
      });
    }
  }

  @DataProvider(name = "CompressionStrategy")
  public static Object[][] compressionStrategy() {
    return DataProviderUtils.allPermutationGenerator(DataProviderUtils.COMPRESSION_STRATEGIES);
  }

  private void setupHybridStore(String storeName, Consumer<UpdateStoreQueryParams> paramsConsumer, int keyCount) {
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setHybridRewindSeconds(10)
        .setHybridOffsetLagThreshold(10)
        .setIncrementalPushEnabled(true);
    paramsConsumer.accept(params);
    cluster.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA);
      cluster.createMetaSystemStore(storeName);
      client.updateStore(storeName, params);
      cluster.createVersion(storeName, DEFAULT_KEY_SCHEMA, DEFAULT_VALUE_SCHEMA, Stream.of());
      if (keyCount > 0) {
        SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(
            cluster,
            storeName,
            Version.PushType.STREAM,
            Pair.create(VENICE_PARTITIONERS, ConstantVenicePartitioner.class.getName()));
        try {
          for (int i = 0; i < keyCount; i++) {
            sendStreamingRecord(producer, storeName, i, i);
          }
        } finally {
          producer.stop();
        }
      }
    });
  }

  private List<DefaultPubSubMessage> getDataMessages(String storeName) {
    // Consume all the RT messages and validated how many data records were produced.
    PubSubBrokerWrapper pubSubBrokerWrapper = cluster.getPubSubBrokerWrapper();
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try (PubSubConsumerAdapter pubSubConsumer = pubSubBrokerWrapper.getPubSubClientsFactory()
        .getConsumerAdapterFactory()
        .create(
            new PubSubConsumerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
                .setConsumerName("testConsumer")
                .build())) {

      pubSubConsumer.subscribe(
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Utils.composeRealTimeTopic(storeName, 1)), 0),
          PubSubSymbolicPosition.EARLIEST,
          false);
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = pubSubConsumer.poll(1000 * Time.MS_PER_SECOND);
      return messages.get(
          new PubSubTopicPartitionImpl(PUB_SUB_TOPIC_REPOSITORY.getTopic(Utils.composeRealTimeTopic(storeName, 1)), 0))
          .stream()
          .filter(m -> !m.getKey().isControlMessage())
          .collect(Collectors.toList());
    }
  }
}
