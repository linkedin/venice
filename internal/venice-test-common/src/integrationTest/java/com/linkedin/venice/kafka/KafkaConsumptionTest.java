package com.linkedin.venice.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicCompletion;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.Cache;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.AggKafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.IngestionThrottler;
import com.linkedin.davinci.kafka.consumer.KafkaClusterBasedRecordThrottler;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerServiceDelegator;
import com.linkedin.davinci.kafka.consumer.PartitionReplicaIngestionContext;
import com.linkedin.davinci.kafka.consumer.StaleTopicChecker;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.kafka.consumer.StorePartitionDataReceiver;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.EndOfPush;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterContext;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class KafkaConsumptionTest {
  /** Wait time for {@link #topicManager} operations, in seconds */
  private static final int WAIT_TIME_IN_SECONDS = 10;
  private static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  private PubSubBrokerWrapper localPubSubBroker;
  private PubSubBrokerWrapper remotePubSubBroker;
  private TopicManager topicManager;
  private TopicManager remoteTopicManager;
  private TestMockTime mockTime;
  private TestMockTime remoteMockTime;
  private PubSubTopic versionTopic;
  private Map<String, String> pubSubClientProperties;

  private PubSubTopic getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(TestUtils.getUniqueTopicString(callingFunction));
    int partitions = 1;
    int replicas = 1;
    topicManager.createTopic(versionTopic, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(
        WAIT_TIME_IN_SECONDS,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(versionTopic)));
    remoteTopicManager.createTopic(versionTopic, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(
        WAIT_TIME_IN_SECONDS,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(remoteTopicManager.containsTopicAndAllPartitionsAreOnline(versionTopic)));
    return versionTopic;
  }

  @BeforeClass
  public void setUp() {
    mockTime = new TestMockTime();
    localPubSubBroker = ServiceFactory
        .getPubSubBroker(new PubSubBrokerConfigs.Builder().setMockTime(mockTime).setRegionName("local-pubsub").build());
    topicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                100L,
                MIN_COMPACTION_LAG,
                localPubSubBroker,
                pubSubTopicRepository)
            .getTopicManager(localPubSubBroker.getAddress());
    Cache cacheNothingCache = mock(Cache.class);
    Mockito.when(cacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
    topicManager.setTopicConfigCache(cacheNothingCache);

    remoteMockTime = new TestMockTime();
    remotePubSubBroker = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setMockTime(remoteMockTime).setRegionName("remote-pubsub").build());

    pubSubClientProperties =
        PubSubBrokerWrapper.getBrokerDetailsForClients(Arrays.asList(localPubSubBroker, remotePubSubBroker));
    remoteTopicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                100L,
                MIN_COMPACTION_LAG,
                remotePubSubBroker,
                pubSubTopicRepository)
            .getTopicManager(remotePubSubBroker.getAddress());
    Cache remoteCacheNothingCache = mock(Cache.class);
    Mockito.when(remoteCacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
    remoteTopicManager.setTopicConfigCache(remoteCacheNothingCache);
  }

  @AfterClass
  public void cleanUp() {
    topicManager.close();
    localPubSubBroker.close();
    remoteTopicManager.close();
    remotePubSubBroker.close();
  }

  @Test(dataProvider = "sharedConsumerStrategy", dataProviderClass = DataProviderUtils.class, timeOut = 10
      * Time.MS_PER_SECOND)
  public void testLocalAndRemoteConsumption(KafkaConsumerService.ConsumerAssignmentStrategy sharedConsumerStrategy)
      throws ExecutionException, InterruptedException {
    // Prepare Aggregate Kafka Consumer Service.
    IngestionThrottler mockIngestionThrottler = mock(IngestionThrottler.class);
    Map<String, EventThrottler> kafkaUrlToRecordsThrottler = new HashMap<>();
    KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler =
        new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);
    MetricsRepository metricsRepository = TehutiUtils.getMetricsRepository(this.getClass().getName());
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);

    doReturn(10L).when(veniceServerConfig).getKafkaReadCycleDelayMs();
    doReturn(2).when(veniceServerConfig).getConsumerPoolSizePerKafkaCluster();
    doReturn(10L).when(veniceServerConfig).getSharedConsumerNonExistingTopicCleanupDelayMS();
    doReturn(true).when(veniceServerConfig).isLiveConfigBasedKafkaThrottlingEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.DEFAULT).when(veniceServerConfig)
        .getConsumerPoolStrategyType();
    doReturn(sharedConsumerStrategy).when(veniceServerConfig).getSharedConsumerAssignmentStrategy();
    doReturn(localPubSubBroker.getPubSubPositionTypeRegistry()).when(veniceServerConfig)
        .getPubSubPositionTypeRegistry();

    String localKafkaUrl = localPubSubBroker.getAddress();
    String remoteKafkaUrl = remotePubSubBroker.getAddress();
    Map<String, String> clusterUrlToAlias = new HashMap<>();
    clusterUrlToAlias.put(localKafkaUrl, localKafkaUrl);
    clusterUrlToAlias.put(remoteKafkaUrl, remoteKafkaUrl);
    doReturn(clusterUrlToAlias).when(veniceServerConfig).getKafkaClusterUrlToAliasMap();
    Object2IntMap<String> clusterUrlToIdMap = new Object2IntOpenHashMap<>(2);
    clusterUrlToIdMap.put(localKafkaUrl, 0);
    clusterUrlToIdMap.put(remoteKafkaUrl, 1);
    doReturn(clusterUrlToIdMap).when(veniceServerConfig).getKafkaClusterUrlToIdMap();

    StaleTopicChecker staleTopicChecker = mock(StaleTopicChecker.class);
    PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory =
        localPubSubBroker.getPubSubClientsFactory().getConsumerAdapterFactory();
    PubSubMessageDeserializer pubSubDeserializer = PubSubMessageDeserializer.createOptimizedDeserializer();

    AggKafkaConsumerService aggKafkaConsumerService = new AggKafkaConsumerService(
        pubSubConsumerAdapterFactory,
        k -> new VeniceProperties(pubSubClientProperties),
        veniceServerConfig,
        mockIngestionThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        staleTopicChecker,
        pubSubDeserializer,
        (ignored) -> {},
        (ignored) -> false,
        mock(ReadOnlyStoreRepository.class));

    versionTopic = getTopic();
    int partition = 0;
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, partition);
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    doReturn(versionTopic).when(storeIngestionTask).getVersionTopic();

    // Local consumer subscription.
    Properties consumerProperties = new Properties();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, localKafkaUrl);
    aggKafkaConsumerService.createKafkaConsumerService(consumerProperties);
    PartitionReplicaIngestionContext partitionReplicaIngestionContext = new PartitionReplicaIngestionContext(
        versionTopic,
        pubSubTopicPartition,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE);
    StorePartitionDataReceiver localDataReceiver = (StorePartitionDataReceiver) aggKafkaConsumerService
        .subscribeConsumerFor(localKafkaUrl, storeIngestionTask, partitionReplicaIngestionContext, -1);
    Assert
        .assertTrue(aggKafkaConsumerService.hasConsumerAssignedFor(localKafkaUrl, versionTopic, pubSubTopicPartition));

    // Remote consumer subscription.
    consumerProperties = new Properties();
    consumerProperties.put(KAFKA_BOOTSTRAP_SERVERS, remoteKafkaUrl);
    aggKafkaConsumerService.createKafkaConsumerService(consumerProperties);
    StorePartitionDataReceiver remoteDataReceiver = (StorePartitionDataReceiver) aggKafkaConsumerService
        .subscribeConsumerFor(remoteKafkaUrl, storeIngestionTask, partitionReplicaIngestionContext, -1);
    Assert
        .assertTrue(aggKafkaConsumerService.hasConsumerAssignedFor(remoteKafkaUrl, versionTopic, pubSubTopicPartition));

    long timestamp = System.currentTimeMillis();
    int dataRecordsNum = 10;
    int controlRecordsNum = 3;
    for (int i = 0; i < dataRecordsNum; i++) {
      timestamp += 1000;
      produceToKafka(versionTopic.getName(), true, timestamp, localPubSubBroker);
    }
    for (int i = 0; i < controlRecordsNum; i++) {
      timestamp += 1000;
      produceToKafka(versionTopic.getName(), false, timestamp, localPubSubBroker);
    }
    final int localExpectedRecordsNum = dataRecordsNum + controlRecordsNum;
    waitForNonDeterministicCompletion(
        1000,
        TimeUnit.MILLISECONDS,
        () -> localDataReceiver.receivedRecordsCount() == localExpectedRecordsNum);

    timestamp = System.currentTimeMillis();
    dataRecordsNum = 5;
    controlRecordsNum = 4;
    for (int i = 0; i < dataRecordsNum; i++) {
      timestamp += 1000;
      produceToKafka(versionTopic.getName(), true, timestamp, remotePubSubBroker);
    }
    for (int i = 0; i < controlRecordsNum; i++) {
      timestamp += 1000;
      produceToKafka(versionTopic.getName(), false, timestamp, remotePubSubBroker);
    }
    final int remoteExpectedRecordsNum = dataRecordsNum + controlRecordsNum;
    waitForNonDeterministicCompletion(
        1000,
        TimeUnit.MILLISECONDS,
        () -> remoteDataReceiver.receivedRecordsCount() == remoteExpectedRecordsNum);
  }

  /**
   * This method produces either a random data record or a control message/record to Kafka with a given producer timestamp.
   *
   * @param topic
   * @param isDataRecord
   * @param producerTimestamp
   * @param pubSubBrokerWrapper
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void produceToKafka(
      String topic,
      boolean isDataRecord,
      long producerTimestamp,
      PubSubBrokerWrapper pubSubBrokerWrapper) throws ExecutionException, InterruptedException {
    Map<String, String> brokerDetails =
        PubSubBrokerWrapper.getBrokerDetailsForClients(Collections.singletonList(pubSubBrokerWrapper));
    Properties properties = new Properties();
    brokerDetails.forEach(properties::setProperty);
    PubSubProducerAdapterContext producerAdapterContext =
        new PubSubProducerAdapterContext.Builder().setVeniceProperties(new VeniceProperties(properties))
            .setBrokerAddress(pubSubBrokerWrapper.getAddress())
            .setProducerName("test-producer")
            .setPubSubPositionTypeRegistry(pubSubBrokerWrapper.getPubSubPositionTypeRegistry())
            .build();
    PubSubProducerAdapter producerAdapter =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory().create(producerAdapterContext);

    final byte[] randomBytes = new byte[] { 0, 1 };

    // Prepare record key
    KafkaKey recordKey = new KafkaKey(isDataRecord ? MessageType.PUT : MessageType.CONTROL_MESSAGE, randomBytes);

    // Prepare record value
    KafkaMessageEnvelope recordValue = new KafkaMessageEnvelope();
    recordValue.producerMetadata = new ProducerMetadata();
    recordValue.producerMetadata.producerGUID = new GUID();
    recordValue.producerMetadata.messageTimestamp = producerTimestamp;
    recordValue.leaderMetadataFooter = new LeaderMetadata();
    recordValue.leaderMetadataFooter.upstreamPubSubPosition = PubSubSymbolicPosition.EARLIEST.toWireFormatBuffer();
    recordValue.leaderMetadataFooter.hostName = "localhost";

    if (isDataRecord) {
      Put put = new Put();
      put.putValue = ByteBuffer.wrap(new byte[] { 0, 1 });
      put.replicationMetadataPayload = ByteBuffer.wrap(randomBytes);
      recordValue.payloadUnion = put;
    } else {
      ControlMessage controlMessage = new ControlMessage();
      controlMessage.controlMessageType = ControlMessageType.END_OF_PUSH.getValue();
      controlMessage.controlMessageUnion = new EndOfPush();
      controlMessage.debugInfo = Collections.emptyMap();
      recordValue.payloadUnion = controlMessage;
    }
    producerAdapter.sendMessage(topic, null, recordKey, recordValue, null, null).get();
  }
}
