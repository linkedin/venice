package com.linkedin.venice.kafka;

import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicCompletion;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.benmanes.caffeine.cache.Cache;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.kafka.consumer.AggKafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.KafkaClusterBasedRecordThrottler;
import com.linkedin.davinci.kafka.consumer.KafkaConsumerService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.davinci.kafka.consumer.StorePartitionDataReceiver;
import com.linkedin.davinci.kafka.consumer.TopicExistenceChecker;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.consumer.KafkaConsumerFactoryImpl;
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
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class KafkaConsumptionTest {
  /** Wait time for {@link #topicManager} operations, in seconds */
  private static final int WAIT_TIME_IN_SECONDS = 10;
  private static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;

  private KafkaBrokerWrapper localKafka;
  private KafkaBrokerWrapper remoteKafka;
  private TopicManager topicManager;
  private TopicManager remoteTopicManager;
  private MockTime mockTime;
  private MockTime remoteMockTime;
  private ZkServerWrapper localZkServer;
  private ZkServerWrapper remoteZkServer;
  private String versionTopic;
  private KafkaClientFactory localKafkaClientFactory;
  private KafkaClientFactory remoteKafkaClientFactory;

  private String getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    String topicName = Utils.getUniqueString(callingFunction);
    int partitions = 1;
    int replicas = 1;
    topicManager.createTopic(topicName, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(
        WAIT_TIME_IN_SECONDS,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(topicManager.containsTopicAndAllPartitionsAreOnline(topicName)));
    remoteTopicManager.createTopic(topicName, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(
        WAIT_TIME_IN_SECONDS,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(remoteTopicManager.containsTopicAndAllPartitionsAreOnline(topicName)));
    return topicName;
  }

  @BeforeClass
  public void setUp() {
    mockTime = new MockTime();
    localZkServer = ServiceFactory.getZkServer();
    localKafka = ServiceFactory.getKafkaBroker(localZkServer, Optional.of(mockTime));
    localKafkaClientFactory = IntegrationTestPushUtils.getVeniceConsumerFactory(localKafka);
    topicManager =
        new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, MIN_COMPACTION_LAG, localKafkaClientFactory);
    Cache cacheNothingCache = mock(Cache.class);
    Mockito.when(cacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
    topicManager.setTopicConfigCache(cacheNothingCache);

    remoteZkServer = ServiceFactory.getZkServer();
    remoteMockTime = new MockTime();
    remoteKafka = ServiceFactory.getKafkaBroker(remoteZkServer, Optional.of(remoteMockTime));
    remoteKafkaClientFactory = IntegrationTestPushUtils.getVeniceConsumerFactory(remoteKafka);
    remoteTopicManager =
        new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, MIN_COMPACTION_LAG, remoteKafkaClientFactory);
    Cache remoteCacheNothingCache = mock(Cache.class);
    Mockito.when(remoteCacheNothingCache.getIfPresent(Mockito.any())).thenReturn(null);
    remoteTopicManager.setTopicConfigCache(remoteCacheNothingCache);
  }

  @AfterClass
  public void cleanUp() {
    topicManager.close();
    localKafka.close();
    localZkServer.close();

    remoteTopicManager.close();
    remoteKafka.close();
    remoteZkServer.close();
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testLocalAndRemoteConsumption(boolean isTopicWiseSharedConsumerAssignmentStrategy)
      throws ExecutionException, InterruptedException {
    // Prepare Aggregate Kafka Consumer Service.
    EventThrottler mockBandwidthThrottler = mock(EventThrottler.class);
    EventThrottler mockRecordsThrottler = mock(EventThrottler.class);
    Map<String, EventThrottler> kafkaUrlToRecordsThrottler = new HashMap<>();
    KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler =
        new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);
    MetricsRepository metricsRepository = TehutiUtils.getMetricsRepository(this.getClass().getName());
    VeniceServerConfig veniceServerConfig = mock(VeniceServerConfig.class);

    doReturn(10L).when(veniceServerConfig).getKafkaReadCycleDelayMs();
    doReturn(2).when(veniceServerConfig).getConsumerPoolSizePerKafkaCluster();
    doReturn(10L).when(veniceServerConfig).getSharedConsumerNonExistingTopicCleanupDelayMS();
    doReturn(true).when(veniceServerConfig).isLiveConfigBasedKafkaThrottlingEnabled();
    if (isTopicWiseSharedConsumerAssignmentStrategy) {
      doReturn(KafkaConsumerService.ConsumerAssignmentStrategy.TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY)
          .when(veniceServerConfig)
          .getSharedConsumerAssignmentStrategy();
    } else {
      doReturn(KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY)
          .when(veniceServerConfig)
          .getSharedConsumerAssignmentStrategy();
    }

    String localKafkaUrl = localKafka.getAddress();
    String remoteKafkaUrl = remoteKafka.getAddress();
    Map<String, String> clusterUrlToAlias = new HashMap<>();
    clusterUrlToAlias.put(localKafkaUrl, localKafkaUrl);
    clusterUrlToAlias.put(remoteKafkaUrl, remoteKafkaUrl);
    doReturn(clusterUrlToAlias).when(veniceServerConfig).getKafkaClusterUrlToAliasMap();
    Object2IntMap<String> clusterUrlToIdMap = new Object2IntOpenHashMap<>(2);
    clusterUrlToIdMap.put(localKafkaUrl, 0);
    clusterUrlToIdMap.put(remoteKafkaUrl, 1);
    doReturn(clusterUrlToIdMap).when(veniceServerConfig).getKafkaClusterUrlToIdMap();

    TopicExistenceChecker topicExistenceChecker = mock(TopicExistenceChecker.class);
    KafkaClientFactory kafkaClientFactory = new KafkaConsumerFactoryImpl(new VeniceProperties());
    AggKafkaConsumerService aggKafkaConsumerService = new AggKafkaConsumerService(
        kafkaClientFactory,
        veniceServerConfig,
        mockBandwidthThrottler,
        mockRecordsThrottler,
        kafkaClusterBasedRecordThrottler,
        metricsRepository,
        topicExistenceChecker);

    versionTopic = getTopic();
    int partition = 0;
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    doReturn(versionTopic).when(storeIngestionTask).getVersionTopic();

    // Local consumer subscription.
    Properties consumerProperties = new Properties();
    consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, localKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    aggKafkaConsumerService.createKafkaConsumerService(consumerProperties);
    StorePartitionDataReceiver localDataReceiver = (StorePartitionDataReceiver) aggKafkaConsumerService
        .subscribeConsumerFor(localKafkaUrl, storeIngestionTask, versionTopic, partition, -1);
    Assert.assertTrue(
        aggKafkaConsumerService.hasConsumerAssignedFor(localKafkaUrl, versionTopic, versionTopic, partition));

    // Remote consumer subscription.
    consumerProperties = new Properties();
    consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, remoteKafkaUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    aggKafkaConsumerService.createKafkaConsumerService(consumerProperties);
    StorePartitionDataReceiver remoteDataReceiver = (StorePartitionDataReceiver) aggKafkaConsumerService
        .subscribeConsumerFor(remoteKafkaUrl, storeIngestionTask, versionTopic, partition, -1);
    Assert.assertTrue(
        aggKafkaConsumerService.hasConsumerAssignedFor(remoteKafkaUrl, versionTopic, versionTopic, partition));

    long timestamp = System.currentTimeMillis();
    int dataRecordsNum = 10;
    int controlRecordsNum = 3;
    for (int i = 0; i < dataRecordsNum; i++) {
      timestamp += 1000;
      produceToKafka(versionTopic, true, timestamp, localKafkaUrl);
    }
    for (int i = 0; i < controlRecordsNum; i++) {
      timestamp += 1000;
      produceToKafka(versionTopic, false, timestamp, localKafkaUrl);
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
      produceToKafka(versionTopic, true, timestamp, remoteKafkaUrl);
    }
    for (int i = 0; i < controlRecordsNum; i++) {
      timestamp += 1000;
      produceToKafka(versionTopic, false, timestamp, remoteKafkaUrl);
    }
    final int remoteExpectedRecordsNum = dataRecordsNum + controlRecordsNum;
    waitForNonDeterministicCompletion(
        1000,
        TimeUnit.MILLISECONDS,
        () -> remoteDataReceiver.receivedRecordsCount() == remoteExpectedRecordsNum);
  }

  /**
   * This method produces either an random data record or a control message/record to Kafka with a given producer timestamp.
   *
   * @param topic
   * @param isDataRecord
   * @param producerTimestamp
   * @param kafkaUrl
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private void produceToKafka(String topic, boolean isDataRecord, long producerTimestamp, String kafkaUrl)
      throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
    KafkaProducer<KafkaKey, KafkaMessageEnvelope> producer = new KafkaProducer(props);
    final byte[] randomBytes = new byte[] { 0, 1 };

    // Prepare record key
    KafkaKey recordKey = new KafkaKey(isDataRecord ? MessageType.PUT : MessageType.CONTROL_MESSAGE, randomBytes);

    // Prepare record value
    KafkaMessageEnvelope recordValue = new KafkaMessageEnvelope();
    recordValue.producerMetadata = new ProducerMetadata();
    recordValue.producerMetadata.producerGUID = new GUID();
    recordValue.producerMetadata.messageTimestamp = producerTimestamp;
    recordValue.leaderMetadataFooter = new LeaderMetadata();
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
    ProducerRecord<KafkaKey, KafkaMessageEnvelope> record = new ProducerRecord<>(topic, recordKey, recordValue);
    producer.send(record).get();
  }
}
