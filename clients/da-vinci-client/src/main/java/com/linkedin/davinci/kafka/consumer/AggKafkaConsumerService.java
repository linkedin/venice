package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link AggKafkaConsumerService} supports Kafka consumer pool for multiple Kafka clusters from different data centers;
 * for each Kafka bootstrap server url, {@link AggKafkaConsumerService} will create one {@link KafkaConsumerService}.
 */
public class AggKafkaConsumerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(AggKafkaConsumerService.class);

  private final KafkaClientFactory consumerFactory;
  private final int numOfConsumersPerKafkaCluster;
  private final long readCycleDelayMs;
  private final long sharedConsumerNonExistingTopicCleanupDelayMS;
  private final EventThrottler bandwidthThrottler;
  private final EventThrottler recordsThrottler;
  private final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private final MetricsRepository metricsRepository;
  private final TopicExistenceChecker topicExistenceChecker;
  private final boolean liveConfigBasedKafkaThrottlingEnabled;
  private final KafkaConsumerService.ConsumerAssignmentStrategy sharedConsumerAssignmentStrategy;
  private final Map<String, KafkaConsumerService> kafkaServerToConsumerServiceMap = new VeniceConcurrentHashMap<>();
  private final Map<String, String> kafkaClusterUrlToAliasMap;
  private final Object2IntMap<String> kafkaClusterUrlToIdMap;
  private final KafkaPubSubMessageDeserializer pubSubDeserializer;

  public AggKafkaConsumerService(
      final KafkaClientFactory consumerFactory,
      final VeniceServerConfig serverConfig,
      final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler,
      KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      TopicExistenceChecker topicExistenceChecker,
      KafkaPubSubMessageDeserializer pubSubDeserializer) {
    this.consumerFactory = consumerFactory;
    this.readCycleDelayMs = serverConfig.getKafkaReadCycleDelayMs();
    this.numOfConsumersPerKafkaCluster = serverConfig.getConsumerPoolSizePerKafkaCluster();
    this.sharedConsumerNonExistingTopicCleanupDelayMS = serverConfig.getSharedConsumerNonExistingTopicCleanupDelayMS();
    this.bandwidthThrottler = bandwidthThrottler;
    this.recordsThrottler = recordsThrottler;
    this.kafkaClusterBasedRecordThrottler = kafkaClusterBasedRecordThrottler;
    this.metricsRepository = metricsRepository;
    this.topicExistenceChecker = topicExistenceChecker;
    this.liveConfigBasedKafkaThrottlingEnabled = serverConfig.isLiveConfigBasedKafkaThrottlingEnabled();
    this.sharedConsumerAssignmentStrategy = serverConfig.getSharedConsumerAssignmentStrategy();
    this.kafkaClusterUrlToAliasMap = serverConfig.getKafkaClusterUrlToAliasMap();
    this.kafkaClusterUrlToIdMap = serverConfig.getKafkaClusterUrlToIdMap();
    this.pubSubDeserializer = pubSubDeserializer;
    LOGGER.info("Successfully initialized AggKafkaConsumerService");
  }

  /**
   * IMPORTANT: All newly created KafkaConsumerService are already started in {@link #createKafkaConsumerService(Properties)},
   * if this is no longer the case in future, make sure to update the startInner logic here.
   */
  @Override
  public boolean startInner() {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.stop();
    }
  }

  /**
   * @return the {@link KafkaConsumerService} for a specific Kafka bootstrap url,
   *         or null if there isn't any.
   */
  private KafkaConsumerService getKafkaConsumerService(final String kafkaURL) {
    return kafkaServerToConsumerServiceMap.get(kafkaURL);
  }

  /**
   * Create a new {@link KafkaConsumerService} given consumerProperties which must contain a value for "bootstrap.servers".
   * If a {@link KafkaConsumerService} for the given "bootstrap.servers" (Kafka URL) has already been created, this method
   * returns the created {@link KafkaConsumerService}.
   *
   * @param consumerProperties consumer properties that are used to create {@link KafkaConsumerService}
   */
  public synchronized KafkaConsumerService createKafkaConsumerService(final Properties consumerProperties) {
    final String kafkaUrl = consumerProperties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    if (kafkaUrl == null || kafkaUrl.isEmpty()) {
      throw new IllegalArgumentException("Kafka URL must be set in the consumer properties config. Got: " + kafkaUrl);
    }
    final KafkaConsumerService alreadyCreatedConsumerService = kafkaServerToConsumerServiceMap.get(kafkaUrl);
    if (alreadyCreatedConsumerService != null) {
      LOGGER.warn("KafkaConsumerService has already been created for Kafka cluster with URL: {}", kafkaUrl);
      return alreadyCreatedConsumerService;
    }

    KafkaConsumerService consumerService = kafkaServerToConsumerServiceMap.computeIfAbsent(
        kafkaUrl,
        url -> sharedConsumerAssignmentStrategy.constructor.construct(
            consumerFactory,
            consumerProperties,
            readCycleDelayMs,
            numOfConsumersPerKafkaCluster,
            bandwidthThrottler,
            recordsThrottler,
            kafkaClusterBasedRecordThrottler,
            metricsRepository,
            kafkaClusterUrlToAliasMap.getOrDefault(url, url),
            sharedConsumerNonExistingTopicCleanupDelayMS,
            topicExistenceChecker,
            liveConfigBasedKafkaThrottlingEnabled,
            pubSubDeserializer,
            SystemTime.INSTANCE,
            null));

    if (!consumerService.isRunning()) {
      consumerService.start();
    }
    return consumerService;
  }

  public boolean hasConsumerAssignedFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService == null) {
      return false;
    }
    SharedKafkaConsumer consumer =
        consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, new TopicPartition(topic, partition));
    return consumer != null && consumer.hasSubscription(topic, partition);
  }

  boolean hasConsumerAssignedFor(String versionTopic, String topic, int partition) {
    for (String kafkaUrl: kafkaServerToConsumerServiceMap.keySet()) {
      if (hasConsumerAssignedFor(kafkaUrl, versionTopic, topic, partition)) {
        return true;
      }
    }
    return false;
  }

  boolean hasAnyConsumerAssignedForVersionTopic(String versionTopic) {
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      if (consumerService.hasAnySubscriptionFor(versionTopic)) {
        return true;
      }
    }
    return false;
  }

  void resetOffsetFor(String versionTopic, String topic, int partition) {
    KafkaConsumerWrapper consumer;
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, topicPartition);
      if (consumer != null) {
        consumer.resetOffset(topic, partition);
      }
    }
  }

  void unsubscribeConsumerFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService != null) {
      consumerService.unSubscribe(versionTopic, topic, partition);
    }
  }

  public void unsubscribeConsumerFor(String versionTopic, String topic, int partition) {
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.unSubscribe(versionTopic, topic, partition);
    }
  }

  void batchUnsubscribeConsumerFor(String versionTopic, Set<PubSubTopicPartition> topicPartitionSet) {
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumerService.batchUnsubscribe(versionTopic, topicPartitionSet);
    }
  }

  public ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> subscribeConsumerFor(
      final String kafkaURL,
      StoreIngestionTask storeIngestionTask,
      PubSubTopicPartition topicPartition,
      long lastOffset) {
    String versionTopic = storeIngestionTask.getVersionTopic();
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService == null) {
      throw new VeniceException(
          "Kafka consumer service must exist for version topic: " + versionTopic + " in Kafka cluster: " + kafkaURL);
    }

    ConsumedDataReceiver<List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> dataReceiver =
        new StorePartitionDataReceiver(
            storeIngestionTask,
            topicPartition,
            kafkaURL,
            kafkaClusterUrlToIdMap.getOrDefault(kafkaURL, -1));

    consumerService.startConsumptionIntoDataReceiver(
        /** TODO: Refactor this to use {@link PubSubTopicPartition} as well */
        new TopicPartition(topicPartition.getPubSubTopic().getName(), topicPartition.getPartitionNumber()),
        lastOffset,
        dataReceiver);

    return dataReceiver;
  }

  public long getOffsetLagFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    return consumerService == null ? -1 : consumerService.getOffsetLagFor(versionTopic, topic, partition);
  }

  public long getLatestOffsetFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    return consumerService == null ? -1 : consumerService.getLatestOffsetFor(versionTopic, topic, partition);
  }

  /**
   * This will be called when an ingestion task is killed. {@link AggKafkaConsumerService}
   * will try to stop all subscription associated with the given version topic.
   */
  void unsubscribeAll(String versionTopic) {
    kafkaServerToConsumerServiceMap.values().forEach(consumerService -> consumerService.unsubscribeAll(versionTopic));
  }

  void pauseConsumerFor(String versionTopic, String topic, int partition) {
    KafkaConsumerWrapper consumer;
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, topicPartition);
      if (consumer != null) {
        consumer.pause(topic, partition);
      }
    }
  }

  void resumeConsumerFor(String versionTopic, String topic, int partition) {
    KafkaConsumerWrapper consumer;
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopicPartition(versionTopic, topicPartition);
      if (consumer != null) {
        consumer.resume(topic, partition);
      }
    }
  }

  /**
   * It returns all Kafka URLs of the consumers assigned for {@param versionTopic}.
   */
  Set<String> getKafkaUrlsFor(String versionTopic) {
    Set<String> kafkaUrls = new HashSet<>(kafkaServerToConsumerServiceMap.size());
    for (Map.Entry<String, KafkaConsumerService> entry: kafkaServerToConsumerServiceMap.entrySet()) {
      if (entry.getValue().hasAnySubscriptionFor(versionTopic)) {
        kafkaUrls.add(entry.getKey());
      }
    }
    return kafkaUrls;
  }
}
