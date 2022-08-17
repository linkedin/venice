package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link AggKafkaConsumerService} supports Kafka consumer pool for multiple Kafka clusters from different data centers;
 * for each Kafka bootstrap server url, {@link AggKafkaConsumerService} will create one {@link KafkaConsumerService}.
 */
public class AggKafkaConsumerService extends AbstractVeniceService {
  private static final Logger logger = LogManager.getLogger(AggKafkaConsumerService.class);

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

  public AggKafkaConsumerService(
      final KafkaClientFactory consumerFactory,
      final VeniceServerConfig serverConfig,
      final EventThrottler bandwidthThrottler,
      final EventThrottler recordsThrottler,
      KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler,
      final MetricsRepository metricsRepository,
      TopicExistenceChecker topicExistenceChecker) {
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
    logger.info("Successfully initialized AggKafkaConsumerService");
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
  public KafkaConsumerService getKafkaConsumerService(final String kafkaURL) {
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
      logger.warn("KafkaConsumerService has already been created for Kafka cluster with URL: {}", kafkaUrl);
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
            liveConfigBasedKafkaThrottlingEnabled));

    if (!consumerService.isRunning()) {
      consumerService.start();
    }
    return consumerService;
  }

  /**
   * Assigns one of the pre-allocated consumers to the provided {@link StoreIngestionTask}.
   *
   * @return the consumer which was assigned,
   *         or null if the {@param kafkaURL} is invalid
   */
  public KafkaConsumerWrapper assignConsumerFor(final String kafkaURL, StoreIngestionTask ingestionTask) {
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService == null) {
      return null;
    }
    KafkaConsumerWrapper selectedConsumer = consumerService.assignConsumerFor(ingestionTask);
    consumerService.attach(selectedConsumer, ingestionTask.getVersionTopic(), ingestionTask);
    return selectedConsumer;
  }

  public void setKafkaConsumerService(String kafkaUrl, KafkaConsumerService kafkaConsumerService) {
    kafkaServerToConsumerServiceMap.putIfAbsent(kafkaUrl, kafkaConsumerService);
  }

  public boolean hasConsumerAssignedFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService == null) {
      return false;
    }
    KafkaConsumerWrapper consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
    return consumer != null && consumer.hasSubscription(topic, partition);
  }

  public boolean hasConsumerAssignedFor(String versionTopic, String topic, int partition) {
    for (String kafkaUrl: kafkaServerToConsumerServiceMap.keySet()) {
      if (hasConsumerAssignedFor(kafkaUrl, versionTopic, topic, partition)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasAnyConsumerAssignedFor(String versionTopic, Set<String> topics) {
    KafkaConsumerWrapper consumer;
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer != null) {
        if (consumer.hasSubscribedAnyTopic(topics)) {
          return true;
        }
      }
    }
    return false;
  }

  public void resetOffsetFor(String versionTopic, String topic, int partition) {
    KafkaConsumerWrapper consumer;
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer != null) {
        consumer.resetOffset(topic, partition);
      }
    }
  }

  public void unsubscribeConsumerFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService != null) {
      KafkaConsumerWrapper consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer != null) {
        consumer.unSubscribe(topic, partition);
      }
    }
  }

  public void unsubscribeConsumerFor(String versionTopic, String topic, int partition) {
    for (String kafkaUrl: kafkaServerToConsumerServiceMap.keySet()) {
      unsubscribeConsumerFor(kafkaUrl, versionTopic, topic, partition);
    }
  }

  public void batchUnsubscribeConsumerFor(String versionTopic, Set<TopicPartition> topicPartitionSet) {
    KafkaConsumerWrapper consumer;
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer != null) {
        consumer.batchUnsubscribe(topicPartitionSet);
      }
    }
  }

  public ConsumedDataReceiver<List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> subscribeConsumerFor(
      final String kafkaURL,
      StoreIngestionTask storeIngestionTask,
      String topic,
      int partition,
      long lastOffset) {
    String versionTopic = storeIngestionTask.getVersionTopic();
    KafkaConsumerService consumerService = getKafkaConsumerService(kafkaURL);
    if (consumerService == null) {
      throw new VeniceException(
          "Kafka consumer service must exist for version topic: " + versionTopic + " in Kafka cluster: " + kafkaURL);
    }

    KafkaConsumerWrapper consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
    if (consumer == null) {
      consumer = this.assignConsumerFor(kafkaURL, storeIngestionTask);
    }

    if (consumer != null) {
      consumerService.attach(consumer, topic, storeIngestionTask);
      consumer.subscribe(topic, partition, lastOffset);
    } else {
      throw new VeniceException(
          "Shared consumer must exist for version topic: " + versionTopic + " in Kafka cluster: " + kafkaURL);
    }
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    ConsumedDataReceiver<List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> dataReceiver =
        new StorePartitionDataReceiver(
            storeIngestionTask,
            topicPartition,
            kafkaURL,
            kafkaClusterUrlToIdMap.getOrDefault(kafkaURL, -1));
    consumerService.setDataReceiver(topicPartition, dataReceiver);
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
  public void unsubscribeAll(String versionTopic) {
    kafkaServerToConsumerServiceMap.values().forEach(consumerService -> consumerService.unsubscribeAll(versionTopic));
  }

  public void pauseConsumerFor(String versionTopic, String topic, int partition) {
    KafkaConsumerWrapper consumer;
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer != null) {
        consumer.pause(topic, partition);
      }
    }
  }

  public void resumeConsumerFor(String versionTopic, String topic, int partition) {
    KafkaConsumerWrapper consumer;
    for (KafkaConsumerService consumerService: kafkaServerToConsumerServiceMap.values()) {
      consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer != null) {
        consumer.resume(topic, partition);
      }
    }
  }

  /**
   * It returns all Kafka URLs of the consumers assigned for {@param versionTopic}.
   */
  public Set<String> getKafkaUrlsFor(String versionTopic) {
    Set<String> kafkaUrls = new HashSet<>(kafkaServerToConsumerServiceMap.size());
    KafkaConsumerWrapper consumer;
    for (Map.Entry<String, KafkaConsumerService> entry: kafkaServerToConsumerServiceMap.entrySet()) {
      consumer = entry.getValue().getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer != null) {
        kafkaUrls.add(entry.getKey());
      }
    }
    return kafkaUrls;
  }
}
