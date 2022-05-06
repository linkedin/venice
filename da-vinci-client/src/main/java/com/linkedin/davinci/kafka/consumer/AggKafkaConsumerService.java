package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
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

  public AggKafkaConsumerService(final KafkaClientFactory consumerFactory, final VeniceServerConfig serverConfig,
      final EventThrottler bandwidthThrottler, final EventThrottler recordsThrottler,
      KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler, final MetricsRepository metricsRepository,
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
    logger.info("Successfully initialized AggKafkaConsumerService");
  }

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    for (KafkaConsumerService consumerService : kafkaServerToConsumerServiceMap.values()) {
      consumerService.stop();
    }
  }

  /**
   * Get {@link KafkaConsumerService} for a specific Kafka bootstrap url.
   */
  public Optional<KafkaConsumerService> getKafkaConsumerService(final String kafkaURL) {
    return Optional.ofNullable(kafkaServerToConsumerServiceMap.get(kafkaURL));
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

    //TODO: introduce builder pattern due to significant # of parameters for KafkaConsumerService.
    switch (sharedConsumerAssignmentStrategy) {
      case PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY:
        return kafkaServerToConsumerServiceMap.computeIfAbsent(kafkaUrl, url -> {
          KafkaConsumerServiceStats stats = createKafkaConsumerServiceStats(url);
          return new PartitionWiseKafkaConsumerService(consumerFactory, consumerProperties, readCycleDelayMs, numOfConsumersPerKafkaCluster,
              bandwidthThrottler, recordsThrottler, kafkaClusterBasedRecordThrottler, stats, sharedConsumerNonExistingTopicCleanupDelayMS,
              topicExistenceChecker, liveConfigBasedKafkaThrottlingEnabled);
        });
      case TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY:
        return kafkaServerToConsumerServiceMap.computeIfAbsent(kafkaUrl, url -> {
          KafkaConsumerServiceStats stats = createKafkaConsumerServiceStats(url);
          return new TopicWiseKafkaConsumerService(consumerFactory, consumerProperties, readCycleDelayMs, numOfConsumersPerKafkaCluster,
              bandwidthThrottler, recordsThrottler, kafkaClusterBasedRecordThrottler, stats, sharedConsumerNonExistingTopicCleanupDelayMS,
              topicExistenceChecker, liveConfigBasedKafkaThrottlingEnabled);
        });
      default:
        throw new VeniceException("Unknown shared consumer assignment strategy: " + sharedConsumerAssignmentStrategy);
    }
  }

  public Optional<KafkaConsumerWrapper> assignConsumerFor(final String kafkaURL, StoreIngestionTask ingestionTask) {
    Optional<KafkaConsumerService> consumerService = getKafkaConsumerService(kafkaURL);
    if (!consumerService.isPresent()) {
      return Optional.empty();
    }
    KafkaConsumerWrapper selectedConsumer = consumerService.get().assignConsumerFor(ingestionTask);
    consumerService.get().attach(selectedConsumer, ingestionTask.getVersionTopic(), ingestionTask);
    return Optional.of(selectedConsumer);
  }

  public boolean hasConsumerAssignedFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    Optional<KafkaConsumerService> consumerService = getKafkaConsumerService(kafkaURL);
    if (!consumerService.isPresent()) {
      return false;
    }
    Optional<KafkaConsumerWrapper> consumer = consumerService.get().getConsumerAssignedToVersionTopic(versionTopic);
    if (consumer.isPresent()) {
      if (consumer.get().hasSubscription(topic, partition)) {
          return true;
      }
    }
    return false;
  }

  public boolean hasConsumerAssignedFor(String versionTopic, String topic, int partition) {
    for (String kafkaUrl : kafkaServerToConsumerServiceMap.keySet()) {
      if (hasConsumerAssignedFor(kafkaUrl, versionTopic, topic, partition)) {
        return true;
      }
    }
    return false;
  }

  public boolean hasAnyConsumerAssignedFor(String versionTopic, Set<String> topics) {
    for (KafkaConsumerService consumerService : kafkaServerToConsumerServiceMap.values()) {
      Optional<KafkaConsumerWrapper> consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer.isPresent()) {
        if (consumer.get().hasSubscribedAnyTopic(topics)) {
          return true;
        }
      }
    }
    return false;
  }

  public void resetOffsetFor(String versionTopic, String topic, int partition) {
    for (KafkaConsumerService consumerService : kafkaServerToConsumerServiceMap.values()) {
      Optional<KafkaConsumerWrapper> consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer.isPresent()) {
        consumer.get().resetOffset(topic, partition);
      }
    }
  }

  public void unsubscribeConsumerFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    Optional<KafkaConsumerService> consumerService = getKafkaConsumerService(kafkaURL);
    if (!consumerService.isPresent()) {
      return;
    }
    Optional<KafkaConsumerWrapper> consumer = consumerService.get().getConsumerAssignedToVersionTopic(versionTopic);
    if (consumer.isPresent()) {
      consumer.get().unSubscribe(topic, partition);
    }
  }

  public void unsubscribeConsumerFor(String versionTopic, String topic, int partition) {
    for (String kafkaUrl : kafkaServerToConsumerServiceMap.keySet()) {
      unsubscribeConsumerFor(kafkaUrl, versionTopic, topic, partition);
    }
  }

  public void batchUnsubscribeConsumerFor(String versionTopic, Set<TopicPartition> topicPartitionSet) {
    for (KafkaConsumerService consumerService : kafkaServerToConsumerServiceMap.values()) {
      Optional<KafkaConsumerWrapper> consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer.isPresent()) {
        consumer.get().batchUnsubscribe(topicPartitionSet);
      }
    }
  }

  public void subscribeConsumerFor(final String kafkaURL, StoreIngestionTask storeIngestionTask,
      String topic, int partition, long lastOffset) {
    String versionTopic = storeIngestionTask.getVersionTopic();
    Optional<KafkaConsumerService> consumerService = getKafkaConsumerService(kafkaURL);
    if (!consumerService.isPresent()) {
      throw new VeniceException("Kafka consumer service must exist for version topic: " + versionTopic
          + " in Kafka cluster: " + kafkaURL);
    }

    Optional<KafkaConsumerWrapper> consumer = consumerService.get().getConsumerAssignedToVersionTopic(versionTopic);
    if (!consumer.isPresent()) {
      consumer = this.assignConsumerFor(kafkaURL, storeIngestionTask);
    }

    if (consumer.isPresent()) {
      consumerService.get().attach(consumer.get(), topic, storeIngestionTask);
      consumer.get().subscribe(topic, partition, lastOffset);
    } else {
      throw new VeniceException("Shared consumer must exist for version topic: " + versionTopic + " in Kafka cluster: "
          + kafkaURL);
    }
  }

  public Optional<Long> getOffsetLagFor(final String kafkaURL, String versionTopic, String topic, int partition) {
    Optional<KafkaConsumerService> consumerService = getKafkaConsumerService(kafkaURL);
    if (!consumerService.isPresent()) {
      return Optional.empty();
    }

    Optional<KafkaConsumerWrapper> consumer = consumerService.get().getConsumerAssignedToVersionTopic(versionTopic);
    if (!consumer.isPresent()) {
      return Optional.empty();
    }
    return consumer.get().getOffsetLag(topic, partition);
  }

  /**
   * {@link #unsubscribeAll(String)} will be called when an ingestion task is killed, {@link AggKafkaConsumerService}
   * will try to stop all subscription associated with the given version topic.
   * @param versionTopic
   */
  public void unsubscribeAll(String versionTopic) {
    kafkaServerToConsumerServiceMap.values().forEach(consumerService -> consumerService.unsubscribeAll(versionTopic));
  }

  private KafkaConsumerServiceStats createKafkaConsumerServiceStats(String kafkaUrl) {
    // Allow integration test with kafka url like "localhost:1234" to pass with default.
    String kafkaClusterAlias = kafkaClusterUrlToAliasMap.getOrDefault(kafkaUrl, kafkaUrl);
    String nameWithKafkaClusterAlias = "kafka_consumer_service_for_" + kafkaClusterAlias;
    return new KafkaConsumerServiceStats(metricsRepository, nameWithKafkaClusterAlias);
  }

  public void pauseConsumerFor(String versionTopic, String topic, int partition) {
    for (KafkaConsumerService consumerService : kafkaServerToConsumerServiceMap.values()) {
      Optional<KafkaConsumerWrapper> consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer.isPresent()) {
        consumer.get().pause(topic, partition);
      }
    }
  }

  public void resumeConsumerFor(String versionTopic, String topic, int partition) {
    for (KafkaConsumerService consumerService : kafkaServerToConsumerServiceMap.values()) {
      Optional<KafkaConsumerWrapper> consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer.isPresent()) {
        consumer.get().resume(topic, partition);
      }
    }
  }

  /**
   * It returns all Kafka URLs of the consumers assigned for {@param versionTopic}.
   * @param versionTopic
   */
  public Set<String> getKafkaUrlsFor(String versionTopic) {
    Set<String> kafkaUrls = new HashSet<>();
    kafkaServerToConsumerServiceMap.forEach((kafkaUrl, consumerService) -> {
      Optional<KafkaConsumerWrapper> consumer = consumerService.getConsumerAssignedToVersionTopic(versionTopic);
      if (consumer.isPresent()) {
        kafkaUrls.add(kafkaUrl);
      }
    });
    return kafkaUrls;
  }

}
