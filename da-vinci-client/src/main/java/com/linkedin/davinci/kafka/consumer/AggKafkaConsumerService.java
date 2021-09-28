package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.KafkaConsumerServiceStats;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.log4j.Logger;


/**
 * {@link AggKafkaConsumerService} supports Kafka consumer pool for multiple Kafka clusters from different data centers;
 * for each Kafka bootstrap server url, {@link AggKafkaConsumerService} will create one {@link KafkaConsumerService}.
 */
public class AggKafkaConsumerService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(AggKafkaConsumerService.class);

  private final KafkaClientFactory consumerFactory;
  private final int numOfConsumersPerKafkaCluster;
  private final long readCycleDelayMs;
  private final long sharedConsumerNonExistingTopicCleanupDelayMS;
  private final EventThrottler bandwidthThrottler;
  private final EventThrottler recordsThrottler;
  private final KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler;
  private final KafkaConsumerServiceStats stats;
  private final TopicExistenceChecker topicExistenceChecker;
  private final boolean liveConfigBasedKafkaThrottlingEnabled;

  private final Map<String, KafkaConsumerService> kafkaServerToConsumerService = new VeniceConcurrentHashMap<>();

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
    this.stats = new KafkaConsumerServiceStats(metricsRepository);
    this.topicExistenceChecker = topicExistenceChecker;
    this.liveConfigBasedKafkaThrottlingEnabled = serverConfig.isLiveConfigBasedKafkaThrottlingEnabled();
    logger.info("Successfully initialized AggKafkaConsumerService");
  }

  @Override
  public boolean startInner() throws Exception {
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    for (KafkaConsumerService consumerService : kafkaServerToConsumerService.values()) {
      consumerService.stop();
    }
  }

  /**
   * Get {@link KafkaConsumerService} for a specific Kafka bootstrap url; if there is no consumer pool for the required
   * Kafka cluster, create and start a new consumer pool
   */
  public KafkaConsumerService getKafkaConsumerService(final Properties consumerProperties) {
    String kafkaUrl = consumerProperties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    return kafkaServerToConsumerService.computeIfAbsent(kafkaUrl, url ->
        new KafkaConsumerService(consumerFactory, consumerProperties, readCycleDelayMs, numOfConsumersPerKafkaCluster,
            bandwidthThrottler, recordsThrottler, kafkaClusterBasedRecordThrottler, stats, sharedConsumerNonExistingTopicCleanupDelayMS, topicExistenceChecker, liveConfigBasedKafkaThrottlingEnabled));
  }

  /**
   * Get a shared consumer based both consumer properties (which contains Kafka bootstrap url) as well as ingestion task.
   */
  public KafkaConsumerWrapper getConsumer(final Properties consumerProperties, StoreIngestionTask ingestionTask) {
    KafkaConsumerService consumerService = getKafkaConsumerService(consumerProperties);
    KafkaConsumerWrapper selectedConsumer = consumerService.getConsumer(ingestionTask);
    consumerService.attach(selectedConsumer, ingestionTask.getVersionTopic(), ingestionTask);
    return selectedConsumer;
  }

  /**
   * {@link #detach(StoreIngestionTask)} will be called when an ingestion task is killed, {@link AggKafkaConsumerService}
   * will try to detach the ingestion task from all consumer pools; if a consumer pool is not polling records for the
   * ingestion task, it will ignore the detach request.
   * @param ingestionTask
   */
  public void detach(StoreIngestionTask ingestionTask) {
    kafkaServerToConsumerService.values().forEach(consumerService -> consumerService.detach(ingestionTask));
  }
}
