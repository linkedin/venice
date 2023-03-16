package com.linkedin.venice.kafka;

import static com.linkedin.venice.ConfigConstants.DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.factory.MetricsParameters;
import com.linkedin.venice.pubsub.factory.PubSubClientFactory;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TopicManagerRepository implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicManagerRepository.class);

  private final String localKafkaBootstrapServers;
  private final int kafkaOperationTimeoutMs;
  private final int topicDeletionStatusPollIntervalMs;
  private final long topicMinLogCompactionLagMs;
  private final PubSubClientFactory pubSubClientFactory;
  private final Function<String, TopicManager> topicManagerCreator;
  private final Map<String, TopicManager> topicManagersMap = new VeniceConcurrentHashMap<>();
  private final Lazy<TopicManager> localTopicManager;

  public TopicManagerRepository(
      String localKafkaBootstrapServers,
      int kafkaOperationTimeoutMs,
      int topicDeletionStatusPollIntervalMs,
      long topicMinLogCompactionLagMs,
      PubSubClientFactory pubSubClientFactory,
      MetricsRepository metricsRepository,
      PubSubTopicRepository pubSubTopicRepository) {
    this.localKafkaBootstrapServers = localKafkaBootstrapServers;
    this.kafkaOperationTimeoutMs = kafkaOperationTimeoutMs;
    this.topicDeletionStatusPollIntervalMs = topicDeletionStatusPollIntervalMs;
    this.topicMinLogCompactionLagMs = topicMinLogCompactionLagMs;
    this.pubSubClientFactory = pubSubClientFactory;
    this.topicManagerCreator = (kafkaServerAddress) -> {
      MetricsParameters metricsParameters = new MetricsParameters(
          this.pubSubClientFactory.getClass(),
          TopicManager.class,
          kafkaServerAddress,
          metricsRepository);
      final PubSubClientFactory pubSubClientFactoryClone =
          this.pubSubClientFactory.clone(kafkaServerAddress, Optional.of(metricsParameters));
      return new TopicManager(
          this.kafkaOperationTimeoutMs,
          this.topicDeletionStatusPollIntervalMs,
          this.topicMinLogCompactionLagMs,
          pubSubClientFactoryClone,
          Optional.of(metricsRepository),
          pubSubTopicRepository);
    };
    this.localTopicManager = Lazy.of(
        () -> topicManagersMap.computeIfAbsent(
            this.localKafkaBootstrapServers,
            k -> topicManagerCreator.apply(this.localKafkaBootstrapServers)));
  }

  public TopicManagerRepository(
      String localKafkaBootstrapServers,
      KafkaClientFactory pubSubClientFactory,
      MetricsRepository metricsRepository,
      PubSubTopicRepository pubSubTopicRepository) {
    this(
        localKafkaBootstrapServers,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS,
        DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS,
        pubSubClientFactory,
        metricsRepository,
        pubSubTopicRepository);
  }

  /**
   * By default, return TopicManager for local Kafka cluster.
   */
  public TopicManager getTopicManager() {
    return localTopicManager.get();
  }

  public TopicManager getTopicManager(String kafkaBootstrapServers) {
    return topicManagersMap
        .computeIfAbsent(kafkaBootstrapServers, k -> topicManagerCreator.apply(kafkaBootstrapServers));
  }

  @Override
  public void close() {
    AtomicReference<Exception> lastException = new AtomicReference<>();
    topicManagersMap.entrySet().stream().forEach(entry -> {
      try {
        LOGGER.info("Closing TopicManager for Kafka cluster [" + entry.getKey() + "]");
        entry.getValue().close();
        LOGGER.info("Closed TopicManager for Kafka cluster [" + entry.getKey() + "]");
      } catch (Exception e) {
        LOGGER.error("Error when closing TopicManager for Kafka cluster [" + entry.getKey() + "]");
        lastException.set(e);
      }
    });
    if (lastException.get() != null) {
      throw new VeniceException(lastException.get());
    }
    LOGGER.info("All TopicManager closed.");
  }
}
