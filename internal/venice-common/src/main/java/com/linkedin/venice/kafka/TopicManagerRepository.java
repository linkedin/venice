package com.linkedin.venice.kafka;

import static com.linkedin.venice.ConfigConstants.DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS;
import static com.linkedin.venice.VeniceConstants.SCALA_BASED_KAFKA_ADMIN_CLIENT_CLASS_NAME;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory.MetricsParameters;
import com.linkedin.venice.utils.Pair;
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
  private final String localKafkaZkAddress;
  private final int kafkaOperationTimeoutMs;
  private final int topicDeletionStatusPollIntervalMs;
  private final long topicMinLogCompactionLagMs;
  private final KafkaClientFactory kafkaClientFactory;
  private final Function<Pair<String, String>, TopicManager> topicManagerCreator;
  private final Map<String, TopicManager> topicManagersMap = new VeniceConcurrentHashMap<>();
  private final Lazy<TopicManager> localTopicManager;

  public TopicManagerRepository(
      String localKafkaBootstrapServers,
      String localKafkaZkAddress,
      int kafkaOperationTimeoutMs,
      int topicDeletionStatusPollIntervalMs,
      long topicMinLogCompactionLagMs,
      KafkaClientFactory kafkaClientFactory,
      MetricsRepository metricsRepository) {
    this.localKafkaBootstrapServers = localKafkaBootstrapServers;
    this.localKafkaZkAddress = localKafkaZkAddress;
    this.kafkaOperationTimeoutMs = kafkaOperationTimeoutMs;
    this.topicDeletionStatusPollIntervalMs = topicDeletionStatusPollIntervalMs;
    this.topicMinLogCompactionLagMs = topicMinLogCompactionLagMs;
    this.kafkaClientFactory = kafkaClientFactory;
    this.topicManagerCreator = (kafkaServerAndZk) -> {
      MetricsParameters metricsParameters = new MetricsParameters(
          this.kafkaClientFactory.getClass(),
          TopicManager.class,
          kafkaServerAndZk.getFirst(),
          metricsRepository);
      final KafkaClientFactory kafkaClientFactoryClone = this.kafkaClientFactory
          .clone(kafkaServerAndZk.getFirst(), kafkaServerAndZk.getSecond(), Optional.of(metricsParameters));
      return new TopicManager(
          this.kafkaOperationTimeoutMs,
          this.topicDeletionStatusPollIntervalMs,
          this.topicMinLogCompactionLagMs,
          kafkaClientFactoryClone,
          Optional.of(metricsRepository));
    };
    this.localTopicManager = Lazy.of(
        () -> topicManagersMap.computeIfAbsent(
            this.localKafkaBootstrapServers,
            k -> topicManagerCreator.apply(Pair.create(this.localKafkaBootstrapServers, this.localKafkaZkAddress))));
  }

  public TopicManagerRepository(
      String localKafkaBootstrapServers,
      String localKafkaZkAddress,
      KafkaClientFactory kafkaClientFactory,
      MetricsRepository metricsRepository) {
    this(
        localKafkaBootstrapServers,
        localKafkaZkAddress,
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS,
        DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS,
        kafkaClientFactory,
        metricsRepository);
  }

  /**
   * By default, return TopicManager for local Kafka cluster.
   */
  public TopicManager getTopicManager() {
    return localTopicManager.get();
  }

  public TopicManager getTopicManager(Pair<String, String> kafkaServerAndZk) {
    return topicManagersMap
        .computeIfAbsent(kafkaServerAndZk.getFirst(), k -> topicManagerCreator.apply(kafkaServerAndZk));
  }

  /**
   * Only use this method via java-based topic manager repository.
   * @param kafkaBootstrapServers
   * @return
   */
  public TopicManager getTopicManager(String kafkaBootstrapServers) {
    // Creating remote topic manager by remote Kafka server requires Java-based Kafka admin client
    if (kafkaClientFactory.getKafkaAdminClass().contains(SCALA_BASED_KAFKA_ADMIN_CLIENT_CLASS_NAME)) {
      throw new VeniceException("Kafka ZK address is required by Scala Kafka admin client.");
    }
    return topicManagersMap
        .computeIfAbsent(kafkaBootstrapServers, k -> topicManagerCreator.apply(Pair.create(kafkaBootstrapServers, "")));
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
