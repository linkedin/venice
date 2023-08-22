package com.linkedin.venice.kafka;

import static com.linkedin.venice.ConfigConstants.DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.VeniceProperties;
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
  private final Map<String, TopicManager> topicManagersMap = new VeniceConcurrentHashMap<>();
  private final Function<String, TopicManager> topicManagerCreator;
  private final Lazy<TopicManager> localTopicManager;

  public TopicManagerRepository(Builder builder) {
    this.topicManagerCreator = (kafkaServerAddress) -> new TopicManager(builder, kafkaServerAddress);
    this.localTopicManager = Lazy.of(
        () -> topicManagersMap.computeIfAbsent(
            builder.localKafkaBootstrapServers,
            k -> topicManagerCreator.apply(builder.localKafkaBootstrapServers)));
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

  /**
   * @return a new builder for the {@link TopicManagerRepository}
   */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private volatile boolean built = false;
    private String localKafkaBootstrapServers;
    private long kafkaOperationTimeoutMs = DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;
    private long topicDeletionStatusPollIntervalMs = DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS;
    private long topicMinLogCompactionLagMs = DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
    private PubSubClientsFactory defaultPubSubClientsFactory;
    private Map<String, PubSubClientsFactory> pubSubClientsFactoryMap;
    private PubSubTopicRepository pubSubTopicRepository;
    private MetricsRepository metricsRepository;
    private SSLPropertiesSupplier pubSubProperties;
    private ClusterNameSupplier clusterNameSupplier;

    private interface Setter {
      void apply();
    }

    private Builder set(Setter setter) {
      if (!built) {
        setter.apply();
      }
      return this;
    }

    public TopicManagerRepository build() {
      // flip the build flag to prevent modification.
      this.built = true;
      return new TopicManagerRepository(this);
    }

    public String getLocalKafkaBootstrapServers() {
      return localKafkaBootstrapServers;
    }

    public long getKafkaOperationTimeoutMs() {
      return kafkaOperationTimeoutMs;
    }

    public long getTopicDeletionStatusPollIntervalMs() {
      return topicDeletionStatusPollIntervalMs;
    }

    public long getTopicMinLogCompactionLagMs() {
      return topicMinLogCompactionLagMs;
    }

    public MetricsRepository getMetricsRepository() {
      return metricsRepository;
    }

    public PubSubTopicRepository getPubSubTopicRepository() {
      return pubSubTopicRepository;
    }

    public Map<String, PubSubClientsFactory> getPubSubClientsFactoryMap() {
      return pubSubClientsFactoryMap;
    }

    public PubSubClientsFactory getPubSubClientsFactory() {
      return defaultPubSubClientsFactory;
    }

    public SSLPropertiesSupplier getPubSubProperties() {
      return pubSubProperties;
    }

    public ClusterNameSupplier getClusterNameSupplier() {
      return clusterNameSupplier;
    }

    public Builder setLocalKafkaBootstrapServers(String localKafkaBootstrapServers) {
      return set(() -> this.localKafkaBootstrapServers = localKafkaBootstrapServers);
    }

    public Builder setKafkaOperationTimeoutMs(long kafkaOperationTimeoutMs) {
      return set(() -> this.kafkaOperationTimeoutMs = kafkaOperationTimeoutMs);
    }

    public Builder setTopicDeletionStatusPollIntervalMs(long topicDeletionStatusPollIntervalMs) {
      return set(() -> this.topicDeletionStatusPollIntervalMs = topicDeletionStatusPollIntervalMs);
    }

    public Builder setTopicMinLogCompactionLagMs(long topicMinLogCompactionLagMs) {
      return set(() -> this.topicMinLogCompactionLagMs = topicMinLogCompactionLagMs);
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      return set(() -> this.metricsRepository = metricsRepository);
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      return set(() -> this.pubSubTopicRepository = pubSubTopicRepository);
    }

    public Builder setDefaultPubSubClientsFactory(PubSubClientsFactory pubSubClientsFactory) {
      return set(() -> this.defaultPubSubClientsFactory = pubSubClientsFactory);
    }

    public Builder setPubSubClientsFactoryMap(Map<String, PubSubClientsFactory> pubSubClientsFactoryMap) {
      return set(() -> this.pubSubClientsFactoryMap = pubSubClientsFactoryMap);
    }

    public Builder setPubSubProperties(SSLPropertiesSupplier pubSubProperties) {
      return set(() -> this.pubSubProperties = pubSubProperties);
    }

    public Builder setClusterNameSupplier(ClusterNameSupplier clusterNameSupplier) {
      return set(() -> this.clusterNameSupplier = clusterNameSupplier);
    }
  }

  public interface SSLPropertiesSupplier {
    VeniceProperties get(String pubSubBootstrapServers);
  }

  public interface ClusterNameSupplier {
    Optional<String> get(PubSubTopic pubSubTopic);
  }
}
