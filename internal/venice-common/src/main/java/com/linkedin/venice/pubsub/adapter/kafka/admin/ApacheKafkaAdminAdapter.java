package com.linkedin.venice.pubsub.adapter.kafka.admin;

import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import com.linkedin.venice.utils.ExceptionUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An implementation of {@link PubSubAdminAdapter} for Apache Kafka.
 */
public class ApacheKafkaAdminAdapter implements PubSubAdminAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminAdapter.class);
  private final AdminClient internalKafkaAdminClient;
  private final ApacheKafkaAdminConfig apacheKafkaAdminConfig;
  private final PubSubTopicRepository pubSubTopicRepository;

  public ApacheKafkaAdminAdapter(
      ApacheKafkaAdminConfig apacheKafkaAdminConfig,
      PubSubTopicRepository pubSubTopicRepository) {
    this(
        AdminClient.create(
            Objects.requireNonNull(
                apacheKafkaAdminConfig.getAdminProperties(),
                "Properties for kafka admin construction cannot be null!")),
        apacheKafkaAdminConfig,
        pubSubTopicRepository);
  }

  ApacheKafkaAdminAdapter(
      AdminClient internalKafkaAdminClient,
      ApacheKafkaAdminConfig apacheKafkaAdminConfig,
      PubSubTopicRepository pubSubTopicRepository) {
    this.apacheKafkaAdminConfig = Objects.requireNonNull(apacheKafkaAdminConfig, "Kafka admin config cannot be null!");
    this.internalKafkaAdminClient =
        Objects.requireNonNull(internalKafkaAdminClient, "Kafka admin client cannot be null!");
    this.pubSubTopicRepository = pubSubTopicRepository;
    LOGGER.info("Created ApacheKafkaAdminAdapter with config: {}", apacheKafkaAdminConfig);
  }

  /**
   * Creates a new topic in the PubSub system with the given parameters.
   *
   * @param pubSubTopic The topic to be created.
   * @param numPartitions The number of partitions to be created for the topic.
   * @param replicationFactor The number of replicas for each partition.
   * @param pubSubTopicConfiguration Additional topic configuration such as retention, compaction policy, etc.
   * @throws IllegalArgumentException If the replication factor is invalid.
   * @throws PubSubTopicExistsException If a topic with the same name already exists.
   * @throws PubSubClientRetriableException If the operation failed due to a retriable error.
   * @throws PubSubClientException For all other issues related to the PubSub client.
   */
  @Override
  public void createTopic(
      PubSubTopic pubSubTopic,
      int numPartitions,
      int replicationFactor,
      PubSubTopicConfiguration pubSubTopicConfiguration) {
    if (replicationFactor < 1 || replicationFactor > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Replication factor cannot be > " + Short.MAX_VALUE + " or < 1");
    }
    // Convert topic configuration into properties
    Properties topicProperties = unmarshallProperties(pubSubTopicConfiguration);
    Map<String, String> topicPropertiesMap = new HashMap<>(topicProperties.size());
    topicProperties.stringPropertyNames().forEach(key -> topicPropertiesMap.put(key, topicProperties.getProperty(key)));
    Collection<NewTopic> newTopics = Collections.singleton(
        new NewTopic(pubSubTopic.getName(), numPartitions, (short) replicationFactor).configs(topicPropertiesMap));
    LOGGER.info(
        "Creating kafka topic: {} with numPartitions: {}, replicationFactor: {} and properties: {}",
        pubSubTopic.getName(),
        numPartitions,
        replicationFactor,
        topicProperties);
    try {
      CreateTopicsResult createTopicsResult = internalKafkaAdminClient.createTopics(newTopics);
      KafkaFuture<Void> topicCreationFuture = createTopicsResult.all();
      topicCreationFuture.get(); // block until topic creation is complete

      int actualNumPartitions = createTopicsResult.numPartitions(pubSubTopic.getName()).get();
      int actualReplicationFactor = createTopicsResult.replicationFactor(pubSubTopic.getName()).get();
      Config actualTopicConfig = createTopicsResult.config(pubSubTopic.getName()).get();

      if (actualNumPartitions != numPartitions) {
        throw new PubSubClientException(
            String.format(
                "Kafka topic %s was created with incorrect num of partitions - requested: %d actual: %d",
                pubSubTopic.getName(),
                numPartitions,
                actualNumPartitions));
      }
      LOGGER.info(
          "Successfully created kafka topic: {} with numPartitions: {}, replicationFactor: {} and properties: {}",
          pubSubTopic.getName(),
          actualNumPartitions,
          actualReplicationFactor,
          actualTopicConfig);
    } catch (ExecutionException e) {
      LOGGER.debug(
          "Failed to create kafka topic: {} with numPartitions: {}, replicationFactor: {} and properties: {}",
          pubSubTopic.getName(),
          numPartitions,
          replicationFactor,
          topicProperties,
          e);
      if (ExceptionUtils.recursiveClassEquals(e, TopicExistsException.class)) {
        throw new PubSubTopicExistsException(pubSubTopic, e);
      }
      // We have been treating InvalidReplicationFactorException as retriable exception, hence keeping it that way
      if (e.getCause() instanceof RetriableException || e.getCause() instanceof InvalidReplicationFactorException) {
        throw new PubSubClientRetriableException("Failed to create kafka topic: " + pubSubTopic, e);
      }
      throw new PubSubClientException("Failed to create kafka topic: " + pubSubTopic + " due to ExecutionException", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.debug(
          "Failed to create kafka topic: {} with numPartitions: {}, replicationFactor: {} and properties: {}",
          pubSubTopic.getName(),
          numPartitions,
          replicationFactor,
          topicProperties,
          e);
      throw new PubSubClientException("Failed to create kafka topic: " + pubSubTopic + "due to Exception", e);
    }
  }

  /**
   * Delete a given topic.
   * The calling thread will block until the topic is deleted or the timeout is reached.
   *
   * @param pubSubTopic The topic to delete.
   * @param timeout The maximum duration to wait for the deletion to complete.
   *
   * @throws PubSubTopicDoesNotExistException If the topic does not exist.
   * @throws PubSubOpTimeoutException If the operation times out.
   * @throws PubSubClientRetriableException If the operation fails and can be retried.
   * @throws PubSubClientException For all other issues related to the PubSub client.
   */
  @Override
  public void deleteTopic(PubSubTopic pubSubTopic, Duration timeout) {
    try {
      LOGGER.debug("Deleting kafka topic: {}", pubSubTopic.getName());
      DeleteTopicsResult deleteTopicsResult =
          internalKafkaAdminClient.deleteTopics(Collections.singleton(pubSubTopic.getName()));
      KafkaFuture<Void> topicDeletionFuture = deleteTopicsResult.all();
      topicDeletionFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS); // block until topic deletion is complete
      LOGGER.debug("Successfully deleted kafka topic: {}", pubSubTopic.getName());
    } catch (ExecutionException e) {
      LOGGER.debug("Failed to delete kafka topic: {}", pubSubTopic.getName(), e);
      if (ExceptionUtils.recursiveClassEquals(e, UnknownTopicOrPartitionException.class)) {
        throw new PubSubTopicDoesNotExistException(pubSubTopic.getName(), e);
      }
      if (ExceptionUtils.recursiveClassEquals(e, TimeoutException.class)) {
        throw new PubSubOpTimeoutException("Timed out while deleting kafka topic: " + pubSubTopic.getName(), e);
      }
      if (e.getCause() instanceof RetriableException) {
        throw new PubSubClientRetriableException("Failed to delete kafka topic: " + pubSubTopic.getName(), e);
      }
      throw new PubSubClientException("Failed to delete topic: " + pubSubTopic.getName(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.debug("Failed to delete kafka topic: {}", pubSubTopic.getName(), e);
      throw new PubSubClientException("Interrupted while deleting kafka topic: " + pubSubTopic.getName(), e);
    } catch (java.util.concurrent.TimeoutException e) {
      LOGGER.debug("Failed to delete kafka topic: {}", pubSubTopic.getName(), e);
      throw new PubSubOpTimeoutException("Timed out while deleting kafka topic: " + pubSubTopic.getName(), e);
    }
  }

  /**
   * Retrieves the configuration of a Kafka topic.
   *
   * @param pubSubTopic The PubSubTopic representing the Kafka topic for which to retrieve the configuration.
   * @return The configuration of the specified Kafka topic as a PubSubTopicConfiguration object.
   *
   * @throws PubSubTopicDoesNotExistException If the specified Kafka topic does not exist.
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to retrieve the configuration.
   * @throws PubSubClientException If an error occurs while attempting to retrieve the configuration or if the current thread is interrupted while attempting to retrieve the configuration.
   */
  @Override
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic pubSubTopic) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, pubSubTopic.getName());
    Collection<ConfigResource> configResources = Collections.singleton(resource);
    DescribeConfigsResult describeConfigsResult = internalKafkaAdminClient.describeConfigs(configResources);
    try {
      Config config = describeConfigsResult.all().get().get(resource);
      return marshallProperties(config);
    } catch (ExecutionException e) {
      LOGGER.debug("Failed to get configs for kafka topic: {}", pubSubTopic, e);
      if (ExceptionUtils.recursiveClassEquals(e, UnknownTopicOrPartitionException.class)) {
        throw new PubSubTopicDoesNotExistException(pubSubTopic, e);
      }
      if (e.getCause() instanceof RetriableException) {
        throw new PubSubClientRetriableException("Failed to get configs for kafka topic: " + pubSubTopic, e);
      }
      throw new PubSubClientException("Failed to get configs for kafka topic: " + pubSubTopic, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Interrupted while getting configs for kafka topic: " + pubSubTopic, e);
    }
  }

  /**
   * Retrieves a set of all available PubSub topics from the Kafka cluster.
   *
   * @return A Set of PubSubTopic objects representing all available Kafka topics.
   *
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to retrieve the list of topics.
   * @throws PubSubClientException If an error occurs while attempting to retrieve the list of topics or the current thread is interrupted while attempting to retrieve the list of topics.
   */
  @Override
  public Set<PubSubTopic> listAllTopics() {
    ListTopicsResult listTopicsResult = internalKafkaAdminClient.listTopics();
    try {
      return listTopicsResult.names()
          .get()
          .stream()
          .map(t -> pubSubTopicRepository.getTopic(t))
          .collect(Collectors.toSet());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RetriableException) {
        throw new PubSubClientRetriableException("Failed to list topics", e);
      }
      throw new PubSubClientException("Failed to list topics", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Interrupted while listing topics", e);
    }
  }

  /**
   * Sets the configuration for a Kafka topic.
   *
   * @param pubSubTopic The PubSubTopic for which to set the configuration.
   * @param pubSubTopicConfiguration The configuration to be set for the specified Kafka topic.
   * @throws PubSubTopicDoesNotExistException If the specified Kafka topic does not exist.
   * @throws PubSubClientException If an error occurs while attempting to set the topic configuration or if the current thread is interrupted while attempting to set the topic configuration.
   */
  @Override
  public void setTopicConfig(PubSubTopic pubSubTopic, PubSubTopicConfiguration pubSubTopicConfiguration)
      throws PubSubTopicDoesNotExistException {
    Properties topicProperties = unmarshallProperties(pubSubTopicConfiguration);
    Collection<ConfigEntry> entries = new ArrayList<>(topicProperties.stringPropertyNames().size());
    topicProperties.stringPropertyNames()
        .forEach(key -> entries.add(new ConfigEntry(key, topicProperties.getProperty(key))));
    Map<ConfigResource, Config> configs = Collections
        .singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, pubSubTopic.getName()), new Config(entries));
    try {
      internalKafkaAdminClient.alterConfigs(configs).all().get();
    } catch (ExecutionException e) {
      if (!containsTopicWithExpectationAndRetry(pubSubTopic, 3, true)) {
        // We assume the exception was caused by a non-existent topic.
        throw new PubSubTopicDoesNotExistException("Topic " + pubSubTopic + " does not exist", e);
      }
      // Topic exists. So not sure what caused the exception.
      throw new PubSubClientException(
          "Topic: " + pubSubTopic + " exists but failed to set config due to exception: ",
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Interrupted while setting topic config for topic: " + pubSubTopic, e);
    }
  }

  /**
   * Checks if a Kafka topic exists.
   *
   * @param pubSubTopic The PubSubTopic to check for existence.
   * @return true if the specified Kafka topic exists, false otherwise.
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to check topic existence.
   * @throws PubSubClientException If an error occurs while attempting to check topic existence.
   */
  @Override
  public boolean containsTopic(PubSubTopic pubSubTopic) {
    try {
      Collection<String> topicNames = Collections.singleton(pubSubTopic.getName());
      TopicDescription topicDescription =
          internalKafkaAdminClient.describeTopics(topicNames).values().get(pubSubTopic.getName()).get();
      if (topicDescription == null) {
        LOGGER.warn(
            "Unexpected: kafkaAdminClient.describeTopics returned null "
                + "(rather than throwing an InvalidTopicException). Will carry on assuming the topic doesn't exist.");
        return false;
      }

      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        // Topic doesn't exist...
        return false;
      }
      if (e.getCause() instanceof RetriableException) {
        throw new PubSubClientRetriableException("Failed to check if '" + pubSubTopic + " exists!", e);
      }
      throw new PubSubClientException("Failed to check if '" + pubSubTopic + " exists!", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Failed to check if '" + pubSubTopic + " exists!", e);
    }
  }

  /**
   * Checks if a topic exists and has the given partition
   *
   * @param pubSubTopicPartition The PubSubTopicPartition representing the Kafka topic and partition to check.
   * @return true if the specified Kafka topic partition exists, false otherwise.
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to check partition existence.
   * @throws PubSubClientException If an error occurs while attempting to check partition existence or of the current thread is interrupted while attempting to check partition existence.
   */
  @Override
  public boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition) {
    PubSubTopic pubSubTopic = pubSubTopicPartition.getPubSubTopic();
    int partitionID = pubSubTopicPartition.getPartitionNumber();
    try {

      Collection<String> topicNames = Collections.singleton(pubSubTopic.getName());
      TopicDescription topicDescription =
          internalKafkaAdminClient.describeTopics(topicNames).values().get(pubSubTopic.getName()).get();

      if (topicDescription == null) {
        LOGGER.warn(
            "Unexpected: kafkaAdminClient.describeTopics returned null "
                + "(rather than throwing an InvalidTopicException). Will carry on assuming the topic doesn't exist.");
        return false;
      }

      if (topicDescription.partitions().size() <= partitionID) {
        LOGGER.warn(
            "{} is trying to check partitionID {}, but total partitions count is {}. "
                + "Will carry on assuming the topic doesn't exist.",
            pubSubTopic,
            partitionID,
            topicDescription.partitions().size());
        return false;
      }
      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        // Topic doesn't exist...
        return false;
      }
      if (e.getCause() instanceof RetriableException) {
        throw new PubSubClientRetriableException("Failed to check if '" + pubSubTopicPartition + " exists!", e);
      }
      throw new PubSubClientException("Failed to check if '" + pubSubTopicPartition + " exists!", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Failed to check if '" + pubSubTopicPartition + " exists!", e);
    }
  }

  private PubSubTopicConfiguration marshallProperties(Config config) {
    Properties properties = new Properties();
    /** marshall the configs from {@link ConfigEntry} to {@link Properties} */
    config.entries().forEach(configEntry -> properties.setProperty(configEntry.name(), configEntry.value()));
    Optional<Long> retentionMs =
        Optional.ofNullable(properties.getProperty(TopicConfig.RETENTION_MS_CONFIG)).map(Long::parseLong);
    Optional<Integer> minInSyncReplicas =
        Optional.ofNullable(properties.getProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)).map(Integer::parseInt);
    Boolean isLogCompacted =
        TopicConfig.CLEANUP_POLICY_COMPACT.equals(properties.getProperty(TopicConfig.CLEANUP_POLICY_CONFIG));
    Long minLogCompactionLagMs = properties.containsKey(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG)
        ? Long.parseLong((String) properties.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG))
        : 0L;
    Optional<Long> maxLogCompactionLagMs = properties.containsKey(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG)
        ? Optional.of(Long.parseLong(properties.getProperty(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG)))
        : Optional.empty();
    return new PubSubTopicConfiguration(
        retentionMs,
        isLogCompacted,
        minInSyncReplicas,
        minLogCompactionLagMs,
        maxLogCompactionLagMs);
  }

  private Properties unmarshallProperties(PubSubTopicConfiguration pubSubTopicConfiguration) {
    Properties topicProperties = new Properties();
    Optional<Long> retentionMs = pubSubTopicConfiguration.retentionInMs();
    if (retentionMs.isPresent()) {
      topicProperties.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionMs.get()));
    }
    if (pubSubTopicConfiguration.isLogCompacted()) {
      topicProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
      topicProperties.put(
          TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,
          Long.toString(pubSubTopicConfiguration.minLogCompactionLagMs()));
      if (pubSubTopicConfiguration.getMaxLogCompactionLagMs().isPresent()) {
        topicProperties.put(
            TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,
            Long.toString(pubSubTopicConfiguration.getMaxLogCompactionLagMs().get()));
      }
    } else {
      topicProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    }
    // If not set, Kafka cluster defaults will apply
    pubSubTopicConfiguration.minInSyncReplicas()
        .ifPresent(
            minIsrConfig -> topicProperties
                .put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minIsrConfig)));
    // Just in case the Kafka cluster isn't configured as expected.
    topicProperties.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime");
    return topicProperties;
  }

  /**
   * Retrieves the retention settings for all Kafka topics.
   *
   * @return A map of Kafka topics and their corresponding retention settings in milliseconds.
   * If a topic does not have a retention setting, it will be mapped to {@link PubSubConstants#PUBSUB_TOPIC_UNKNOWN_RETENTION}.
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to retrieve retention settings.
   * @throws PubSubClientException If an error occurs while attempting to retrieve retention settings or if the current thread is interrupted while attempting to retrieve retention settings.
   */
  @Override
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return getSomethingForAllTopics(
        config -> Optional.ofNullable(config.get(TopicConfig.RETENTION_MS_CONFIG))
            // Option A: perform a string-to-long conversion if it's present...
            .map(configEntry -> Long.parseLong(configEntry.value()))
            // Option B: ... or default to a sentinel value if it's missing
            .orElse(PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION),
        "retention");
  }

  private <T> Map<PubSubTopic, T> getSomethingForAllTopics(Function<Config, T> configTransformer, String content) {
    try {
      Set<PubSubTopic> pubSubTopics = internalKafkaAdminClient.listTopics()
          .names()
          .get()
          .stream()
          .map(t -> pubSubTopicRepository.getTopic(t))
          .collect(Collectors.toSet());
      return getSomethingForSomeTopics(pubSubTopics, configTransformer, content);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RetriableException) {
        throw new PubSubClientRetriableException("Failed to get " + content + " for all topics", e);
      }
      throw new PubSubClientException("Failed to get " + content + " for all topics", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Interrupted while getting " + content + " for all topics", e);
    }
  }

  /**
   * Retrieves the configurations for a set of Kafka topics.
   *
   * @param pubSubTopics The set of Kafka topics to retrieve configurations for.
   * @return A map of Kafka topics and their corresponding configurations.
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to retrieve configurations.
   * @throws PubSubClientException If an error occurs while attempting to retrieve configurations or if the current thread is interrupted while attempting to retrieve configurations.
   */
  @Override
  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> pubSubTopics) {
    return getSomethingForSomeTopics(pubSubTopics, config -> marshallProperties(config), "configs");
  }

  private <T> Map<PubSubTopic, T> getSomethingForSomeTopics(
      Set<PubSubTopic> pubSubTopics,
      Function<Config, T> configTransformer,
      String content) {
    Map<PubSubTopic, T> topicToSomething = new HashMap<>();
    try {
      // Step 1: Marshall topic names into config resources
      Collection<ConfigResource> configResources = pubSubTopics.stream()
          .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName.getName()))
          .collect(Collectors.toCollection(ArrayList::new));

      // Step 2: retrieve the configs of specified topics
      internalKafkaAdminClient.describeConfigs(configResources)
          .all()
          .get()
          // Step 3: populate the map to be returned
          .forEach(
              (configResource, config) -> topicToSomething.put(
                  pubSubTopicRepository.getTopic(configResource.name()),
                  // Step 4: transform the config
                  configTransformer.apply(config)));

      return topicToSomething;
    } catch (ExecutionException e) {
      int numberOfTopic = pubSubTopics.size();
      String numberOfTopicString = numberOfTopic + " topic" + (numberOfTopic > 1 ? "s" : "");
      if (e.getCause() instanceof RetriableException) {
        throw new PubSubClientRetriableException("Failed to get " + content + " for " + numberOfTopicString, e);
      }
      throw new PubSubClientException("Failed to get " + content + " for " + numberOfTopicString, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Interrupted while getting " + content + " for some topics", e);
    } catch (Exception e) {
      throw new PubSubClientException("Failed to get " + content + " for some topics", e);
    }
  }

  @Override
  public String getClassName() {
    return ApacheKafkaAdminAdapter.class.getName();
  }

  @Override
  public void close() throws IOException {
    if (this.internalKafkaAdminClient != null) {
      try {
        this.internalKafkaAdminClient.close(Duration.ofSeconds(60));
      } catch (Exception e) {
        LOGGER.warn("Exception (suppressed) during kafkaAdminClient.close()", e);
      }
    }
  }

  @Override
  public long getTopicConfigMaxRetryInMs() {
    return apacheKafkaAdminConfig.getTopicConfigMaxRetryInMs();
  }
}
