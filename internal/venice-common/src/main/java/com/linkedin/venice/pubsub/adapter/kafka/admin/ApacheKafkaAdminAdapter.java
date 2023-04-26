package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static com.linkedin.venice.ConfigKeys.KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC;
import static com.linkedin.venice.utils.Time.MS_PER_SECOND;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ApacheKafkaAdminAdapter implements PubSubAdminAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaAdminAdapter.class);
  private AdminClient kafkaAdminClient;
  private Long maxRetryInMs;
  private PubSubTopicRepository pubSubTopicRepository;

  public ApacheKafkaAdminAdapter(Properties properties, PubSubTopicRepository pubSubTopicRepository) {
    if (properties == null) {
      throw new IllegalArgumentException("properties cannot be null!");
    }
    this.kafkaAdminClient = AdminClient.create(properties);
    this.maxRetryInMs = (Long) properties.get(KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC) * MS_PER_SECOND;
    this.pubSubTopicRepository = pubSubTopicRepository;
  }

  @Override
  public void createTopic(
      PubSubTopic topic,
      int numPartitions,
      int replication,
      PubSubTopicConfiguration pubSubTopicConfiguration) {
    if (replication > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Replication factor cannot be > " + Short.MAX_VALUE);
    }
    // Convert topic configuration into properties
    Properties topicProperties = unmarshallProperties(pubSubTopicConfiguration);
    Map<String, String> topicPropertiesMap = new HashMap<>();
    topicProperties.stringPropertyNames().forEach(key -> topicPropertiesMap.put(key, topicProperties.getProperty(key)));
    Collection<NewTopic> newTopics = Collections
        .singleton(new NewTopic(topic.getName(), numPartitions, (short) replication).configs(topicPropertiesMap));
    try {
      getKafkaAdminClient().createTopics(newTopics).all().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof InvalidReplicationFactorException) {
        throw (InvalidReplicationFactorException) e.getCause();
      } else if (e.getCause() instanceof TopicExistsException) {
        throw (TopicExistsException) e.getCause();
      } else {
        throw new VeniceException("Failed to create topic: " + topic + " due to ExecutionException", e);
      }
    } catch (Exception e) {
      throw new VeniceException("Failed to create topic: " + topic + "due to Exception", e);
    }
  }

  @Override
  public Set<PubSubTopic> listAllTopics() {
    ListTopicsResult listTopicsResult = getKafkaAdminClient().listTopics();
    try {
      return listTopicsResult.names()
          .get()
          .stream()
          .map(t -> pubSubTopicRepository.getTopic(t))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new VeniceException("Failed to list all topics due to exception: ", e);
    }
  }

  // TODO: If we decide that topic deletion is always going to be blocking then we might want to get the future here and
  // catch/extract any expected exceptions such as UnknownTopicOrPartitionException.
  @Override
  public KafkaFuture<Void> deleteTopic(PubSubTopic topic) {
    return getKafkaAdminClient().deleteTopics(Collections.singleton(topic.getName())).values().get(topic.getName());
  }

  @Override
  public void setTopicConfig(PubSubTopic topic, PubSubTopicConfiguration pubSubTopicConfiguration)
      throws TopicDoesNotExistException {
    Properties topicProperties = unmarshallProperties(pubSubTopicConfiguration);
    Collection<ConfigEntry> entries = new ArrayList<>(topicProperties.stringPropertyNames().size());
    topicProperties.stringPropertyNames()
        .forEach(key -> entries.add(new ConfigEntry(key, topicProperties.getProperty(key))));
    Map<ConfigResource, Config> configs =
        Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, topic.getName()), new Config(entries));
    try {
      getKafkaAdminClient().alterConfigs(configs).all().get();
    } catch (ExecutionException | InterruptedException e) {
      if (!containsTopicWithExpectationAndRetry(topic, 3, true)) {
        // We assume the exception was caused by a non-existent topic.
        throw new TopicDoesNotExistException("Topic " + topic + " does not exist");
      }
      // Topic exists. So not sure what caused the exception.
      throw new VeniceException(e);
    }
  }

  @Override
  public Map<PubSubTopic, Long> getAllTopicRetentions() {
    return getSomethingForAllTopics(
        config -> Optional.ofNullable(config.get(TopicConfig.RETENTION_MS_CONFIG))
            // Option A: perform a string-to-long conversion if it's present...
            .map(configEntry -> Long.parseLong(configEntry.value()))
            // Option B: ... or default to a sentinel value if it's missing
            .orElse(TopicManager.UNKNOWN_TOPIC_RETENTION),
        "retention");
  }

  @Override
  public PubSubTopicConfiguration getTopicConfig(PubSubTopic topic) throws TopicDoesNotExistException {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic.getName());
    Collection<ConfigResource> configResources = Collections.singleton(resource);
    DescribeConfigsResult result = getKafkaAdminClient().describeConfigs(configResources);
    try {
      Config config = result.all().get().get(resource);
      return marshallProperties(config);
    } catch (Exception e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        throw new TopicDoesNotExistException("Topic: " + topic + " doesn't exist");
      }
      throw new VeniceException("Failed to get topic configs for: " + topic, e);
    }
  }

  @Override
  public PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topic) {
    long accumWaitTime = 0;
    long sleepIntervalInMs = 100;
    VeniceException veniceException = null;
    while (accumWaitTime < this.maxRetryInMs) {
      try {
        return getTopicConfig(topic);
      } catch (VeniceException e) {
        veniceException = e;
        Utils.sleep(sleepIntervalInMs);
        accumWaitTime += sleepIntervalInMs;
        sleepIntervalInMs = Math.min(5 * MS_PER_SECOND, sleepIntervalInMs * 2);
      }
    }
    throw new VeniceException(
        "After retrying for " + accumWaitTime + "ms, failed to get topic configs for: " + topic,
        veniceException);
  }

  @Override
  public boolean containsTopic(PubSubTopic topic) {
    try {
      Collection<String> topicNames = Collections.singleton(topic.getName());
      TopicDescription topicDescription =
          getKafkaAdminClient().describeTopics(topicNames).values().get(topic.getName()).get();

      if (topicDescription == null) {
        LOGGER.warn(
            "Unexpected: kafkaAdminClient.describeTopics returned null "
                + "(rather than throwing an InvalidTopicException). Will carry on assuming the topic doesn't exist.");
        return false;
      }

      return true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException || e.getCause() instanceof InvalidTopicException) {
        // Topic doesn't exist...
        return false;
      } else {
        throw new VeniceException("Failed to check if '" + topic + " exists!", e);
      }
    } catch (Exception e) {
      throw new VeniceException("Failed to check if '" + topic + " exists!", e);
    }
  }

  @Override
  public boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition) {
    PubSubTopic pubSubTopic = pubSubTopicPartition.getPubSubTopic();
    int partitionID = pubSubTopicPartition.getPartitionNumber();
    try {

      Collection<String> topicNames = Collections.singleton(pubSubTopic.getName());
      TopicDescription topicDescription =
          getKafkaAdminClient().describeTopics(topicNames).values().get(pubSubTopic.getName()).get();

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
    } catch (Exception e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException || e.getCause() instanceof InvalidTopicException) {
        // Topic doesn't exist...
        return false;
      } else {
        throw new VeniceException("Failed to check if '" + pubSubTopic + " exists!", e);
      }
    }
  }

  @Override
  public List<Class<? extends Throwable>> getRetriableExceptions() {
    return Collections.unmodifiableList(Arrays.asList(VeniceRetriableException.class, TimeoutException.class));
  }

  @Override
  public Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> pubSubTopics) {
    return getSomethingForSomeTopics(pubSubTopics, config -> marshallProperties(config), "configs");
  }

  // TODO: This method should be removed from the interface once we migrate to use Java kafka admin since we no longer
  // need to know if a topic deletion is underway or not. Duplicate calls in the Java kafka admin will not result
  // errors.
  @Override
  public boolean isTopicDeletionUnderway() {
    // Always return false to bypass the checks since concurrent topic delete request for Java kafka admin client is
    // harmless.
    return false;
  }

  @Override
  public String getClassName() {
    return ApacheKafkaAdminAdapter.class.getName();
  }

  @Override
  public void close() throws IOException {
    if (this.kafkaAdminClient != null) {
      try {
        this.kafkaAdminClient.close(Duration.ofSeconds(60));
      } catch (Exception e) {
        LOGGER.warn("Exception (suppressed) during kafkaAdminClient.close()", e);
      }
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
    return new PubSubTopicConfiguration(retentionMs, isLogCompacted, minInSyncReplicas, minLogCompactionLagMs);
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

  private AdminClient getKafkaAdminClient() {
    if (kafkaAdminClient == null) {
      throw new IllegalStateException("initialize(properties) has not been called!");
    }
    return kafkaAdminClient;
  }

  private <T> Map<PubSubTopic, T> getSomethingForAllTopics(Function<Config, T> configTransformer, String content) {
    try {
      Set<PubSubTopic> pubSubTopics = getKafkaAdminClient().listTopics()
          .names()
          .get()
          .stream()
          .map(t -> pubSubTopicRepository.getTopic(t))
          .collect(Collectors.toSet());
      return getSomethingForSomeTopics(pubSubTopics, configTransformer, content);
    } catch (Exception e) {
      throw new VeniceException("Failed to get " + content + " for all topics", e);
    }
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
      getKafkaAdminClient().describeConfigs(configResources)
          .all()
          .get()
          // Step 3: populate the map to be returned
          .forEach(
              (configResource, config) -> topicToSomething.put(
                  pubSubTopicRepository.getTopic(configResource.name()),
                  // Step 4: transform the config
                  configTransformer.apply(config)));

      return topicToSomething;
    } catch (Exception e) {
      int numberOfTopic = pubSubTopics.size();
      String numberOfTopicString = numberOfTopic + " topic" + (numberOfTopic > 1 ? "s" : "");
      throw new VeniceException("Failed to get " + content + " for " + numberOfTopicString, e);
    }
  }
}
