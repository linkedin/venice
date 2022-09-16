package com.linkedin.venice.kafka.admin;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.utils.Time.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class KafkaAdminClient implements KafkaAdminWrapper {
  private static final Logger logger = LogManager.getLogger(KafkaAdminClient.class);
  private AdminClient kafkaAdminClient;
  private Long maxRetryInMs;

  public KafkaAdminClient() {
  }

  @Override
  public void initialize(Properties properties) {
    if (properties == null) {
      throw new IllegalArgumentException("properties cannot be null!");
    }
    this.kafkaAdminClient = AdminClient.create(properties);
    this.maxRetryInMs = (Long) properties.get(KAFKA_ADMIN_GET_TOPIC_CONFIG_MAX_RETRY_TIME_SEC) * MS_PER_SECOND;
  }

  @Override
  public void createTopic(String topicName, int numPartitions, int replication, Properties topicProperties) {
    if (replication > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Replication factor cannot be > " + Short.MAX_VALUE);
    }
    Map<String, String> topicPropertiesMap = new HashMap<>();
    topicProperties.stringPropertyNames().forEach(key -> topicPropertiesMap.put(key, topicProperties.getProperty(key)));
    Collection<NewTopic> newTopics =
        Collections.singleton(new NewTopic(topicName, numPartitions, (short) replication).configs(topicPropertiesMap));
    try {
      getKafkaAdminClient().createTopics(newTopics).all().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof InvalidReplicationFactorException) {
        throw (InvalidReplicationFactorException) e.getCause();
      } else if (e.getCause() instanceof TopicExistsException) {
        throw (TopicExistsException) e.getCause();
      } else {
        throw new VeniceException("Failed to create topic: " + topicName + " due to ExecutionException", e);
      }
    } catch (Exception e) {
      throw new VeniceException("Failed to create topic: " + topicName + "due to Exception", e);
    }
  }

  @Override
  public Set<String> listAllTopics() {
    ListTopicsResult listTopicsResult = getKafkaAdminClient().listTopics();
    try {
      return listTopicsResult.names().get();
    } catch (Exception e) {
      throw new VeniceException("Failed to list all topics due to exception: ", e);
    }
  }

  // TODO: If we decide that topic deletion is always going to be blocking then we might want to get the future here and
  // catch/extract any expected exceptions such as UnknownTopicOrPartitionException.
  @Override
  public KafkaFuture<Void> deleteTopic(String topicName) {
    return getKafkaAdminClient().deleteTopics(Collections.singleton(topicName)).values().get(topicName);
  }

  @Override
  public void setTopicConfig(String topicName, Properties topicProperties) throws TopicDoesNotExistException {
    Collection<ConfigEntry> entries = new ArrayList<>(topicProperties.stringPropertyNames().size());
    topicProperties.stringPropertyNames()
        .forEach(key -> entries.add(new ConfigEntry(key, topicProperties.getProperty(key))));
    Map<ConfigResource, Config> configs =
        Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, topicName), new Config(entries));
    try {
      getKafkaAdminClient().alterConfigs(configs).all().get();
    } catch (ExecutionException | InterruptedException e) {
      if (!containsTopicWithExpectationAndRetry(topicName, 3, true)) {
        // We assume the exception was caused by a non-existent topic.
        throw new TopicDoesNotExistException("Topic " + topicName + " does not exist");
      }
      // Topic exists. So not sure what caused the exception.
      throw new VeniceException(e);
    }
  }

  @Override
  public Map<String, Long> getAllTopicRetentions() {
    return getSomethingForAllTopics(
        config -> Optional.ofNullable(config.get(TopicConfig.RETENTION_MS_CONFIG))
            // Option A: perform a string-to-long conversion if it's present...
            .map(configEntry -> Long.parseLong(configEntry.value()))
            // Option B: ... or default to a sentinel value if it's missing
            .orElse(TopicManager.UNKNOWN_TOPIC_RETENTION),
        "retention");
  }

  @Override
  public Properties getTopicConfig(String topicName) throws TopicDoesNotExistException {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    Collection<ConfigResource> configResources = Collections.singleton(resource);
    DescribeConfigsResult result = getKafkaAdminClient().describeConfigs(configResources);
    try {
      Config config = result.all().get().get(resource);
      Properties properties = new Properties();
      config.entries().forEach(configEntry -> properties.setProperty(configEntry.name(), configEntry.value()));
      return properties;
    } catch (Exception e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException) {
        throw new TopicDoesNotExistException("Topic: " + topicName + " doesn't exist");
      }
      throw new VeniceException("Failed to get topic configs for: " + topicName, e);
    }
  }

  @Override
  public Properties getTopicConfigWithRetry(String topic) {
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
  public boolean containsTopic(String topic) {
    try {
      Collection<String> topicNames = Collections.singleton(topic);
      TopicDescription topicDescription = getKafkaAdminClient().describeTopics(topicNames).values().get(topic).get();

      if (topicDescription == null) {
        logger.warn(
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
  public boolean containsTopicWithPartitionCheck(String topic, int partitionID) {
    try {
      Collection<String> topicNames = Collections.singleton(topic);
      TopicDescription topicDescription = getKafkaAdminClient().describeTopics(topicNames).values().get(topic).get();

      if (topicDescription == null) {
        logger.warn(
            "Unexpected: kafkaAdminClient.describeTopics returned null "
                + "(rather than throwing an InvalidTopicException). Will carry on assuming the topic doesn't exist.");
        return false;
      }

      if (topicDescription.partitions().size() <= partitionID) {
        logger.warn(
            topic + " is trying to check partitionID " + partitionID + ", but total partitions count "
                + topicDescription.partitions().size() + " Will carry on assuming the topic doesn't exist.");
        return false;
      }
      return true;
    } catch (Exception e) {
      if (e.getCause() instanceof UnknownTopicOrPartitionException || e.getCause() instanceof InvalidTopicException) {
        // Topic doesn't exist...
        return false;
      } else {
        throw new VeniceException("Failed to check if '" + topic + " exists!", e);
      }
    }
  }

  @Override
  public Map<String, Properties> getSomeTopicConfigs(Set<String> topicNames) {
    return getSomethingForSomeTopics(topicNames, config -> marshallProperties(config), "configs");
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
    return KafkaAdminClient.class.getName();
  }

  @Override
  public Map<String, KafkaFuture<TopicDescription>> describeTopics(Collection<String> topicNames) {
    return kafkaAdminClient.describeTopics(topicNames).values();
  }

  @Override
  public void close() throws IOException {
    if (this.kafkaAdminClient != null) {
      try {
        this.kafkaAdminClient.close(Duration.ofSeconds(60));
      } catch (Exception e) {
        logger.warn("Exception (suppressed) during kafkaAdminClient.close()", e);
      }
    }
  }

  private Properties marshallProperties(Config config) {
    Properties properties = new Properties();
    /** marshall the configs from {@link ConfigEntry} to {@link Properties} */
    config.entries().forEach(configEntry -> properties.setProperty(configEntry.name(), configEntry.value()));
    return properties;
  }

  private AdminClient getKafkaAdminClient() {
    if (kafkaAdminClient == null) {
      throw new IllegalStateException("initialize(properties) has not been called!");
    }
    return kafkaAdminClient;
  }

  private <T> Map<String, T> getSomethingForAllTopics(Function<Config, T> configTransformer, String content) {
    try {
      Set<String> topicNames = getKafkaAdminClient().listTopics().names().get();

      return getSomethingForSomeTopics(topicNames, configTransformer, content);
    } catch (Exception e) {
      throw new VeniceException("Failed to get " + content + " for all topics", e);
    }
  }

  private <T> Map<String, T> getSomethingForSomeTopics(
      Set<String> topicNames,
      Function<Config, T> configTransformer,
      String content) {
    Map<String, T> topicToSomething = new HashMap<>();
    try {
      // Step 1: Marshall topic names into config resources
      Collection<ConfigResource> configResources = topicNames.stream()
          .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
          .collect(Collectors.toCollection(ArrayList::new));

      // Step 2: retrieve the configs of specified topics
      getKafkaAdminClient().describeConfigs(configResources)
          .all()
          .get()
          // Step 3: populate the map to be returned
          .forEach(
              (configResource, config) -> topicToSomething.put(
                  configResource.name(),
                  // Step 4: transform the config
                  configTransformer.apply(config)));

      return topicToSomething;
    } catch (Exception e) {
      int numberOfTopic = topicNames.size();
      String numberOfTopicString = numberOfTopic + " topic" + (numberOfTopic > 1 ? "s" : "");
      throw new VeniceException("Failed to get " + content + " for " + numberOfTopicString, e);
    }
  }
}
