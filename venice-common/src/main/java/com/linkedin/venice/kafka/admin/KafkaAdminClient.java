package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.log4j.Logger;


public class KafkaAdminClient implements KafkaAdminWrapper {
  private static final Logger logger = Logger.getLogger(KafkaAdminClient.class);

  private Properties properties;
  private AdminClient kafkaAdminClient;

  public KafkaAdminClient() {}

  @Override
  public void initialize(Properties properties) {
    if (null == properties) {
      throw new IllegalArgumentException("properties cannot be null!");
    }
    this.properties = properties;
  }

  @Override
  public void createTopic(String topicName, int numPartitions, int replication, Properties topicProperties) {
    if (replication > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Replication factor cannot be > " + Short.MAX_VALUE);
    }
    Collection<NewTopic> newTopics = new ArrayList<>();
    Map<String, String> topicPropertiesMap = new HashMap<>();
    topicProperties.stringPropertyNames().forEach(key -> topicPropertiesMap.put(key, topicProperties.getProperty(key)));
    newTopics.add(new NewTopic(topicName, numPartitions, (short) replication).configs(topicPropertiesMap));
    try {
      getKafkaAdminClient().createTopics(newTopics).all().get();
    } catch (InvalidReplicationFactorException e) {
      throw e;
    } catch (Exception e) {
      throw new VeniceException("Failed to create a topic!", e);
    }
  }

  @Override
  public void deleteTopic(String topicName) {
    Collection<String> topics = new ArrayList<>();
    topics.add(topicName);
    getKafkaAdminClient().deleteTopics(topics).all();
  }

  @Override
  public void setTopicConfig(String topicName, Properties topicProperties) {
    Collection<ConfigEntry> entries = new ArrayList<>();
    topicProperties.stringPropertyNames().forEach(key -> entries.add(new ConfigEntry(key, topicProperties.getProperty(key))));
    Map<ConfigResource, Config> configs = new HashMap<>();
    configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topicName), new Config(entries));
    getKafkaAdminClient().alterConfigs(configs).all();
  }

  @Override
  public Map<String, Long> getAllTopicRetentions() {
    return getSomethingForAllTopics(config ->
        Optional.ofNullable(config.get(TopicConfig.RETENTION_MS_CONFIG))
            // Option A: perform a string-to-long conversion if it's present...
            .map(configEntry -> Long.parseLong(configEntry.value()))
            // Option B: ... or default to a sentinel value if it's missing
            .orElse(TopicManager.UNKNOWN_TOPIC_RETENTION));
  }

  @Override
  public Properties getTopicConfig(String topicName) {
    Collection<ConfigResource> configResources = new ArrayList<>();
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    configResources.add(resource);
    DescribeConfigsResult result = getKafkaAdminClient().describeConfigs(configResources);
    try {
      Config config = result.all().get().get(resource);
      Properties properties = new Properties();
      config.entries().forEach(configEntry -> properties.setProperty(
          configEntry.name(),
          configEntry.value()));
      return properties;
    } catch (Exception e) {
      throw new VeniceException("Failed to get topic configs for: " + topicName, e);
    }
  }

  @Override
  public boolean containsTopic(String topic) {
    try {
      // Another way of doing this... not sure which is better:
      // return getKafkaAdminClient().listTopics().names().get().contains(topic);

      Collection<String> topicNames = new ArrayList<>(1);
      topicNames.add(topic);
      TopicDescription topicDescription = getKafkaAdminClient().describeTopics(topicNames).all().get().get(topic);

      if (null == topicDescription) {
        logger.warn("Unexpected: kafkaAdminClient.describeTopics returned null "
            + "(rather than throwing an InvalidTopicException). Will carry on assuming the topic doesn't exist.");
        return false;
      }

      return true;
    } catch (InvalidTopicException e) {
      // Topic doesn't exist...
      return false;
    } catch (Exception e) {
      throw new VeniceException("Failed to list topics in order to find out if '" + topic + " exists!", e);
    }
  }

  @Override
  public Map<String, Properties> getAllTopicConfig() {
    return getSomethingForAllTopics(config -> {
      Properties properties = new Properties();
      /** marshall the configs from {@link ConfigEntry} to {@link Properties} */
      config.entries().forEach(configEntry -> properties.setProperty(configEntry.name(), configEntry.value()));
      return properties;
    });
  }

  @Override
  public boolean isTopicDeletionUnderway() {
    // TODO: fix this so that we can get rid the Scala Admin in the backend as well...
    // We could achieve the this by passing another kind of ZK client, or perhaps some other way.
    throw new VeniceException("isTopicDeletionUnderway is not supported in " + this.getClass().getSimpleName());
  }

  @Override
  public void close() throws IOException {
    if (null != this.kafkaAdminClient) {
      try {
        this.kafkaAdminClient.close();
      } catch (Exception e) {
        logger.warn("Exception (suppressed) during kafkaAdminClient.close()", e);
      }
    }
  }

  private synchronized AdminClient getKafkaAdminClient() {
    if (null != this.kafkaAdminClient) {
      return this.kafkaAdminClient;
    }

    if (null == this.properties) {
      throw new IllegalStateException("initialize(properties) has not been called!");
    }

    this.kafkaAdminClient = AdminClient.create(this.properties);

    return this.kafkaAdminClient;
  }

  private <T> Map<String, T> getSomethingForAllTopics(Function<Config, T> configTransformer) {
    Map<String, T> topicToSomething = new HashMap<>();
    try {
      // Step 1: retrieve all topics
      Collection<ConfigResource> configResources = getKafkaAdminClient().listTopics().names().get()
          .stream()
          .map(topicName -> new ConfigResource(ConfigResource.Type.TOPIC, topicName))
          .collect(Collectors.toCollection(ArrayList::new));

      // Step 2: retrieve the configs of all topics
      getKafkaAdminClient().describeConfigs(configResources).all().get()
          // Step 3: populate the map to be returned
          .forEach((configResource, config) -> topicToSomething.put(
              configResource.name(),
              // Step 4: transform the config
              configTransformer.apply(config)));

      return topicToSomething;
    } catch (Exception e) {
      throw new VeniceException("Failed to get topic retentions", e);
    }
  }
}
