package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A repository of {@link TopicManager} instances, each associated with a specific PubSub region and cluster.
 * This repository maintains one {@link TopicManager} for each unique PubSub bootstrap server address.
 * While not mandatory, it is expected that each Venice component will have one and only one instance of this class.
 */
public class TopicManagerRepository implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(TopicManagerRepository.class);
  private final Map<String, TopicManager> topicManagers = new VeniceConcurrentHashMap<>();
  private final TopicManagerContext topicManagerContext;
  private final String localPubSubAddress;

  public TopicManagerRepository(TopicManagerContext topicManagerContext, String localPubSubAddress) {
    this.topicManagerContext = Objects.requireNonNull(topicManagerContext, "topicManagerContext cannot be null");
    this.localPubSubAddress = Objects.requireNonNull(localPubSubAddress, "localPubSubAddress cannot be null");
  }

  // added in order to help with testing; visibility is package-private for testing purposes
  TopicManager createTopicManager(String pubSubAddress) {
    return new TopicManager(pubSubAddress, topicManagerContext);
  }

  /**
   * By default, return TopicManager for local PubSub cluster.
   */
  public TopicManager getLocalTopicManager() {
    return getTopicManager(localPubSubAddress);
  }

  public TopicManager getTopicManager(String pubSubAddress) {
    return topicManagers.computeIfAbsent(pubSubAddress, this::createTopicManager);
  }

  Collection<TopicManager> getAllTopicManagers() {
    return topicManagers.values();
  }

  /**
   * Invalidates the cache for the given PubSub topic across all TopicManagers in the repository.
   * @param pubSubTopic the PubSub topic to invalidate
   */
  public void invalidateTopicManagerCaches(PubSubTopic pubSubTopic) {
    for (TopicManager topicManager: getAllTopicManagers()) {
      topicManager.invalidateCache(pubSubTopic);
    }
  }

  @Override
  public void close() {
    for (TopicManager topicManager: topicManagers.values()) {
      topicManager.close();
    }
    LOGGER.info("TopicManagerRepository has started closing all TopicManagers in the repository.");
  }
}
