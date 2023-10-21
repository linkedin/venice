package com.linkedin.venice.pubsub.manager;

import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  public Collection<TopicManager> getAllTopicManagers() {
    return topicManagers.values();
  }

  @Override
  public void close() {
    long startTime = System.currentTimeMillis();
    List<CompletableFuture<Void>> closeFutures = new ArrayList<>(topicManagers.size());
    for (TopicManager topicManager: topicManagers.values()) {
      closeFutures.add(topicManager.closeAsync());
    }
    try {
      CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[0])).get(2, TimeUnit.MINUTES);
      LOGGER.info(
          "All TopicManagers in the TopicManagerRepository have been closed in {} ms",
          System.currentTimeMillis() - startTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted while waiting for TopicManagers to close", e);
    } catch (ExecutionException | TimeoutException e) {
      // log and ignore exception
      LOGGER.error("Error when closing TopicManagerRepository", e);
    }
  }
}
