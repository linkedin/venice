package com.linkedin.venice.pubsub;

import com.linkedin.venice.meta.AsyncStoreChangeNotifier;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;


/**
 * {@code PubSubContext} is a container class that holds all the core components required
 * for managing PubSub infrastructure, including topic managers, position registry,
 * position deserializer, and topic repository.
 */
public class PubSubContext {
  private final TopicManagerRepository topicManagerRepository;
  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
  private final PubSubPositionDeserializer pubSubPositionDeserializer;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final AsyncStoreChangeNotifier asyncStoreChangeNotifier;
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final PubSubClientsFactory pubSubClientsFactory;
  private final boolean useCheckpointedPubSubPositionWithFallback;

  private PubSubContext(Builder builder) {
    this.topicManagerRepository = builder.topicManagerRepository;
    this.pubSubPositionTypeRegistry = builder.pubSubPositionTypeRegistry;
    this.pubSubPositionDeserializer = builder.pubSubPositionDeserializer;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.asyncStoreChangeNotifier = builder.asyncStoreChangeNotifier;
    this.pubSubMessageDeserializer = builder.pubSubMessageDeserializer;
    this.pubSubClientsFactory = builder.pubSubClientsFactory;
    this.useCheckpointedPubSubPositionWithFallback = builder.useCheckpointedPubSubPositionWithFallback;
  }

  public TopicManager getTopicManager(String topicName) {
    return topicManagerRepository.getTopicManager(topicName);
  }

  public TopicManagerRepository getTopicManagerRepository() {
    return topicManagerRepository;
  }

  public PubSubPositionTypeRegistry getPubSubPositionTypeRegistry() {
    return pubSubPositionTypeRegistry;
  }

  public PubSubPositionDeserializer getPubSubPositionDeserializer() {
    return pubSubPositionDeserializer;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public AsyncStoreChangeNotifier getStoreChangeNotifier() {
    return asyncStoreChangeNotifier;
  }

  public PubSubMessageDeserializer getPubSubMessageDeserializer() {
    return pubSubMessageDeserializer;
  }

  public PubSubClientsFactory getPubSubClientsFactory() {
    return pubSubClientsFactory;
  }

  public boolean isUseCheckpointedPubSubPositionWithFallbackEnabled() {
    return useCheckpointedPubSubPositionWithFallback;
  }

  // Builder for PubSubContext
  public static class Builder {
    private TopicManagerRepository topicManagerRepository;
    private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
    private PubSubPositionDeserializer pubSubPositionDeserializer;
    private PubSubTopicRepository pubSubTopicRepository;
    private AsyncStoreChangeNotifier asyncStoreChangeNotifier;
    private PubSubMessageDeserializer pubSubMessageDeserializer;
    private PubSubClientsFactory pubSubClientsFactory;
    private boolean useCheckpointedPubSubPositionWithFallback = true; // Default to true

    public Builder setTopicManagerRepository(TopicManagerRepository topicManagerRepository) {
      this.topicManagerRepository = topicManagerRepository;
      return this;
    }

    public Builder setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry pubSubPositionTypeRegistry) {
      this.pubSubPositionTypeRegistry = pubSubPositionTypeRegistry;
      return this;
    }

    public Builder setPubSubPositionDeserializer(PubSubPositionDeserializer pubSubPositionDeserializer) {
      this.pubSubPositionDeserializer = pubSubPositionDeserializer;
      return this;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository = pubSubTopicRepository;
      return this;
    }

    public Builder setStoreChangeNotifier(AsyncStoreChangeNotifier asyncStoreChangeNotifier) {
      this.asyncStoreChangeNotifier = asyncStoreChangeNotifier;
      return this;
    }

    public Builder setPubSubMessageDeserializer(PubSubMessageDeserializer pubSubMessageDeserializer) {
      this.pubSubMessageDeserializer = pubSubMessageDeserializer;
      return this;
    }

    public Builder setPubSubClientsFactory(PubSubClientsFactory pubSubClientsFactory) {
      this.pubSubClientsFactory = pubSubClientsFactory;
      return this;
    }

    public Builder setUseCheckpointedPubSubPositionWithFallback(boolean useCheckpointedPubSubPositionWithFallback) {
      this.useCheckpointedPubSubPositionWithFallback = useCheckpointedPubSubPositionWithFallback;
      return this;
    }

    public PubSubContext build() {
      return new PubSubContext(this);
    }
  }
}
