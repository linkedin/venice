package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.listener.StoreChangeNotifier;
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
  private final StoreChangeNotifier storeChangeNotifier;
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final PubSubClientsFactory pubSubClientsFactory;

  private PubSubContext(Builder builder) {
    this.topicManagerRepository = builder.topicManagerRepository;
    this.pubSubPositionTypeRegistry = builder.pubSubPositionTypeRegistry;
    this.pubSubPositionDeserializer = builder.pubSubPositionDeserializer;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.storeChangeNotifier = builder.storeChangeNotifier;
    this.pubSubMessageDeserializer = builder.pubSubMessageDeserializer;
    this.pubSubClientsFactory = builder.pubSubClientsFactory;
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

  public StoreChangeNotifier getStoreChangeNotifier() {
    return storeChangeNotifier;
  }

  public PubSubMessageDeserializer getPubSubMessageDeserializer() {
    return pubSubMessageDeserializer;
  }

  public PubSubClientsFactory getPubSubClientsFactory() {
    return pubSubClientsFactory;
  }

  // Builder for PubSubContext
  public static class Builder {
    private TopicManagerRepository topicManagerRepository;
    private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
    private PubSubPositionDeserializer pubSubPositionDeserializer;
    private PubSubTopicRepository pubSubTopicRepository;
    private StoreChangeNotifier storeChangeNotifier;
    private PubSubMessageDeserializer pubSubMessageDeserializer;
    private PubSubClientsFactory pubSubClientsFactory;

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

    public Builder setStoreChangeNotifier(StoreChangeNotifier storeChangeNotifier) {
      this.storeChangeNotifier = storeChangeNotifier;
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

    public PubSubContext build() {
      return new PubSubContext(this);
    }
  }
}
