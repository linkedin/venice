package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;


/**
 * Encapsulates all dependencies and configurations required to create a consumer using a consumer factory.
 * This class serves as a unified context for managing consumer setup across different PubSub systems.
 *
 * <p>Common configurations applicable to all PubSub implementations should be defined as member variables
 * in this class, while system-specific configurations should be stored in {@link VeniceProperties}.</p>
 *
 * <p>Each PubSub implementation is expected to interpret both the common configurations and the
 * PubSub-specific settings based on namespace-scoped configurations.</p>
 */
public class PubSubConsumerAdapterContext {
  private final String consumerName;
  private final String pubSubBrokerAddress;
  private final VeniceProperties veniceProperties;
  private final PubSubSecurityProtocol pubSubSecurityProtocol;
  private final MetricsRepository metricsRepository;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final boolean isOffsetCollectionEnabled;

  private PubSubConsumerAdapterContext(Builder builder) {
    this.consumerName = builder.consumerName;
    this.pubSubBrokerAddress = builder.pubSubBrokerAddress;
    this.veniceProperties = builder.veniceProperties;
    this.pubSubSecurityProtocol = builder.pubSubSecurityProtocol;
    this.metricsRepository = builder.metricsRepository;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.isOffsetCollectionEnabled = builder.isOffsetCollectionEnabled;
    this.pubSubMessageDeserializer = builder.pubSubMessageDeserializer;
  }

  public String getConsumerName() {
    return consumerName;
  }

  public String getPubSubBrokerAddress() {
    return pubSubBrokerAddress;
  }

  public VeniceProperties getVeniceProperties() {
    return veniceProperties;
  }

  public PubSubSecurityProtocol getPubSubSecurityProtocol() {
    return pubSubSecurityProtocol;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public boolean isOffsetCollectionEnabled() {
    return isOffsetCollectionEnabled;
  }

  public PubSubMessageDeserializer getPubSubMessageDeserializer() {
    return pubSubMessageDeserializer;
  }

  public static class Builder {
    private String consumerName;
    private String pubSubBrokerAddress;
    private VeniceProperties veniceProperties;
    private PubSubSecurityProtocol pubSubSecurityProtocol;
    private MetricsRepository metricsRepository;
    private PubSubTopicRepository pubSubTopicRepository;
    private boolean isOffsetCollectionEnabled;
    private PubSubMessageDeserializer pubSubMessageDeserializer;

    public Builder setConsumerName(String consumerName) {
      this.consumerName = consumerName;
      return this;
    }

    public Builder setPubSubBrokerAddress(String pubSubBrokerAddress) {
      this.pubSubBrokerAddress = pubSubBrokerAddress;
      return this;
    }

    public Builder setVeniceProperties(VeniceProperties veniceProperties) {
      this.veniceProperties = veniceProperties;
      return this;
    }

    public Builder setPubSubSecurityProtocol(PubSubSecurityProtocol pubSubSecurityProtocol) {
      this.pubSubSecurityProtocol = pubSubSecurityProtocol;
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
      return this;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository = pubSubTopicRepository;
      return this;
    }

    public Builder setIsOffsetCollectionEnabled(boolean shouldValidateProducerConfigStrictly) {
      this.isOffsetCollectionEnabled = shouldValidateProducerConfigStrictly;
      return this;
    }

    public Builder setPubSubMessageDeserializer(PubSubMessageDeserializer pubSubMessageDeserializer) {
      this.pubSubMessageDeserializer = pubSubMessageDeserializer;
      return this;
    }

    public PubSubConsumerAdapterContext build() {

      if (veniceProperties == null) {
        throw new IllegalArgumentException("Venice properties cannot be null.");
      }
      if (pubSubBrokerAddress == null) {
        pubSubBrokerAddress = PubSubUtil.getPubSubBrokerAddressOrFail(veniceProperties);
      }

      if (pubSubSecurityProtocol == null) {
        // throw new VeniceException("PubSubSecurityProtocol must be provided to create a pub-sub consumer");
        pubSubSecurityProtocol = PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProperties);
      }

      consumerName = PubSubUtil.generatePubSubClientId(PubSubClientType.CONSUMER, consumerName, pubSubBrokerAddress);

      return new PubSubConsumerAdapterContext(this);
    }
  }
}
