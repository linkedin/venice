package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;


public class PubSubAdminAdapterContext {
  private final String pubSubBrokerAddress;
  private final String adminClientName;
  private final VeniceProperties veniceProperties;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
  private final PubSubSecurityProtocol pubSubSecurityProtocol;
  private final MetricsRepository metricsRepository;

  private PubSubAdminAdapterContext(Builder builder) {
    this.pubSubBrokerAddress = builder.pubSubBrokerAddress;
    this.adminClientName = builder.adminClientName;
    this.veniceProperties = builder.veniceProperties;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.pubSubSecurityProtocol = builder.pubSubSecurityProtocol;
    this.metricsRepository = builder.metricsRepository;
    this.pubSubPositionTypeRegistry = builder.pubSubPositionTypeRegistry;
  }

  public String getPubSubBrokerAddress() {
    return pubSubBrokerAddress;
  }

  public VeniceProperties getVeniceProperties() {
    return veniceProperties;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public String getAdminClientName() {
    return adminClientName;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public PubSubSecurityProtocol getPubSubSecurityProtocol() {
    return pubSubSecurityProtocol;
  }

  public PubSubPositionTypeRegistry getPubSubPositionTypeRegistry() {
    return pubSubPositionTypeRegistry;
  }

  public static class Builder {
    private String pubSubBrokerAddress;
    private String adminClientName;
    private VeniceProperties veniceProperties;
    private PubSubSecurityProtocol pubSubSecurityProtocol;
    private PubSubTopicRepository pubSubTopicRepository;
    private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
    private MetricsRepository metricsRepository;

    public Builder setPubSubBrokerAddress(String pubSubBrokerAddress) {
      this.pubSubBrokerAddress = pubSubBrokerAddress;
      return this;
    }

    public Builder setAdminClientName(String adminClientName) {
      this.adminClientName = adminClientName;
      return this;
    }

    public Builder setVeniceProperties(VeniceProperties veniceProperties) {
      this.veniceProperties = veniceProperties;
      return this;
    }

    public Builder setPubSubTopicRepository(PubSubTopicRepository pubSubTopicRepository) {
      this.pubSubTopicRepository = pubSubTopicRepository;
      return this;
    }

    public Builder setMetricsRepository(MetricsRepository metricsRepository) {
      this.metricsRepository = metricsRepository;
      return this;
    }

    public Builder setPubSubSecurityProtocol(PubSubSecurityProtocol pubSubSecurityProtocol) {
      this.pubSubSecurityProtocol = pubSubSecurityProtocol;
      return this;
    }

    public Builder setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry pubSubPositionTypeRegistry) {
      this.pubSubPositionTypeRegistry = pubSubPositionTypeRegistry;
      return this;
    }

    public PubSubAdminAdapterContext build() {
      if (veniceProperties == null) {
        throw new IllegalArgumentException("Missing required properties. Please specify the properties.");
      }
      if (pubSubBrokerAddress == null) {
        pubSubBrokerAddress = PubSubUtil.getPubSubBrokerAddressOrFail(veniceProperties);
      }
      if (pubSubTopicRepository == null) {
        throw new IllegalArgumentException("Missing required topic repository. Please specify the topic repository.");
      }
      if (pubSubPositionTypeRegistry == null) {
        throw new IllegalArgumentException(
            "Missing required position type registry. Please specify the position type registry.");
      }
      if (pubSubSecurityProtocol == null) {
        pubSubSecurityProtocol = PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProperties);
      }
      adminClientName = PubSubUtil.generatePubSubClientId(PubSubClientType.ADMIN, adminClientName, pubSubBrokerAddress);
      return new PubSubAdminAdapterContext(this);
    }
  }
}
