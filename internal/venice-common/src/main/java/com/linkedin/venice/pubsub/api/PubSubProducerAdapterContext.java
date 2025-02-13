package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;


public class PubSubProducerAdapterContext {
  private final String producerName;
  private final String targetBrokerAddress;
  private final VeniceProperties veniceProperties;
  private final PubSubSecurityProtocol securityProtocol;
  private final MetricsRepository metricsRepository;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final boolean shouldValidateProducerConfigStrictly;
  private final PubSubMessageSerializer pubSubMessageSerializer;

  private PubSubProducerAdapterContext(Builder builder) {
    this.producerName = builder.producerName;
    this.targetBrokerAddress = builder.targetBrokerAddress;
    this.veniceProperties = builder.veniceProperties;
    this.securityProtocol = builder.securityProtocol;
    this.metricsRepository = builder.metricsRepository;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.shouldValidateProducerConfigStrictly = builder.shouldValidateProducerConfigStrictly;
    this.pubSubMessageSerializer = builder.pubSubMessageSerializer;
  }

  public String getProducerName() {
    return producerName;
  }

  public String getTargetBrokerAddress() {
    return targetBrokerAddress;
  }

  public VeniceProperties getVeniceProperties() {
    return veniceProperties;
  }

  public PubSubSecurityProtocol getSecurityProtocol() {
    return securityProtocol;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public boolean shouldValidateProducerConfigStrictly() {
    return shouldValidateProducerConfigStrictly;
  }

  public PubSubMessageSerializer getPubSubMessageSerializer() {
    return pubSubMessageSerializer;
  }

  public static class Builder {
    private String producerName;
    private String targetBrokerAddress;
    private VeniceProperties veniceProperties;
    private PubSubSecurityProtocol securityProtocol;
    private MetricsRepository metricsRepository;
    private PubSubTopicRepository pubSubTopicRepository;
    private PubSubMessageSerializer pubSubMessageSerializer;
    private boolean shouldValidateProducerConfigStrictly = true;

    public Builder setProducerName(String producerName) {
      this.producerName = producerName;
      return this;
    }

    public Builder setTargetBrokerAddress(String targetBrokerAddress) {
      this.targetBrokerAddress = targetBrokerAddress;
      return this;
    }

    public Builder setVeniceProperties(VeniceProperties veniceProperties) {
      this.veniceProperties = veniceProperties;
      return this;
    }

    public Builder setSecurityProtocol(PubSubSecurityProtocol securityProtocol) {
      this.securityProtocol = securityProtocol;
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

    public Builder setShouldValidateProducerConfigStrictly(boolean shouldValidateProducerConfigStrictly) {
      this.shouldValidateProducerConfigStrictly = shouldValidateProducerConfigStrictly;
      return this;
    }

    public Builder setPubSubMessageSerializer(PubSubMessageSerializer pubSubMessageSerializer) {
      this.pubSubMessageSerializer = pubSubMessageSerializer;
      return this;
    }

    public PubSubProducerAdapterContext build() {
      if (veniceProperties == null) {
        veniceProperties = VeniceProperties.empty();
      }
      if (pubSubMessageSerializer == null) {
        pubSubMessageSerializer = PubSubMessageSerializer.DEFAULT_PUBSUB_SERIALIZER;
      }
      return new PubSubProducerAdapterContext(this);
    }
  }
}
