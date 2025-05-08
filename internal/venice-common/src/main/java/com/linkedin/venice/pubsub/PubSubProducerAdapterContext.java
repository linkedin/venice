package com.linkedin.venice.pubsub;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubMessageSerializer;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Encapsulates all dependencies and configurations required to create a producer using a producer factory.
 * This class serves as a unified context for managing producer setup across different PubSub systems.
 *
 * <p>Common configurations applicable to all PubSub implementations should be defined as member variables
 * in this class, while system-specific configurations should be stored in {@link VeniceProperties}.</p>
 *
 * <p>Each PubSub implementation is expected to interpret both the common configurations and the
 * PubSub-specific settings based on namespace-scoped configurations.</p>
 *
 */
public class PubSubProducerAdapterContext {
  private static final Logger LOGGER = LogManager.getLogger(PubSubProducerAdapterContext.class);
  private final String producerName;
  private final String brokerAddress;
  private final VeniceProperties veniceProperties;
  private final PubSubSecurityProtocol securityProtocol;
  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
  private final MetricsRepository metricsRepository;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final boolean shouldValidateProducerConfigStrictly;
  private final PubSubMessageSerializer pubSubMessageSerializer;
  private final boolean isProducerCompressionEnabled;
  private final String compressionType;

  private PubSubProducerAdapterContext(Builder builder) {
    this.producerName = builder.producerName;
    this.brokerAddress = builder.brokerAddress;
    this.veniceProperties = builder.veniceProperties;
    this.securityProtocol = builder.securityProtocol;
    this.metricsRepository = builder.metricsRepository;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.shouldValidateProducerConfigStrictly = builder.shouldValidateProducerConfigStrictly;
    this.pubSubMessageSerializer = builder.pubSubMessageSerializer;
    this.isProducerCompressionEnabled = builder.isProducerCompressionEnabled;
    this.compressionType = builder.compressionType;
    this.pubSubPositionTypeRegistry = builder.pubSubPositionTypeRegistry;
  }

  public String getProducerName() {
    return producerName;
  }

  public String getBrokerAddress() {
    return brokerAddress;
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

  public PubSubPositionTypeRegistry getPubSubPositionTypeRegistry() {
    return pubSubPositionTypeRegistry;
  }

  public boolean isProducerCompressionEnabled() {
    return isProducerCompressionEnabled;
  }

  public String getCompressionType() {
    return compressionType;
  }

  public static class Builder {
    private String producerName;
    private String brokerAddress;
    private VeniceProperties veniceProperties;
    private PubSubSecurityProtocol securityProtocol;
    private PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
    private MetricsRepository metricsRepository;
    private PubSubTopicRepository pubSubTopicRepository;
    private PubSubMessageSerializer pubSubMessageSerializer;
    private boolean shouldValidateProducerConfigStrictly = true;
    private boolean isProducerCompressionEnabled = true;
    private String compressionType;

    public Builder setProducerName(String producerName) {
      this.producerName = producerName;
      return this;
    }

    public Builder setBrokerAddress(String brokerAddress) {
      this.brokerAddress = brokerAddress;
      return this;
    }

    public Builder setVeniceProperties(VeniceProperties veniceProperties) {
      this.veniceProperties = veniceProperties;
      return this;
    }

    public Builder setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry pubSubPositionTypeRegistry) {
      this.pubSubPositionTypeRegistry =
          Objects.requireNonNull(pubSubPositionTypeRegistry, "PubSubPositionTypeRegistry cannot be null");
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

    public Builder setProducerCompressionEnabled(boolean isProducerCompressionEnabled) {
      this.isProducerCompressionEnabled = isProducerCompressionEnabled;
      return this;
    }

    public Builder setCompressionType(String compressionType) {
      this.compressionType = compressionType;
      return this;
    }

    public PubSubProducerAdapterContext build() {
      if (pubSubPositionTypeRegistry == null) {
        LOGGER.info(
            "PubSubPositionMapper is not set. Using default reserved position type registry: {}",
            PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY);
        pubSubPositionTypeRegistry = PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;
      }
      if (brokerAddress == null) {
        throw new VeniceException("Broker address must be provided to create a pub-sub producer");
      }
      if (veniceProperties == null) {
        veniceProperties = VeniceProperties.empty();
      }
      if (pubSubMessageSerializer == null) {
        pubSubMessageSerializer = PubSubMessageSerializer.DEFAULT_PUBSUB_SERIALIZER;
      }
      if (!isProducerCompressionEnabled) {
        compressionType = "none";
      } else if (compressionType == null) {
        compressionType = "gzip";
      }

      producerName = PubSubUtil.generatePubSubClientId(PubSubClientType.PRODUCER, producerName, brokerAddress);
      return new PubSubProducerAdapterContext(this);
    }
  }
}
