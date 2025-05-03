package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS_DEFAULT_VALUE;

import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.function.Function;


/**
 * A context object that contains all the dependencies needed by {@link TopicManager}.
 */
public class TopicManagerContext {
  private final PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> pubSubAdminAdapterFactory;
  private final PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> pubSubConsumerAdapterFactory;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final MetricsRepository metricsRepository;
  private final VeniceProperties veniceProperties;
  private final Function<String, String> pubSubBrokerUrlResolver;
  private final Function<String, PubSubSecurityProtocol> pubSubSecurityProtocolResolver;
  private final long pubSubOperationTimeoutMs;
  private final long topicDeletionStatusPollIntervalMs;
  private final long topicMinLogCompactionLagMs;
  private final long topicOffsetCheckIntervalMs;
  private final int topicMetadataFetcherConsumerPoolSize;
  private final int topicMetadataFetcherThreadPoolSize;

  private TopicManagerContext(Builder builder) {
    this.pubSubOperationTimeoutMs = builder.pubSubOperationTimeoutMs;
    this.topicDeletionStatusPollIntervalMs = builder.topicDeletionStatusPollIntervalMs;
    this.topicMinLogCompactionLagMs = builder.topicMinLogCompactionLagMs;
    this.pubSubAdminAdapterFactory = builder.pubSubAdminAdapterFactory;
    this.pubSubConsumerAdapterFactory = builder.pubSubConsumerAdapterFactory;
    this.pubSubTopicRepository = builder.pubSubTopicRepository;
    this.metricsRepository = builder.metricsRepository;
    this.veniceProperties = builder.veniceProperties;
    this.pubSubBrokerUrlResolver = builder.pubSubBrokerUrlResolver;
    this.pubSubSecurityProtocolResolver = builder.pubSubSecurityProtocolResolver;
    this.topicOffsetCheckIntervalMs = builder.topicOffsetCheckIntervalMs;
    this.topicMetadataFetcherConsumerPoolSize = builder.topicMetadataFetcherConsumerPoolSize;
    this.topicMetadataFetcherThreadPoolSize = builder.topicMetadataFetcherThreadPoolSize;
  }

  public long getPubSubOperationTimeoutMs() {
    return pubSubOperationTimeoutMs;
  }

  public long getTopicDeletionStatusPollIntervalMs() {
    return topicDeletionStatusPollIntervalMs;
  }

  public long getTopicMinLogCompactionLagMs() {
    return topicMinLogCompactionLagMs;
  }

  public PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> getPubSubAdminAdapterFactory() {
    return pubSubAdminAdapterFactory;
  }

  public PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> getPubSubConsumerAdapterFactory() {
    return pubSubConsumerAdapterFactory;
  }

  public PubSubTopicRepository getPubSubTopicRepository() {
    return pubSubTopicRepository;
  }

  public MetricsRepository getMetricsRepository() {
    return metricsRepository;
  }

  public VeniceProperties getVeniceProperties() {
    return veniceProperties;
  }

  public long getTopicOffsetCheckIntervalMs() {
    return topicOffsetCheckIntervalMs;
  }

  public int getTopicMetadataFetcherConsumerPoolSize() {
    return topicMetadataFetcherConsumerPoolSize;
  }

  public int getTopicMetadataFetcherThreadPoolSize() {
    return topicMetadataFetcherThreadPoolSize;
  }

  public Function<String, String> getPubSubBrokerUrlResolver() {
    return pubSubBrokerUrlResolver;
  }

  public Function<String, PubSubSecurityProtocol> getPubSubSecurityProtocolResolver() {
    return pubSubSecurityProtocolResolver;
  }

  @Override
  public String toString() {
    return "TopicManagerContext{pubSubOperationTimeoutMs=" + pubSubOperationTimeoutMs
        + ", topicDeletionStatusPollIntervalMs=" + topicDeletionStatusPollIntervalMs + ", topicMinLogCompactionLagMs="
        + topicMinLogCompactionLagMs + ", topicOffsetCheckIntervalMs=" + topicOffsetCheckIntervalMs
        + ", topicMetadataFetcherConsumerPoolSize=" + topicMetadataFetcherConsumerPoolSize
        + ", topicMetadataFetcherThreadPoolSize=" + topicMetadataFetcherThreadPoolSize + ", pubSubAdminAdapterFactory="
        + pubSubAdminAdapterFactory.getClass().getSimpleName() + ", pubSubConsumerAdapterFactory="
        + pubSubConsumerAdapterFactory.getClass().getSimpleName() + '}';
  }

  public static class Builder {
    private PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> pubSubAdminAdapterFactory;
    private PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> pubSubConsumerAdapterFactory;
    private PubSubTopicRepository pubSubTopicRepository;
    private MetricsRepository metricsRepository;
    private VeniceProperties veniceProperties;
    private Function<String, String> pubSubBrokerUrlResolver;
    private Function<String, PubSubSecurityProtocol> pubSubSecurityProtocolResolver;
    private long pubSubOperationTimeoutMs = PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
    private long topicDeletionStatusPollIntervalMs = PUBSUB_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS_DEFAULT_VALUE;
    private long topicMinLogCompactionLagMs = DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS;
    private long topicOffsetCheckIntervalMs = 60_000L; // 1 minute
    private int topicMetadataFetcherConsumerPoolSize = 1;
    private int topicMetadataFetcherThreadPoolSize = 2;

    public Builder setPubSubOperationTimeoutMs(long pubSubOperationTimeoutMs) {
      this.pubSubOperationTimeoutMs = pubSubOperationTimeoutMs;
      return this;
    }

    public Builder setTopicDeletionStatusPollIntervalMs(long topicDeletionStatusPollIntervalMs) {
      this.topicDeletionStatusPollIntervalMs = topicDeletionStatusPollIntervalMs;
      return this;
    }

    public Builder setTopicMinLogCompactionLagMs(long topicMinLogCompactionLagMs) {
      this.topicMinLogCompactionLagMs = topicMinLogCompactionLagMs;
      return this;
    }

    public Builder setPubSubAdminAdapterFactory(
        PubSubAdminAdapterFactory<? extends PubSubAdminAdapter> pubSubAdminAdapterFactory) {
      this.pubSubAdminAdapterFactory = pubSubAdminAdapterFactory;
      return this;
    }

    public Builder setPubSubConsumerAdapterFactory(
        PubSubConsumerAdapterFactory<? extends PubSubConsumerAdapter> pubSubConsumerAdapterFactory) {
      this.pubSubConsumerAdapterFactory = pubSubConsumerAdapterFactory;
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

    public Builder setTopicOffsetCheckIntervalMs(long topicOffsetCheckIntervalMs) {
      this.topicOffsetCheckIntervalMs = topicOffsetCheckIntervalMs;
      return this;
    }

    public Builder setTopicMetadataFetcherConsumerPoolSize(int topicMetadataFetcherConsumerPoolSize) {
      this.topicMetadataFetcherConsumerPoolSize = topicMetadataFetcherConsumerPoolSize;
      return this;
    }

    public Builder setTopicMetadataFetcherThreadPoolSize(int topicMetadataFetcherThreadPoolSize) {
      this.topicMetadataFetcherThreadPoolSize = topicMetadataFetcherThreadPoolSize;
      return this;
    }

    public Builder setVeniceProperties(VeniceProperties veniceProperties) {
      this.veniceProperties = veniceProperties;
      return this;
    }

    public Builder setPubSubBrokerUrlResolver(Function<String, String> pubSubBrokerUrlResolver) {
      this.pubSubBrokerUrlResolver = pubSubBrokerUrlResolver;
      return this;
    }

    public Builder setPubSubSecurityProtocolResolver(
        Function<String, PubSubSecurityProtocol> pubSubSecurityProtocolResolver) {
      this.pubSubSecurityProtocolResolver = pubSubSecurityProtocolResolver;
      return this;
    }

    public void verify() {
      if (pubSubAdminAdapterFactory == null) {
        throw new IllegalArgumentException("pubSubAdminAdapterFactory cannot be null");
      }

      if (pubSubConsumerAdapterFactory == null) {
        throw new IllegalArgumentException("pubSubConsumerAdapterFactory cannot be null");
      }

      if (pubSubTopicRepository == null) {
        throw new IllegalArgumentException("pubSubTopicRepository cannot be null");
      }

      if (veniceProperties == null) {
        throw new IllegalArgumentException("veniceProperties cannot be null");
      }

      if (pubSubOperationTimeoutMs <= 0) {
        throw new IllegalArgumentException("pubSubOperationTimeoutMs must be positive");
      }

      if (topicDeletionStatusPollIntervalMs < 0) {
        throw new IllegalArgumentException("topicDeletionStatusPollIntervalMs must be positive");
      }

      if (topicOffsetCheckIntervalMs < 0) {
        throw new IllegalArgumentException("topicOffsetCheckIntervalMs must be positive");
      }

      if (topicMetadataFetcherConsumerPoolSize <= 0) {
        throw new IllegalArgumentException("topicMetadataFetcherConsumerPoolSize must be positive");
      }

      if (topicMetadataFetcherThreadPoolSize <= 0) {
        throw new IllegalArgumentException("topicMetadataFetcherThreadPoolSize must be positive");
      }

      if (pubSubBrokerUrlResolver == null) {
        pubSubBrokerUrlResolver = Function.identity();
      }

      if (pubSubSecurityProtocolResolver == null) {
        pubSubSecurityProtocolResolver =
            inputBrokerAddress -> PubSubUtil.getPubSubSecurityProtocolOrDefault(veniceProperties);
      }
    }

    public TopicManagerContext build() {
      verify();
      return new TopicManagerContext(this);
    }
  }
}
