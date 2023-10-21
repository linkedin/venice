package com.linkedin.venice.pubsub.manager;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import io.tehuti.metrics.MetricsRepository;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TopicManagerContextTest {
  private PubSubAdminAdapterFactory<PubSubAdminAdapter> pubSubAdminAdapterFactory;
  private PubSubConsumerAdapterFactory<PubSubConsumerAdapter> pubSubConsumerAdapterFactory;
  private PubSubTopicRepository pubSubTopicRepository;
  private MetricsRepository metricsRepository;
  private TopicManagerContext.PubSubPropertiesSupplier pubSubPropertiesSupplier;
  private long pubSubOperationTimeoutMs;
  private long topicDeletionStatusPollIntervalMs;
  private long topicMinLogCompactionLagMs;
  private long topicOffsetCheckIntervalMs;
  private int topicMetadataFetcherConsumerPoolSize;
  private int topicMetadataFetcherThreadPoolSize;

  @BeforeMethod
  public void setUp() {
    pubSubAdminAdapterFactory = mock(PubSubAdminAdapterFactory.class);
    pubSubConsumerAdapterFactory = mock(PubSubConsumerAdapterFactory.class);
    pubSubTopicRepository = new PubSubTopicRepository();
    metricsRepository = new MetricsRepository();
    pubSubPropertiesSupplier = mock(TopicManagerContext.PubSubPropertiesSupplier.class);
    pubSubOperationTimeoutMs = 11;
    topicDeletionStatusPollIntervalMs = 12;
    topicMinLogCompactionLagMs = 12;
    topicOffsetCheckIntervalMs = 14;
    topicMetadataFetcherConsumerPoolSize = 15;
    topicMetadataFetcherThreadPoolSize = 16;
  }

  @Test
  public void testTopicManagerContext() {
    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubAdminAdapterFactory(pubSubAdminAdapterFactory)
            .setPubSubConsumerAdapterFactory(pubSubConsumerAdapterFactory)
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setMetricsRepository(metricsRepository)
            .setPubSubPropertiesSupplier(pubSubPropertiesSupplier)
            .setPubSubOperationTimeoutMs(pubSubOperationTimeoutMs)
            .setTopicDeletionStatusPollIntervalMs(topicDeletionStatusPollIntervalMs)
            .setTopicMinLogCompactionLagMs(topicMinLogCompactionLagMs)
            .setTopicOffsetCheckIntervalMs(topicOffsetCheckIntervalMs)
            .setTopicMetadataFetcherConsumerPoolSize(topicMetadataFetcherConsumerPoolSize)
            .setTopicMetadataFetcherThreadPoolSize(topicMetadataFetcherThreadPoolSize)
            .build();

    assertNotNull(topicManagerContext);
    assertEquals(topicManagerContext.getPubSubAdminAdapterFactory(), pubSubAdminAdapterFactory);
    assertEquals(topicManagerContext.getPubSubConsumerAdapterFactory(), pubSubConsumerAdapterFactory);
    assertEquals(topicManagerContext.getPubSubTopicRepository(), pubSubTopicRepository);
    assertEquals(topicManagerContext.getMetricsRepository(), metricsRepository);
    assertEquals(topicManagerContext.getPubSubPropertiesSupplier(), pubSubPropertiesSupplier);
    assertEquals(topicManagerContext.getPubSubOperationTimeoutMs(), pubSubOperationTimeoutMs);
    assertEquals(topicManagerContext.getTopicDeletionStatusPollIntervalMs(), topicDeletionStatusPollIntervalMs);
    assertEquals(topicManagerContext.getTopicMinLogCompactionLagMs(), topicMinLogCompactionLagMs);
    assertEquals(topicManagerContext.getTopicOffsetCheckIntervalMs(), topicOffsetCheckIntervalMs);
    assertEquals(topicManagerContext.getTopicMetadataFetcherConsumerPoolSize(), topicMetadataFetcherConsumerPoolSize);
    assertEquals(topicManagerContext.getTopicMetadataFetcherThreadPoolSize(), topicMetadataFetcherThreadPoolSize);
  }

  // test invalid arguments
  @Test
  public void testTopicManagerContextInvalidArguments() {
    TopicManagerContext.Builder builder =
        new TopicManagerContext.Builder().setPubSubAdminAdapterFactory(pubSubAdminAdapterFactory)
            .setPubSubConsumerAdapterFactory(pubSubConsumerAdapterFactory)
            .setPubSubTopicRepository(pubSubTopicRepository)
            .setMetricsRepository(metricsRepository)
            .setPubSubPropertiesSupplier(pubSubPropertiesSupplier)
            .setPubSubOperationTimeoutMs(pubSubOperationTimeoutMs)
            .setTopicDeletionStatusPollIntervalMs(topicDeletionStatusPollIntervalMs)
            .setTopicMinLogCompactionLagMs(topicMinLogCompactionLagMs)
            .setTopicOffsetCheckIntervalMs(topicOffsetCheckIntervalMs)
            .setTopicMetadataFetcherConsumerPoolSize(topicMetadataFetcherConsumerPoolSize)
            .setTopicMetadataFetcherThreadPoolSize(topicMetadataFetcherThreadPoolSize);

    // set admin adapter factory to null
    builder.setPubSubAdminAdapterFactory(null);
    Throwable ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setPubSubAdminAdapterFactory(pubSubAdminAdapterFactory);
    assertEquals(ex.getMessage(), "pubSubAdminAdapterFactory cannot be null");

    // set consumer adapter factory to null
    builder.setPubSubConsumerAdapterFactory(null);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setPubSubConsumerAdapterFactory(pubSubConsumerAdapterFactory);
    assertEquals(ex.getMessage(), "pubSubConsumerAdapterFactory cannot be null");

    // set topic repository to null
    builder.setPubSubTopicRepository(null);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setPubSubTopicRepository(pubSubTopicRepository);
    assertEquals(ex.getMessage(), "pubSubTopicRepository cannot be null");

    // set pub sub properties supplier to null
    builder.setPubSubPropertiesSupplier(null);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setPubSubPropertiesSupplier(pubSubPropertiesSupplier);
    assertEquals(ex.getMessage(), "pubSubPropertiesSupplier cannot be null");

    // set pub sub operation timeout to -1
    builder.setPubSubOperationTimeoutMs(-1);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setPubSubOperationTimeoutMs(pubSubOperationTimeoutMs);
    assertEquals(ex.getMessage(), "pubSubOperationTimeoutMs must be positive");

    // set topic deletion status poll interval to -1
    builder.setTopicDeletionStatusPollIntervalMs(-1);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setTopicDeletionStatusPollIntervalMs(topicDeletionStatusPollIntervalMs);
    assertEquals(ex.getMessage(), "topicDeletionStatusPollIntervalMs must be positive");

    // set topic offset check interval to -1
    builder.setTopicOffsetCheckIntervalMs(-1);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setTopicOffsetCheckIntervalMs(topicOffsetCheckIntervalMs);
    assertEquals(ex.getMessage(), "topicOffsetCheckIntervalMs must be positive");

    // set topic metadata fetcher consumer pool size to 0
    builder.setTopicMetadataFetcherConsumerPoolSize(0);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setTopicMetadataFetcherConsumerPoolSize(topicMetadataFetcherConsumerPoolSize);
    assertEquals(ex.getMessage(), "topicMetadataFetcherConsumerPoolSize must be positive");

    // set topic metadata fetcher thread pool size to 0
    builder.setTopicMetadataFetcherThreadPoolSize(0);
    ex = expectThrows(IllegalArgumentException.class, builder::build);
    builder.setTopicMetadataFetcherThreadPoolSize(topicMetadataFetcherThreadPoolSize);
    assertEquals(ex.getMessage(), "topicMetadataFetcherThreadPoolSize must be positive");
  }
}
