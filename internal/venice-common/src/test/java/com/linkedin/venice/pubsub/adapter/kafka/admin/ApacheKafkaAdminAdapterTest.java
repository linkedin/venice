package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubInvalidReplicationFactorException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ApacheKafkaAdminAdapterTest {
  private AdminClient internalKafkaAdminClientMock;
  private PubSubTopicRepository pubSubTopicRepository;
  private PubSubTopic testPubSubTopic;
  private ApacheKafkaAdminAdapter kafkaAdminAdapter;
  private PubSubTopicConfiguration sampleTopicConfiguration;
  private Config sampleConfig;
  private ApacheKafkaAdminConfig apacheKafkaAdminConfig;

  private static final int NUM_PARTITIONS = 3;
  private static final int REPLICATION_FACTOR = 4;

  @BeforeMethod
  public void setUp() {
    internalKafkaAdminClientMock = mock(AdminClient.class);
    pubSubTopicRepository = new PubSubTopicRepository();
    testPubSubTopic = pubSubTopicRepository.getTopic("test-topic");
    apacheKafkaAdminConfig = mock(ApacheKafkaAdminConfig.class);
    kafkaAdminAdapter =
        new ApacheKafkaAdminAdapter(internalKafkaAdminClientMock, apacheKafkaAdminConfig, pubSubTopicRepository);
    sampleTopicConfiguration = new PubSubTopicConfiguration(
        Optional.of(Duration.ofDays(3).toMillis()),
        true,
        Optional.of(2),
        Duration.ofDays(1).toMillis());
    sampleConfig = new Config(
        Arrays.asList(
            new ConfigEntry("retention.ms", "259200000"),
            new ConfigEntry("cleanup.policy", "compact"),
            new ConfigEntry("min.insync.replicas", "2"),
            new ConfigEntry("min.compaction.lag.ms", "86400000")));
  }

  @Test
  public void testCreateTopicValidTopicCreation() throws Exception {
    CreateTopicsResult createTopicsResultMock = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createTopicsKafkaFutureMock = mock(KafkaFuture.class);
    when(internalKafkaAdminClientMock.createTopics(any())).thenReturn(createTopicsResultMock);
    when(createTopicsResultMock.all()).thenReturn(createTopicsKafkaFutureMock);
    when(createTopicsResultMock.numPartitions(testPubSubTopic.getName()))
        .thenReturn(KafkaFuture.completedFuture(NUM_PARTITIONS));
    when(createTopicsResultMock.replicationFactor(testPubSubTopic.getName()))
        .thenReturn(KafkaFuture.completedFuture(REPLICATION_FACTOR));
    when(createTopicsResultMock.config(testPubSubTopic.getName()))
        .thenReturn(KafkaFuture.completedFuture(sampleConfig));
    when(createTopicsKafkaFutureMock.get()).thenReturn(null);

    kafkaAdminAdapter.createTopic(testPubSubTopic, NUM_PARTITIONS, REPLICATION_FACTOR, sampleTopicConfiguration);

    verify(internalKafkaAdminClientMock).createTopics(any());
    verify(createTopicsResultMock).all();
    verify(createTopicsKafkaFutureMock).get();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateTopicInvalidReplicationFactor() {
    kafkaAdminAdapter.createTopic(testPubSubTopic, NUM_PARTITIONS, Short.MAX_VALUE + 1, sampleTopicConfiguration);
  }

  @Test(expectedExceptions = PubSubClientException.class, expectedExceptionsMessageRegExp = ".* created with incorrect num of partitions.*")
  public void testCreateTopicInvalidNumPartition() throws ExecutionException, InterruptedException {
    CreateTopicsResult createTopicsResultMock = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createTopicsKafkaFutureMock = mock(KafkaFuture.class);
    when(internalKafkaAdminClientMock.createTopics(any())).thenReturn(createTopicsResultMock);
    when(createTopicsResultMock.all()).thenReturn(createTopicsKafkaFutureMock);
    when(createTopicsResultMock.numPartitions(testPubSubTopic.getName())).thenReturn(KafkaFuture.completedFuture(11));
    when(createTopicsResultMock.replicationFactor(testPubSubTopic.getName()))
        .thenReturn(KafkaFuture.completedFuture(REPLICATION_FACTOR));
    when(createTopicsResultMock.config(testPubSubTopic.getName()))
        .thenReturn(KafkaFuture.completedFuture(sampleConfig));
    when(createTopicsKafkaFutureMock.get()).thenReturn(null);

    kafkaAdminAdapter.createTopic(testPubSubTopic, 12, REPLICATION_FACTOR, sampleTopicConfiguration);
  }

  @Test
  public void testCreateTopicThrowsException() throws Exception {
    CreateTopicsResult createTopicsResultMock = mock(CreateTopicsResult.class);
    KafkaFuture<Void> createTopicsKafkaFutureMock = mock(KafkaFuture.class);
    when(internalKafkaAdminClientMock.createTopics(any())).thenReturn(createTopicsResultMock);
    when(createTopicsResultMock.all()).thenReturn(createTopicsKafkaFutureMock);

    when(createTopicsKafkaFutureMock.get())
        .thenThrow(new ExecutionException(new InvalidReplicationFactorException("Invalid replication factor")))
        .thenThrow(new ExecutionException(new TopicExistsException("Topic exists")))
        .thenThrow(new ExecutionException(new NetworkException("Retryable network exception")))
        .thenThrow(new ExecutionException(new UnknownServerException("Unknown server exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    // First call throws InvalidReplicationFactorException
    assertThrows(
        PubSubInvalidReplicationFactorException.class,
        () -> kafkaAdminAdapter
            .createTopic(testPubSubTopic, NUM_PARTITIONS, REPLICATION_FACTOR, sampleTopicConfiguration));
    verify(internalKafkaAdminClientMock).createTopics(any());

    // Second call throws TopicExistsException
    assertThrows(
        PubSubTopicExistsException.class,
        () -> kafkaAdminAdapter
            .createTopic(testPubSubTopic, NUM_PARTITIONS, REPLICATION_FACTOR, sampleTopicConfiguration));

    // Third call throws NetworkException
    assertThrows(
        PubSubClientRetriableException.class,
        () -> kafkaAdminAdapter
            .createTopic(testPubSubTopic, NUM_PARTITIONS, REPLICATION_FACTOR, sampleTopicConfiguration));

    // Fourth call throws UnknownServerException
    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter
            .createTopic(testPubSubTopic, NUM_PARTITIONS, REPLICATION_FACTOR, sampleTopicConfiguration));

    // Fifth call throws InterruptedException
    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter
            .createTopic(testPubSubTopic, NUM_PARTITIONS, REPLICATION_FACTOR, sampleTopicConfiguration));
    assertTrue(Thread.currentThread().isInterrupted());

    // Verify that createTopics() and get() are called 5 times
    verify(internalKafkaAdminClientMock, times(5)).createTopics(any());
    verify(createTopicsKafkaFutureMock, times(5)).get();
  }

  @Test
  public void testDeleteTopicValidTopicDeletion() throws Exception {
    DeleteTopicsResult deleteTopicsResultMock = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> topicDeletionFutureMock = mock(KafkaFuture.class);

    when(internalKafkaAdminClientMock.deleteTopics(any())).thenReturn(deleteTopicsResultMock);
    when(deleteTopicsResultMock.all()).thenReturn(topicDeletionFutureMock);
    when(topicDeletionFutureMock.get(eq(1000L), eq(TimeUnit.MILLISECONDS))).thenReturn(null);

    kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L));

    verify(internalKafkaAdminClientMock).deleteTopics(eq(Collections.singleton(testPubSubTopic.getName())));
    verify(deleteTopicsResultMock).all();
    verify(topicDeletionFutureMock).get(eq(1000L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testDeleteTopicThrowsException() throws Exception {
    DeleteTopicsResult deleteTopicsResultMock = mock(DeleteTopicsResult.class);
    KafkaFuture<Void> topicDeletionFutureMock = mock(KafkaFuture.class);

    when(internalKafkaAdminClientMock.deleteTopics(any())).thenReturn(deleteTopicsResultMock);
    when(deleteTopicsResultMock.all()).thenReturn(topicDeletionFutureMock);

    when(topicDeletionFutureMock.get(eq(1000L), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("Unknown topic or partition")))
        .thenThrow(new ExecutionException(new TimeoutException("Timeout exception")))
        .thenThrow(new ExecutionException(new UnknownServerException("Unknown server exception")))
        .thenThrow(new java.util.concurrent.TimeoutException("Timeout exception"))
        .thenThrow(new InterruptedException("Interrupted exception"));

    // First call throws UnknownServerException
    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));
    verify(internalKafkaAdminClientMock).deleteTopics(eq(Collections.singleton(testPubSubTopic.getName())));

    // Second call throws TimeoutException
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));

    // Third call throws UnknownServerException
    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));

    // Fourth call throws TimeoutException
    assertThrows(
        PubSubOpTimeoutException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));

    // Fifth call throws InterruptedException
    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));
    assertTrue(Thread.currentThread().isInterrupted());

    // Verify that deleteTopics() and get() are called 5 times
    verify(internalKafkaAdminClientMock, times(5)).deleteTopics(eq(Collections.singleton(testPubSubTopic.getName())));
    verify(deleteTopicsResultMock, times(5)).all();
    verify(topicDeletionFutureMock, times(5)).get(eq(1000L), eq(TimeUnit.MILLISECONDS));
  }
}
