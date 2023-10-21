package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
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
        Duration.ofDays(1).toMillis(),
        Optional.empty());
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
        PubSubClientRetriableException.class,
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
        .thenThrow(new ExecutionException(new NetworkException("Retryable network exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));
    verify(internalKafkaAdminClientMock).deleteTopics(eq(Collections.singleton(testPubSubTopic.getName())));

    assertThrows(
        PubSubOpTimeoutException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));

    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));

    assertThrows(
        PubSubOpTimeoutException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));

    assertThrows(
        PubSubClientRetriableException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));

    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter.deleteTopic(testPubSubTopic, Duration.ofMillis(1000L)));
    assertTrue(Thread.currentThread().isInterrupted());

    // Verify that deleteTopics() and get() are called 5 times
    verify(internalKafkaAdminClientMock, times(6)).deleteTopics(eq(Collections.singleton(testPubSubTopic.getName())));
    verify(deleteTopicsResultMock, times(6)).all();
    verify(topicDeletionFutureMock, times(6)).get(eq(1000L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testGetTopicConfig() throws Exception {
    DescribeConfigsResult describeConfigsResultMock = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsKafkaFutureMock = mock(KafkaFuture.class);

    List<ConfigEntry> configEntries = Arrays.asList(
        new ConfigEntry("cleanup.policy", "compact"),
        new ConfigEntry("retention.ms", "1111"),
        new ConfigEntry("min.compaction.lag.ms", "2222"),
        new ConfigEntry("min.insync.replicas", "3333"));
    Config config = new Config(configEntries);
    Map<ConfigResource, Config> configMap = new HashMap<>();
    configMap.put(new ConfigResource(ConfigResource.Type.TOPIC, testPubSubTopic.getName()), config);

    when(internalKafkaAdminClientMock.describeConfigs(any())).thenReturn(describeConfigsResultMock);
    when(describeConfigsResultMock.all()).thenReturn(describeConfigsKafkaFutureMock);
    when(describeConfigsKafkaFutureMock.get()).thenReturn(configMap);

    PubSubTopicConfiguration topicConfiguration = kafkaAdminAdapter.getTopicConfig(testPubSubTopic);

    assertTrue(topicConfiguration.isLogCompacted());

    assertTrue(topicConfiguration.retentionInMs().isPresent());
    assertEquals(topicConfiguration.retentionInMs().get(), Long.valueOf(1111));

    assertEquals(topicConfiguration.minLogCompactionLagMs(), Long.valueOf(2222));

    assertTrue(topicConfiguration.minInSyncReplicas().isPresent());
    assertEquals(topicConfiguration.minInSyncReplicas().get(), Integer.valueOf(3333));

    verify(internalKafkaAdminClientMock).describeConfigs(any());
    verify(describeConfigsResultMock).all();
    verify(describeConfigsKafkaFutureMock).get();
  }

  @Test
  public void testGetTopicThrowsException() throws Exception {
    DescribeConfigsResult describeConfigsResultMock = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsKafkaFutureMock = mock(KafkaFuture.class);

    when(internalKafkaAdminClientMock.describeConfigs(any())).thenReturn(describeConfigsResultMock);
    when(describeConfigsResultMock.all()).thenReturn(describeConfigsKafkaFutureMock);

    when(describeConfigsKafkaFutureMock.get())
        .thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("Unknown topic or partition")))
        .thenThrow(new ExecutionException(new NetworkException("Retryable network exception")))
        .thenThrow(new ExecutionException(new UnknownServerException("Unknown server exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    assertThrows(PubSubTopicDoesNotExistException.class, () -> kafkaAdminAdapter.getTopicConfig(testPubSubTopic));

    assertThrows(PubSubClientRetriableException.class, () -> kafkaAdminAdapter.getTopicConfig(testPubSubTopic));

    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.getTopicConfig(testPubSubTopic));

    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.getTopicConfig(testPubSubTopic));
    assertTrue(Thread.currentThread().isInterrupted());

    verify(internalKafkaAdminClientMock, times(4)).describeConfigs(any());
    verify(describeConfigsResultMock, times(4)).all();
    verify(describeConfigsKafkaFutureMock, times(4)).get();
  }

  @Test
  public void testListAllTopics() throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResultMock = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsKafkaFutureMock = mock(KafkaFuture.class);
    Set<String> sampleTopics = new HashSet<>(Arrays.asList("t1_v1", "t2_rt", "t3_v2"));

    when(internalKafkaAdminClientMock.listTopics()).thenReturn(listTopicsResultMock);
    when(listTopicsResultMock.names()).thenReturn(listTopicsKafkaFutureMock);
    when(listTopicsKafkaFutureMock.get()).thenReturn(sampleTopics);

    Set<PubSubTopic> topics = kafkaAdminAdapter.listAllTopics();

    assertEquals(topics.size(), 3);
    assertTrue(topics.contains(pubSubTopicRepository.getTopic("t1_v1")));
    assertTrue(topics.contains(pubSubTopicRepository.getTopic("t2_rt")));
    assertTrue(topics.contains(pubSubTopicRepository.getTopic("t3_v2")));

    verify(internalKafkaAdminClientMock).listTopics();
    verify(listTopicsResultMock).names();
    verify(listTopicsKafkaFutureMock).get();
  }

  @Test
  public void testListAllTopicsThrowsException() throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResultMock = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsKafkaFutureMock = mock(KafkaFuture.class);

    when(internalKafkaAdminClientMock.listTopics()).thenReturn(listTopicsResultMock);
    when(listTopicsResultMock.names()).thenReturn(listTopicsKafkaFutureMock);

    when(listTopicsKafkaFutureMock.get())
        .thenThrow(new ExecutionException(new NetworkException("Retryable network exception")))
        .thenThrow(new ExecutionException(new UnknownServerException("Non-retryable exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    assertThrows(PubSubClientRetriableException.class, () -> kafkaAdminAdapter.listAllTopics());
    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.listAllTopics());
    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.listAllTopics());
    assertTrue(Thread.currentThread().isInterrupted());

    verify(internalKafkaAdminClientMock, times(3)).listTopics();
    verify(listTopicsResultMock, times(3)).names();
    verify(listTopicsKafkaFutureMock, times(3)).get();
  }

  @Test
  public void testSetTopicConfig() {
    PubSubTopicConfiguration topicConfiguration =
        new PubSubTopicConfiguration(Optional.of(1111L), true, Optional.of(222), 333L, Optional.empty());
    AlterConfigsResult alterConfigsResultMock = mock(AlterConfigsResult.class);
    KafkaFuture<Void> alterConfigsKafkaFutureMock = mock(KafkaFuture.class);

    Map<ConfigResource, Config> resourceConfigMap = new HashMap<>();
    when(internalKafkaAdminClientMock.alterConfigs(any())).thenAnswer(invocation -> {
      resourceConfigMap.putAll(invocation.getArgument(0));
      return alterConfigsResultMock;
    });
    when(alterConfigsResultMock.all()).thenReturn(alterConfigsKafkaFutureMock);

    kafkaAdminAdapter.setTopicConfig(testPubSubTopic, topicConfiguration);

    assertEquals(resourceConfigMap.size(), 1);
    for (Map.Entry<ConfigResource, Config> entry: resourceConfigMap.entrySet()) {
      assertEquals(entry.getKey().type(), ConfigResource.Type.TOPIC);
      assertEquals(entry.getKey().name(), testPubSubTopic.getName());

      Config config = entry.getValue();
      assertEquals(config.get(TopicConfig.RETENTION_MS_CONFIG).value(), "1111");
      assertEquals(config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value(), TopicConfig.CLEANUP_POLICY_COMPACT);
      assertEquals(config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value(), "222");
      assertEquals(config.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG).value(), "333");
    }
  }

  @Test
  public void testSetTopicConfigThrowsException() throws ExecutionException, InterruptedException {
    PubSubTopicConfiguration topicConfiguration =
        new PubSubTopicConfiguration(Optional.of(1111L), true, Optional.of(222), 333L, Optional.empty());
    AlterConfigsResult alterConfigsResultMock = mock(AlterConfigsResult.class);
    KafkaFuture<Void> alterConfigsKafkaFutureMock = mock(KafkaFuture.class);

    when(internalKafkaAdminClientMock.alterConfigs(any())).thenReturn(alterConfigsResultMock);
    when(alterConfigsResultMock.all()).thenReturn(alterConfigsKafkaFutureMock);

    when(alterConfigsKafkaFutureMock.get())
        .thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("Unknown topic or partition")))
        .thenThrow(new ExecutionException(new UnknownServerException("Unknown server exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    ApacheKafkaAdminAdapter apacheKafkaAdminAdapterSpy = spy(kafkaAdminAdapter);
    doReturn(false, true, true).when(apacheKafkaAdminAdapterSpy)
        .containsTopicWithExpectationAndRetry(testPubSubTopic, 3, true);

    assertThrows(
        PubSubTopicDoesNotExistException.class,
        () -> apacheKafkaAdminAdapterSpy.setTopicConfig(testPubSubTopic, topicConfiguration));

    assertThrows(
        PubSubClientException.class,
        () -> apacheKafkaAdminAdapterSpy.setTopicConfig(testPubSubTopic, topicConfiguration));

    assertThrows(
        PubSubClientException.class,
        () -> apacheKafkaAdminAdapterSpy.setTopicConfig(testPubSubTopic, topicConfiguration));
    assertTrue(Thread.currentThread().isInterrupted());

    verify(internalKafkaAdminClientMock, times(3)).alterConfigs(any());
    verify(alterConfigsResultMock, times(3)).all();
    verify(alterConfigsKafkaFutureMock, times(3)).get();
  }

  @Test
  public void testContainsTopic() throws ExecutionException, InterruptedException {
    DescribeTopicsResult describeTopicsResultMock = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> describeTopicsKafkaFutureMock = mock(KafkaFuture.class);
    Map<String, KafkaFuture<TopicDescription>> futureHashMap = new HashMap<>();
    futureHashMap.put(testPubSubTopic.getName(), describeTopicsKafkaFutureMock);

    when(internalKafkaAdminClientMock.describeTopics(any())).thenReturn(describeTopicsResultMock);
    when(describeTopicsResultMock.values()).thenReturn(futureHashMap);
    when(describeTopicsKafkaFutureMock.get())
        .thenReturn(new TopicDescription(testPubSubTopic.getName(), false, new ArrayList<>()))
        .thenReturn(null);

    assertTrue(kafkaAdminAdapter.containsTopic(testPubSubTopic));
    assertFalse(kafkaAdminAdapter.containsTopic(testPubSubTopic));

    verify(internalKafkaAdminClientMock, times(2)).describeTopics(any());
    verify(describeTopicsResultMock, times(2)).values();
    verify(describeTopicsKafkaFutureMock, times(2)).get();
  }

  @Test
  public void testContainsTopicThrowsException() throws ExecutionException, InterruptedException {
    DescribeTopicsResult describeTopicsResultMock = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> describeTopicsKafkaFutureMock = mock(KafkaFuture.class);
    Map<String, KafkaFuture<TopicDescription>> futureHashMap = new HashMap<>();
    futureHashMap.put(testPubSubTopic.getName(), describeTopicsKafkaFutureMock);

    when(internalKafkaAdminClientMock.describeTopics(any())).thenReturn(describeTopicsResultMock);
    when(describeTopicsResultMock.values()).thenReturn(futureHashMap);
    when(describeTopicsKafkaFutureMock.get())
        .thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("Unknown topic or partition")))
        .thenThrow(new ExecutionException(new NetworkException("Retriable exception")))
        .thenThrow(new ExecutionException(new UnknownServerException("Unknown server exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    assertFalse(kafkaAdminAdapter.containsTopic(testPubSubTopic));
    assertThrows(PubSubClientRetriableException.class, () -> kafkaAdminAdapter.containsTopic(testPubSubTopic));
    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.containsTopic(testPubSubTopic));
    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.containsTopic(testPubSubTopic));
    assertTrue(Thread.currentThread().isInterrupted());

    verify(internalKafkaAdminClientMock, times(4)).describeTopics(any());
    verify(describeTopicsResultMock, times(4)).values();
    verify(describeTopicsKafkaFutureMock, times(4)).get();
  }

  @Test
  public void testContainsTopicWithPartitionCheck() throws ExecutionException, InterruptedException {
    DescribeTopicsResult describeTopicsResultMock = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> describeTopicsKafkaFutureMock = mock(KafkaFuture.class);
    Map<String, KafkaFuture<TopicDescription>> futureHashMap = new HashMap<>();
    futureHashMap.put(testPubSubTopic.getName(), describeTopicsKafkaFutureMock);

    List<TopicPartitionInfo> partitions =
        Arrays.asList(mock(TopicPartitionInfo.class), mock(TopicPartitionInfo.class), mock(TopicPartitionInfo.class));

    when(internalKafkaAdminClientMock.describeTopics(any())).thenReturn(describeTopicsResultMock);
    when(describeTopicsResultMock.values()).thenReturn(futureHashMap);
    when(describeTopicsKafkaFutureMock.get()).thenReturn(null) // first call to containsTopicWithPartitionCheck should
                                                               // return null
        .thenReturn(new TopicDescription(testPubSubTopic.getName(), false, partitions));

    assertFalse(kafkaAdminAdapter.containsTopicWithPartitionCheck(new PubSubTopicPartitionImpl(testPubSubTopic, 0)));
    assertTrue(kafkaAdminAdapter.containsTopicWithPartitionCheck(new PubSubTopicPartitionImpl(testPubSubTopic, 0)));
    assertTrue(kafkaAdminAdapter.containsTopicWithPartitionCheck(new PubSubTopicPartitionImpl(testPubSubTopic, 1)));
    assertTrue(kafkaAdminAdapter.containsTopicWithPartitionCheck(new PubSubTopicPartitionImpl(testPubSubTopic, 2)));
    assertFalse(kafkaAdminAdapter.containsTopicWithPartitionCheck(new PubSubTopicPartitionImpl(testPubSubTopic, 3)));
  }

  @Test
  public void testContainsTopicWithPartitionCheckThrowsException() throws ExecutionException, InterruptedException {
    DescribeTopicsResult describeTopicsResultMock = mock(DescribeTopicsResult.class);
    KafkaFuture<TopicDescription> describeTopicsKafkaFutureMock = mock(KafkaFuture.class);
    Map<String, KafkaFuture<TopicDescription>> futureHashMap = new HashMap<>();
    futureHashMap.put(testPubSubTopic.getName(), describeTopicsKafkaFutureMock);

    when(internalKafkaAdminClientMock.describeTopics(any())).thenReturn(describeTopicsResultMock);
    when(describeTopicsResultMock.values()).thenReturn(futureHashMap);
    when(describeTopicsKafkaFutureMock.get())
        .thenThrow(new ExecutionException(new UnknownTopicOrPartitionException("Unknown topic or partition")))
        .thenThrow(new ExecutionException(new NetworkException("Retriable exception")))
        .thenThrow(new ExecutionException(new UnknownServerException("Unknown server exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    PubSubTopicPartition testPubSubTopicPartition = new PubSubTopicPartitionImpl(testPubSubTopic, 0);
    assertFalse(kafkaAdminAdapter.containsTopicWithPartitionCheck(testPubSubTopicPartition));
    assertThrows(
        PubSubClientRetriableException.class,
        () -> kafkaAdminAdapter.containsTopicWithPartitionCheck(testPubSubTopicPartition));
    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter.containsTopicWithPartitionCheck(testPubSubTopicPartition));
    assertThrows(
        PubSubClientException.class,
        () -> kafkaAdminAdapter.containsTopicWithPartitionCheck(testPubSubTopicPartition));
    assertTrue(Thread.currentThread().isInterrupted());

    verify(internalKafkaAdminClientMock, times(4)).describeTopics(any());
    verify(describeTopicsResultMock, times(4)).values();
    verify(describeTopicsKafkaFutureMock, times(4)).get();
  }

  @Test
  public void testGetAllTopicRetentions() throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResultMock = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsKafkaFutureMock = mock(KafkaFuture.class);
    Set<String> sampleTopics = new HashSet<>(Arrays.asList("t1_v1", "t2_rt", "t3_v2", "t4_rt"));

    when(internalKafkaAdminClientMock.listTopics()).thenReturn(listTopicsResultMock);
    when(listTopicsResultMock.names()).thenReturn(listTopicsKafkaFutureMock);
    when(listTopicsKafkaFutureMock.get()).thenReturn(sampleTopics);

    DescribeConfigsResult describeConfigsResultMock = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsKafkaFutureMock = mock(KafkaFuture.class);
    Map<ConfigResource, Config> resourceConfigMap = new HashMap<>();
    resourceConfigMap.put(
        new ConfigResource(ConfigResource.Type.TOPIC, "t1_v1"),
        new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "1"))));
    resourceConfigMap.put(
        new ConfigResource(ConfigResource.Type.TOPIC, "t2_rt"),
        new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "2"))));
    resourceConfigMap.put(
        new ConfigResource(ConfigResource.Type.TOPIC, "t3_v2"),
        new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "3"))));
    resourceConfigMap.put(new ConfigResource(ConfigResource.Type.TOPIC, "t4_rt"), new Config(Collections.emptyList()));

    when(internalKafkaAdminClientMock.describeConfigs(any())).thenReturn(describeConfigsResultMock);
    when(describeConfigsResultMock.all()).thenReturn(describeConfigsKafkaFutureMock);
    when(describeConfigsKafkaFutureMock.get()).thenReturn(resourceConfigMap);

    Map<PubSubTopic, Long> result = kafkaAdminAdapter.getAllTopicRetentions();
    assertEquals(result.size(), 4);
    assertEquals(result.get(pubSubTopicRepository.getTopic("t1_v1")), Long.valueOf(1));
    assertEquals(result.get(pubSubTopicRepository.getTopic("t2_rt")), Long.valueOf(2));
    assertEquals(result.get(pubSubTopicRepository.getTopic("t3_v2")), Long.valueOf(3));
    assertEquals(
        result.get(pubSubTopicRepository.getTopic("t4_rt")),
        Long.valueOf(PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION));
  }

  @Test
  public void testGetAllTopicRetentionsThrowsException() throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResultMock = mock(ListTopicsResult.class);
    KafkaFuture<Set<String>> listTopicsKafkaFutureMock = mock(KafkaFuture.class);
    Set<String> sampleTopics = new HashSet<>(Arrays.asList("t1_v1", "t2_rt", "t3_v2", "t4_rt"));

    when(internalKafkaAdminClientMock.listTopics()).thenReturn(listTopicsResultMock);
    when(listTopicsResultMock.names()).thenReturn(listTopicsKafkaFutureMock);
    when(listTopicsKafkaFutureMock.get()).thenReturn(sampleTopics);

    DescribeConfigsResult describeConfigsResultMock = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsKafkaFutureMock = mock(KafkaFuture.class);
    when(internalKafkaAdminClientMock.describeConfigs(any())).thenReturn(describeConfigsResultMock);
    when(describeConfigsResultMock.all()).thenReturn(describeConfigsKafkaFutureMock);
    when(describeConfigsKafkaFutureMock.get())
        .thenThrow(new ExecutionException(new NetworkException("Retriable exception")))
        .thenThrow(new ExecutionException(new UnknownServerException("Unknown server exception")))
        .thenThrow(new InterruptedException("Interrupted exception"));

    assertThrows(PubSubClientRetriableException.class, () -> kafkaAdminAdapter.getAllTopicRetentions());
    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.getAllTopicRetentions());
    assertThrows(PubSubClientException.class, () -> kafkaAdminAdapter.getAllTopicRetentions());
    assertTrue(Thread.currentThread().isInterrupted());

    verify(internalKafkaAdminClientMock, times(3)).describeConfigs(any());
    verify(describeConfigsResultMock, times(3)).all();
    verify(describeConfigsKafkaFutureMock, times(3)).get();
  }

  @Test
  public void testGetSomeTopicConfigs() throws ExecutionException, InterruptedException {
    Set<PubSubTopic> pubSubTopics =
        new HashSet<>(Arrays.asList(pubSubTopicRepository.getTopic("t1_v1"), pubSubTopicRepository.getTopic("t3_v2")));

    DescribeConfigsResult describeConfigsResultMock = mock(DescribeConfigsResult.class);
    KafkaFuture<Map<ConfigResource, Config>> describeConfigsKafkaFutureMock = mock(KafkaFuture.class);
    Map<ConfigResource, Config> resourceConfigMap = new HashMap<>();
    resourceConfigMap.put(
        new ConfigResource(ConfigResource.Type.TOPIC, "t1_v1"),
        new Config(Collections.singletonList(new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "1"))));

    resourceConfigMap.put(
        new ConfigResource(ConfigResource.Type.TOPIC, "t3_v2"),
        new Config(
            Arrays.asList(
                new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, "3"),
                new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))));

    when(internalKafkaAdminClientMock.describeConfigs(any())).thenReturn(describeConfigsResultMock);
    when(describeConfigsResultMock.all()).thenReturn(describeConfigsKafkaFutureMock);
    when(describeConfigsKafkaFutureMock.get()).thenReturn(resourceConfigMap);

    Map<PubSubTopic, PubSubTopicConfiguration> result = kafkaAdminAdapter.getSomeTopicConfigs(pubSubTopics);

    assertEquals(result.size(), 2);

    assertEquals(result.get(pubSubTopicRepository.getTopic("t1_v1")).isLogCompacted(), false);
    assertEquals(result.get(pubSubTopicRepository.getTopic("t1_v1")).retentionInMs(), Optional.of(1L));

    assertEquals(result.get(pubSubTopicRepository.getTopic("t3_v2")).isLogCompacted(), true);
    assertEquals(result.get(pubSubTopicRepository.getTopic("t3_v2")).retentionInMs(), Optional.of(3L));
  }
}
