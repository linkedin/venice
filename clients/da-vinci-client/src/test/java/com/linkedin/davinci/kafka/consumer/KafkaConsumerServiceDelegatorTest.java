package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class KafkaConsumerServiceDelegatorTest {
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final String VERSION_TOPIC_NAME = "test_store_v1";
  private static final String RT_TOPIC_NAME = "test_store_rt";
  private static final int PARTITION_ID = 1;

  @DataProvider(name = "Method-List")
  public static Object[][] methodList() {
    return new Object[][] { { "getConsumerAssignedToVersionTopicPartition" }, { "assignConsumerFor" },
        { "unSubscribe" }, { "getOffsetLagBasedOnMetrics" }, { "getLatestOffsetBasedOnMetrics" } };
  }

  private void invokeAndVerify(
      KafkaConsumerServiceDelegator delegator,
      KafkaConsumerService invokedConsumerService,
      KafkaConsumerService unusedConsumerService,
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartition,
      String methodName) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    boolean includeLongParam = methodName.equals("unSubscribe");
    if (includeLongParam) {
      Method testMethod = KafkaConsumerServiceDelegator.class
          .getMethod(methodName, PubSubTopic.class, PubSubTopicPartition.class, long.class);
      Method verifyMethod =
          KafkaConsumerService.class.getMethod(methodName, PubSubTopic.class, PubSubTopicPartition.class, long.class);
      testMethod.invoke(delegator, versionTopic, topicPartition, 0L);
      verifyMethod.invoke(verify(invokedConsumerService), versionTopic, topicPartition, 0L);
      verifyMethod.invoke(verify(unusedConsumerService, never()), versionTopic, topicPartition, 0L);
    } else {
      Method testMethod =
          KafkaConsumerServiceDelegator.class.getMethod(methodName, PubSubTopic.class, PubSubTopicPartition.class);
      Method verifyMethod =
          KafkaConsumerService.class.getMethod(methodName, PubSubTopic.class, PubSubTopicPartition.class);
      testMethod.invoke(delegator, versionTopic, topicPartition);
      verifyMethod.invoke(verify(invokedConsumerService), versionTopic, topicPartition);
      verifyMethod.invoke(verify(unusedConsumerService, never()), versionTopic, topicPartition);
    }

    reset(invokedConsumerService);
    reset(unusedConsumerService);
  }

  @Test(dataProvider = "Method-List")
  public void chooseConsumerServiceTest(String methodName)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    KafkaConsumerService defaultMockService = mock(KafkaConsumerService.class);
    KafkaConsumerService dedicatedMockService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.AA_OR_WC_LEADER_DEDICATED).when(mockConfig)
        .getConsumerPoolStrategyType();

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    KafkaConsumerServiceDelegator.KafkaConsumerServiceBuilder consumerServiceBuilder =
        (ignored, poolType) -> poolType.equals(ConsumerPoolType.REGULAR_POOL)
            ? defaultMockService
            : dedicatedMockService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);

    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PubSubTopicPartition topicPartitionForVT = new PubSubTopicPartitionImpl(versionTopic, PARTITION_ID);
    PubSubTopicPartition topicPartitionForRT = new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID);

    ConsumedDataReceiver dataReceiver = mock(ConsumedDataReceiver.class);
    doReturn(versionTopic).when(dataReceiver).destinationIdentifier();

    PartitionReplicaIngestionContext topicPartitionIngestionContextForVT = new PartitionReplicaIngestionContext(
        versionTopic,
        topicPartitionForVT,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForVT, 0, dataReceiver);
    PartitionReplicaIngestionContext topicPartitionIngestionContextForRT = new PartitionReplicaIngestionContext(
        versionTopic,
        topicPartitionForRT,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForRT, 0, dataReceiver);

    invokeAndVerify(delegator, defaultMockService, dedicatedMockService, versionTopic, topicPartitionForVT, methodName);
    invokeAndVerify(delegator, dedicatedMockService, defaultMockService, versionTopic, topicPartitionForRT, methodName);

    isAAWCStoreFunc = vt -> false;
    delegator = new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForVT, 0, dataReceiver);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForRT, 0, dataReceiver);

    invokeAndVerify(delegator, defaultMockService, dedicatedMockService, versionTopic, topicPartitionForVT, methodName);
    invokeAndVerify(delegator, defaultMockService, dedicatedMockService, versionTopic, topicPartitionForRT, methodName);
  }

  @Test
  public void unsubscribeAllTest() {
    KafkaConsumerService mockDefaultConsumerService = mock(KafkaConsumerService.class);
    KafkaConsumerService mockDedicatedConsumerService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.AA_OR_WC_LEADER_DEDICATED).when(mockConfig)
        .getConsumerPoolStrategyType();

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    KafkaConsumerServiceDelegator.KafkaConsumerServiceBuilder consumerServiceBuilder =
        (ignored, poolType) -> poolType.equals(ConsumerPoolType.REGULAR_POOL)
            ? mockDefaultConsumerService
            : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);
    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    delegator.unsubscribeAll(versionTopic);
    verify(mockDefaultConsumerService).unsubscribeAll(versionTopic);
    verify(mockDedicatedConsumerService).unsubscribeAll(versionTopic);
  }

  @Test
  public void batchUnsubscribe_start_stop_getMaxElapsedTimeMSSinceLastPollInConsumerPool_hasAnySubscriptionFor_Test()
      throws Exception {
    KafkaConsumerService mockDefaultConsumerService = mock(KafkaConsumerService.class);
    KafkaConsumerService mockDedicatedConsumerService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.AA_OR_WC_LEADER_DEDICATED).when(mockConfig)
        .getConsumerPoolStrategyType();

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    KafkaConsumerServiceDelegator.KafkaConsumerServiceBuilder consumerServiceBuilder =
        (ignored, poolType) -> poolType.equals(ConsumerPoolType.REGULAR_POOL)
            ? mockDefaultConsumerService
            : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);
    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PubSubTopicPartition topicPartitionForVT = new PubSubTopicPartitionImpl(versionTopic, PARTITION_ID);
    PubSubTopicPartition topicPartitionForRT = new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID);
    Set<PubSubTopicPartition> partitionSet = new HashSet<>();
    partitionSet.add(topicPartitionForVT);
    partitionSet.add(topicPartitionForRT);

    ConsumedDataReceiver dataReceiver = mock(ConsumedDataReceiver.class);
    doReturn(versionTopic).when(dataReceiver).destinationIdentifier();

    PartitionReplicaIngestionContext topicPartitionIngestionContextForVT = new PartitionReplicaIngestionContext(
        versionTopic,
        topicPartitionForVT,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForVT, 0, dataReceiver);
    PartitionReplicaIngestionContext topicPartitionIngestionContextForRT = new PartitionReplicaIngestionContext(
        versionTopic,
        topicPartitionForRT,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForRT, 0, dataReceiver);

    delegator.batchUnsubscribe(versionTopic, partitionSet);
    verify(mockDefaultConsumerService).batchUnsubscribe(versionTopic, partitionSet);
    verify(mockDedicatedConsumerService).batchUnsubscribe(versionTopic, partitionSet);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.startInner();
    verify(mockDefaultConsumerService).start();
    verify(mockDedicatedConsumerService).start();

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.stopInner();
    verify(mockDefaultConsumerService).stop();
    verify(mockDedicatedConsumerService).stop();

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(mockDedicatedConsumerService).getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(mockDefaultConsumerService).getMaxElapsedTimeMSSinceLastPollInConsumerPool();

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    doReturn(false).when(mockDefaultConsumerService).hasAnySubscriptionFor(any());
    doReturn(true).when(mockDedicatedConsumerService).hasAnySubscriptionFor(any());
    assertTrue(delegator.hasAnySubscriptionFor(versionTopic));
    verify(mockDedicatedConsumerService).hasAnySubscriptionFor(versionTopic);
    verify(mockDefaultConsumerService).hasAnySubscriptionFor(versionTopic);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    doReturn(true).when(mockDefaultConsumerService).hasAnySubscriptionFor(any());
    doReturn(true).when(mockDedicatedConsumerService).hasAnySubscriptionFor(any());
    assertTrue(delegator.hasAnySubscriptionFor(versionTopic));
    verify(mockDedicatedConsumerService, never()).hasAnySubscriptionFor(versionTopic);
    verify(mockDefaultConsumerService).hasAnySubscriptionFor(versionTopic);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    doReturn(false).when(mockDefaultConsumerService).hasAnySubscriptionFor(any());
    doReturn(false).when(mockDedicatedConsumerService).hasAnySubscriptionFor(any());
    assertFalse(delegator.hasAnySubscriptionFor(versionTopic));
    verify(mockDedicatedConsumerService).hasAnySubscriptionFor(versionTopic);
    verify(mockDefaultConsumerService).hasAnySubscriptionFor(versionTopic);

    // When dedicated consumer pool is disabled.
    reset(mockConfig);
    doReturn(false).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.DEFAULT).when(mockConfig)
        .getConsumerPoolStrategyType();
    delegator = new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForVT, 0, dataReceiver);
    delegator.startConsumptionIntoDataReceiver(topicPartitionIngestionContextForRT, 0, dataReceiver);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.startInner();
    verify(mockDefaultConsumerService).start();
    verify(mockDedicatedConsumerService, never()).start();

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.stopInner();
    verify(mockDefaultConsumerService).stop();
    verify(mockDedicatedConsumerService, never()).stop();

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.batchUnsubscribe(versionTopic, partitionSet);
    verify(mockDefaultConsumerService).batchUnsubscribe(versionTopic, partitionSet);
    verify(mockDedicatedConsumerService, never()).batchUnsubscribe(versionTopic, partitionSet);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(mockDefaultConsumerService).getMaxElapsedTimeMSSinceLastPollInConsumerPool();
    verify(mockDedicatedConsumerService, never()).getMaxElapsedTimeMSSinceLastPollInConsumerPool();
  }

  @Test
  public void consumerAssignmentStickiness() {
    KafkaConsumerService mockDefaultConsumerService = mock(KafkaConsumerService.class);
    KafkaConsumerService mockDedicatedConsumerService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.AA_OR_WC_LEADER_DEDICATED).when(mockConfig)
        .getConsumerPoolStrategyType();

    AtomicBoolean retValueForIsAAWCStoreFunc = new AtomicBoolean(false);
    Function<String, Boolean> isAAWCStoreFunc = vt -> retValueForIsAAWCStoreFunc.get();
    KafkaConsumerServiceDelegator.KafkaConsumerServiceBuilder consumerServiceBuilder =
        (ignored, poolType) -> poolType.equals(ConsumerPoolType.REGULAR_POOL)
            ? mockDefaultConsumerService
            : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);

    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PubSubTopicPartition topicPartitionForRT = new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID);
    ConsumedDataReceiver dataReceiver = mock(ConsumedDataReceiver.class);
    doReturn(versionTopic).when(dataReceiver).destinationIdentifier();

    PartitionReplicaIngestionContext topicPartitionForVT = new PartitionReplicaIngestionContext(
        versionTopic,
        topicPartitionForRT,
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    delegator.startConsumptionIntoDataReceiver(topicPartitionForVT, 0, dataReceiver);
    delegator.assignConsumerFor(versionTopic, topicPartitionForRT);
    verify(mockDefaultConsumerService).assignConsumerFor(versionTopic, topicPartitionForRT);
    verify(mockDedicatedConsumerService, never()).assignConsumerFor(versionTopic, topicPartitionForRT);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    // Change the AAWC flag
    retValueForIsAAWCStoreFunc.set(true);
    delegator.unSubscribe(versionTopic, topicPartitionForRT);
    verify(mockDefaultConsumerService)
        .unSubscribe(versionTopic, topicPartitionForRT, SharedKafkaConsumer.DEFAULT_MAX_WAIT_MS);
    verify(mockDedicatedConsumerService, never()).unSubscribe(versionTopic, topicPartitionForRT);
  }

  @Test
  public void startConsumptionIntoDataReceiverTest() {
    KafkaConsumerService mockDefaultConsumerService = mock(KafkaConsumerService.class);
    KafkaConsumerService mockDedicatedConsumerService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.AA_OR_WC_LEADER_DEDICATED).when(mockConfig)
        .getConsumerPoolStrategyType();

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    KafkaConsumerServiceDelegator.KafkaConsumerServiceBuilder consumerServiceBuilder =
        (ignored, poolType) -> poolType.equals(ConsumerPoolType.REGULAR_POOL)
            ? mockDefaultConsumerService
            : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);
    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PartitionReplicaIngestionContext topicPartitionForVT = new PartitionReplicaIngestionContext(
        versionTopic,
        new PubSubTopicPartitionImpl(versionTopic, PARTITION_ID),
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);
    PartitionReplicaIngestionContext topicPartitionForRT = new PartitionReplicaIngestionContext(
        versionTopic,
        new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID),
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.NON_AA_OR_WRITE_COMPUTE);

    ConsumedDataReceiver dataReceiver = mock(ConsumedDataReceiver.class);
    doReturn(versionTopic).when(dataReceiver).destinationIdentifier();

    delegator.startConsumptionIntoDataReceiver(topicPartitionForVT, 0, dataReceiver);
    verify(mockDefaultConsumerService).startConsumptionIntoDataReceiver(topicPartitionForVT, 0, dataReceiver);
    verify(mockDedicatedConsumerService, never())
        .startConsumptionIntoDataReceiver(topicPartitionForVT, 0, dataReceiver);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.startConsumptionIntoDataReceiver(topicPartitionForRT, 0, dataReceiver);
    verify(mockDefaultConsumerService, never()).startConsumptionIntoDataReceiver(topicPartitionForRT, 0, dataReceiver);
    verify(mockDedicatedConsumerService).startConsumptionIntoDataReceiver(topicPartitionForRT, 0, dataReceiver);

    // Test non-AA/WC cases
    isAAWCStoreFunc = vt -> false;
    delegator = new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.startConsumptionIntoDataReceiver(topicPartitionForVT, 0, dataReceiver);
    verify(mockDefaultConsumerService).startConsumptionIntoDataReceiver(topicPartitionForVT, 0, dataReceiver);
    verify(mockDedicatedConsumerService, never())
        .startConsumptionIntoDataReceiver(topicPartitionForVT, 0, dataReceiver);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    delegator.startConsumptionIntoDataReceiver(topicPartitionForRT, 0, dataReceiver);
    verify(mockDefaultConsumerService).startConsumptionIntoDataReceiver(topicPartitionForRT, 0, dataReceiver);
    verify(mockDedicatedConsumerService, never())
        .startConsumptionIntoDataReceiver(topicPartitionForRT, 0, dataReceiver);

    // Test current version prioritization strategy
    PartitionReplicaIngestionContext tpForCurrentAAWCLeader = new PartitionReplicaIngestionContext(
        versionTopic,
        new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID),
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE);
    PartitionReplicaIngestionContext tpForCurrentAAWCFollower = new PartitionReplicaIngestionContext(
        versionTopic,
        new PubSubTopicPartitionImpl(versionTopic, PARTITION_ID),
        PartitionReplicaIngestionContext.VersionRole.CURRENT,
        PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE);

    PubSubTopic futureVersionTopic = TOPIC_REPOSITORY.getTopic("test_store_v2");
    PartitionReplicaIngestionContext tpForNonCurrentAAWCLeader = new PartitionReplicaIngestionContext(
        versionTopic,
        new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID),
        PartitionReplicaIngestionContext.VersionRole.FUTURE,
        PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE);
    PartitionReplicaIngestionContext tpForNonCurrentAAWCFollower = new PartitionReplicaIngestionContext(
        versionTopic,
        new PubSubTopicPartitionImpl(futureVersionTopic, PARTITION_ID),
        PartitionReplicaIngestionContext.VersionRole.BACKUP,
        PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE);

    doReturn(false).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(true).when(mockConfig).isResubscriptionTriggeredByVersionIngestionContextChangeEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION).when(mockConfig)
        .getConsumerPoolStrategyType();

    KafkaConsumerService consumerServiceForCurrentVersionAAWCLeader = mock(KafkaConsumerService.class);
    KafkaConsumerService consumerServiceForCurrentVersionNonAAWCLeader = mock(KafkaConsumerService.class);
    KafkaConsumerService consumerServiceForNonCurrentVersionAAWCLeader = mock(KafkaConsumerService.class);
    KafkaConsumerService consumerServiceForNonCurrentVersionNonAAWCLeader = mock(KafkaConsumerService.class);

    Map<PartitionReplicaIngestionContext, KafkaConsumerService> consumerServiceMap = new HashMap<>();
    consumerServiceMap.put(tpForCurrentAAWCLeader, consumerServiceForCurrentVersionAAWCLeader);
    consumerServiceMap.put(tpForCurrentAAWCFollower, consumerServiceForCurrentVersionNonAAWCLeader);
    consumerServiceMap.put(tpForNonCurrentAAWCLeader, consumerServiceForNonCurrentVersionAAWCLeader);
    consumerServiceMap.put(tpForNonCurrentAAWCFollower, consumerServiceForNonCurrentVersionNonAAWCLeader);

    consumerServiceBuilder = (ignored, poolType) -> {
      if (poolType.equals(ConsumerPoolType.CURRENT_VERSION_AA_WC_LEADER_POOL)) {
        return consumerServiceForCurrentVersionAAWCLeader;
      } else if (poolType.equals(ConsumerPoolType.CURRENT_VERSION_NON_AA_WC_LEADER_POOL)) {
        return consumerServiceForCurrentVersionNonAAWCLeader;
      } else if (poolType.equals(ConsumerPoolType.NON_CURRENT_VERSION_AA_WC_LEADER_POOL)) {
        return consumerServiceForNonCurrentVersionAAWCLeader;
      } else if (poolType.equals(ConsumerPoolType.NON_CURRENT_VERSION_NON_AA_WC_LEADER_POOL)) {
        return consumerServiceForNonCurrentVersionNonAAWCLeader;
      }
      return null;
    };

    delegator = new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);
    verifyConsumerServiceStartConsumptionIntoDataReceiver(
        delegator,
        consumerServiceMap,
        tpForCurrentAAWCLeader,
        dataReceiver);
    verifyConsumerServiceStartConsumptionIntoDataReceiver(
        delegator,
        consumerServiceMap,
        tpForCurrentAAWCFollower,
        dataReceiver);
    verifyConsumerServiceStartConsumptionIntoDataReceiver(
        delegator,
        consumerServiceMap,
        tpForNonCurrentAAWCFollower,
        dataReceiver);
    verifyConsumerServiceStartConsumptionIntoDataReceiver(
        delegator,
        consumerServiceMap,
        tpForNonCurrentAAWCLeader,
        dataReceiver);
  }

  private void verifyConsumerServiceStartConsumptionIntoDataReceiver(
      KafkaConsumerServiceDelegator delegator,
      Map<PartitionReplicaIngestionContext, KafkaConsumerService> consumerServiceMap,
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      ConsumedDataReceiver dataReceiver) {
    delegator.startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, 0, dataReceiver);
    for (Map.Entry<PartitionReplicaIngestionContext, KafkaConsumerService> entry: consumerServiceMap.entrySet()) {
      if (entry.getKey().equals(partitionReplicaIngestionContext)) {
        verify(entry.getValue()).startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, 0, dataReceiver);
      } else {
        verify(entry.getValue(), never())
            .startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, 0, dataReceiver);
      }
    }
    for (KafkaConsumerService consumerService: consumerServiceMap.values()) {
      reset(consumerService);
    }
    delegator.unSubscribe(
        partitionReplicaIngestionContext.getVersionTopic(),
        partitionReplicaIngestionContext.getPubSubTopicPartition());
  }

  /**
   * This test simulates multiple threads resubscribing to the same real-time topic partition for different store
   * versions. It verifies if the lock effectively protects the handoff between {@link ConsumptionTask} and
   * {@link ConsumedDataReceiver} during the re-subscription process. Previously, an unprotected handoff led to an
   * {@link IllegalStateException} being thrown within {@link ConsumptionTask#setDataReceiver}.
   * To induce the race condition, we assume there are 5 store versions, with each version assigned to a dedicated
   * thread that continuously resubscribes to the same real-time topic partition. This continues until either a race
   * condition is encountered or the test times out (30 seconds).
   */
  @Test
  public void testKafkaConsumerServiceResubscriptionConcurrency() throws Exception {
    ApacheKafkaConsumerAdapter consumer1 = mock(ApacheKafkaConsumerAdapter.class);
    PubSubConsumerAdapterFactory factory = mock(PubSubConsumerAdapterFactory.class);
    when(factory.create(any(), anyBoolean(), any(), any())).thenReturn(consumer1);

    Properties properties = new Properties();
    String testKafkaUrl = "test_kafka_url";
    properties.put(KAFKA_BOOTSTRAP_SERVERS, testKafkaUrl);
    MetricsRepository mockMetricsRepository = mock(MetricsRepository.class);
    final Sensor mockSensor = mock(Sensor.class);
    doReturn(mockSensor).when(mockMetricsRepository).sensor(anyString(), any());

    int versionNum = 5;
    PubSubMessageDeserializer pubSubDeserializer = new PubSubMessageDeserializer(
        new OptimizedKafkaValueSerializer(),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    KafkaConsumerService consumerService = new PartitionWiseKafkaConsumerService(
        ConsumerPoolType.REGULAR_POOL,
        factory,
        properties,
        1000l,
        versionNum + 2, // To simulate real production cases: consumers # >> version # per store.
        mock(IngestionThrottler.class),
        mock(KafkaClusterBasedRecordThrottler.class),
        mockMetricsRepository,
        "test_kafka_cluster_alias",
        TimeUnit.MINUTES.toMillis(1),
        mock(TopicExistenceChecker.class),
        false,
        pubSubDeserializer,
        SystemTime.INSTANCE,
        mock(AggKafkaConsumerServiceStats.class),
        false,
        mock(ReadOnlyStoreRepository.class),
        false);
    String storeName = Utils.getUniqueString("test_consumer_service");

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    KafkaConsumerServiceDelegator.KafkaConsumerServiceBuilder consumerServiceBuilder =
        (ignored, poolType) -> consumerService;
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(false).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();
    doReturn(true).when(mockConfig).isResubscriptionTriggeredByVersionIngestionContextChangeEnabled();
    doReturn(KafkaConsumerServiceDelegator.ConsumerPoolStrategyType.CURRENT_VERSION_PRIORITIZATION).when(mockConfig)
        .getConsumerPoolStrategyType();
    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceBuilder, isAAWCStoreFunc);
    PubSubTopicPartition realTimeTopicPartition =
        new PubSubTopicPartitionImpl(TOPIC_REPOSITORY.getTopic(Utils.composeRealTimeTopic(storeName)), 0);

    CountDownLatch countDownLatch = new CountDownLatch(1);
    List<Thread> infiniteSubUnSubThreads = new ArrayList<>();
    for (int i = 0; i < versionNum; i++) {
      PubSubTopic versionTopicForStoreName3 = TOPIC_REPOSITORY.getTopic(Version.composeKafkaTopic(storeName, i));
      StoreIngestionTask task = mock(StoreIngestionTask.class);
      when(task.getVersionTopic()).thenReturn(versionTopicForStoreName3);
      when(task.isHybridMode()).thenReturn(true);

      PartitionReplicaIngestionContext partitionReplicaIngestionContext = new PartitionReplicaIngestionContext(
          versionTopicForStoreName3,
          realTimeTopicPartition,
          PartitionReplicaIngestionContext.VersionRole.CURRENT,
          PartitionReplicaIngestionContext.WorkloadType.AA_OR_WRITE_COMPUTE);
      ConsumedDataReceiver consumedDataReceiver = mock(ConsumedDataReceiver.class);
      when(consumedDataReceiver.destinationIdentifier()).thenReturn(versionTopicForStoreName3);
      Runnable infiniteSubUnSub = getResubscriptionRunnableFor(
          delegator,
          partitionReplicaIngestionContext,
          consumedDataReceiver,
          countDownLatch);
      Thread infiniteSubUnSubThread = new Thread(infiniteSubUnSub, "infiniteResubscribe: " + versionTopicForStoreName3);
      infiniteSubUnSubThread.start();
      infiniteSubUnSubThreads.add(infiniteSubUnSubThread);
      // Wait for the thread to start.
      waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        assertTrue(
            infiniteSubUnSubThread.getState().equals(Thread.State.WAITING)
                || infiniteSubUnSubThread.getState().equals(Thread.State.TIMED_WAITING)
                || infiniteSubUnSubThread.getState().equals(Thread.State.BLOCKED)
                || infiniteSubUnSubThread.getState().equals(Thread.State.RUNNABLE));
      });
    }
    long currentTime = System.currentTimeMillis();
    Boolean raceConditionFound = countDownLatch.await(30, TimeUnit.SECONDS);
    long elapsedTime = System.currentTimeMillis() - currentTime;
    for (Thread infiniteSubUnSubThread: infiniteSubUnSubThreads) {
      assertTrue(
          infiniteSubUnSubThread.getState().equals(Thread.State.WAITING)
              || infiniteSubUnSubThread.getState().equals(Thread.State.TIMED_WAITING)
              || infiniteSubUnSubThread.getState().equals(Thread.State.BLOCKED)
              || infiniteSubUnSubThread.getState().equals(Thread.State.RUNNABLE));
      infiniteSubUnSubThread.interrupt();
      infiniteSubUnSubThread.join();
      assertEquals(Thread.State.TERMINATED, infiniteSubUnSubThread.getState());
    }
    Assert.assertFalse(
        raceConditionFound,
        "Found race condition in KafkaConsumerService with time passed in milliseconds: " + elapsedTime);
    delegator.close();
  }

  private Runnable getResubscriptionRunnableFor(
      KafkaConsumerServiceDelegator consumerServiceDelegator,
      PartitionReplicaIngestionContext partitionReplicaIngestionContext,
      ConsumedDataReceiver consumedDataReceiver,
      CountDownLatch countDownLatch) {
    PubSubTopic versionTopic = partitionReplicaIngestionContext.getVersionTopic();
    PubSubTopicPartition pubSubTopicPartition = partitionReplicaIngestionContext.getPubSubTopicPartition();
    return () -> {
      try {
        while (true) {
          if (Thread.currentThread().isInterrupted()) {
            consumerServiceDelegator.unSubscribe(versionTopic, pubSubTopicPartition);
            break;
          }
          consumerServiceDelegator
              .startConsumptionIntoDataReceiver(partitionReplicaIngestionContext, 0, consumedDataReceiver);
          // Use low wait time to trigger unsubscribe and poll lock handoff.
          consumerServiceDelegator.assignConsumerFor(versionTopic, pubSubTopicPartition).setTimeoutMsOverride(1L);
          int versionNum =
              Version.parseVersionFromKafkaTopicName(partitionReplicaIngestionContext.getVersionTopic().getName());
          if (versionNum % 3 == 0) {
            consumerServiceDelegator.unSubscribe(versionTopic, pubSubTopicPartition);
          } else if (versionNum % 3 == 1) {
            consumerServiceDelegator.unsubscribeAll(partitionReplicaIngestionContext.getVersionTopic());
          } else {
            consumerServiceDelegator.batchUnsubscribe(
                partitionReplicaIngestionContext.getVersionTopic(),
                Collections.singleton(partitionReplicaIngestionContext.getPubSubTopicPartition()));
          }
        }
      } catch (Exception e) {
        // If any thread encounter an exception, count down the latch to 0 to indicate main thread to catch the issue.
        e.printStackTrace();
        countDownLatch.countDown();
      }
    };
  }
}
