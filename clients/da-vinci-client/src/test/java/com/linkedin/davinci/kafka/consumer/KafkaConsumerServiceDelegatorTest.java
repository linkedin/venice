package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.ingestion.consumption.ConsumedDataReceiver;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
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

  @Test(dataProvider = "Method-List")
  public void chooseConsumerServiceTest(String methodName)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    KafkaConsumerService mockDefaultConsumerService = mock(KafkaConsumerService.class);
    KafkaConsumerService mockDedicatedConsumerService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor =
        (ignored, statSuffix) -> statSuffix.isEmpty() ? mockDefaultConsumerService : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);

    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PubSubTopicPartition topicPartitionForVT = new PubSubTopicPartitionImpl(versionTopic, PARTITION_ID);
    PubSubTopicPartition topicPartitionForRT = new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID);

    Method testMethod =
        KafkaConsumerServiceDelegator.class.getMethod(methodName, PubSubTopic.class, PubSubTopicPartition.class);
    Method verifyMethod =
        KafkaConsumerService.class.getMethod(methodName, PubSubTopic.class, PubSubTopicPartition.class);

    testMethod.invoke(delegator, versionTopic, topicPartitionForVT);
    verifyMethod.invoke(verify(mockDefaultConsumerService), versionTopic, topicPartitionForVT);
    verifyMethod.invoke(verify(mockDedicatedConsumerService, never()), versionTopic, topicPartitionForVT);
    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    testMethod.invoke(delegator, versionTopic, topicPartitionForRT);
    verifyMethod.invoke(verify(mockDedicatedConsumerService), versionTopic, topicPartitionForRT);
    verifyMethod.invoke(verify(mockDefaultConsumerService, never()), versionTopic, topicPartitionForRT);
    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);

    isAAWCStoreFunc = vt -> false;
    delegator = new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);

    testMethod.invoke(delegator, versionTopic, topicPartitionForVT);
    verifyMethod.invoke(verify(mockDefaultConsumerService), versionTopic, topicPartitionForVT);
    verifyMethod.invoke(verify(mockDedicatedConsumerService, never()), versionTopic, topicPartitionForVT);
    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    testMethod.invoke(delegator, versionTopic, topicPartitionForRT);
    verifyMethod.invoke(verify(mockDefaultConsumerService), versionTopic, topicPartitionForRT);
    verifyMethod.invoke(verify(mockDedicatedConsumerService, never()), versionTopic, topicPartitionForRT);
  }

  @Test
  public void unsubscribeAllTest() {
    KafkaConsumerService mockDefaultConsumerService = mock(KafkaConsumerService.class);
    KafkaConsumerService mockDedicatedConsumerService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor =
        (ignored, statSuffix) -> statSuffix.isEmpty() ? mockDefaultConsumerService : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);
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

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor =
        (ignored, statSuffix) -> statSuffix.isEmpty() ? mockDefaultConsumerService : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);
    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PubSubTopicPartition topicPartitionForVT = new PubSubTopicPartitionImpl(versionTopic, PARTITION_ID);
    PubSubTopicPartition topicPartitionForRT = new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID);
    Set<PubSubTopicPartition> partitionSet = new HashSet<>();
    partitionSet.add(topicPartitionForVT);
    partitionSet.add(topicPartitionForRT);

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
    delegator = new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);
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

    AtomicBoolean retValueForIsAAWCStoreFunc = new AtomicBoolean(false);
    Function<String, Boolean> isAAWCStoreFunc = vt -> retValueForIsAAWCStoreFunc.get();
    BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor =
        (ignored, statSuffix) -> statSuffix.isEmpty() ? mockDefaultConsumerService : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);

    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PubSubTopicPartition topicPartitionForRT = new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID);

    delegator.assignConsumerFor(versionTopic, topicPartitionForRT);
    verify(mockDefaultConsumerService).assignConsumerFor(versionTopic, topicPartitionForRT);
    verify(mockDedicatedConsumerService, never()).assignConsumerFor(versionTopic, topicPartitionForRT);

    reset(mockDefaultConsumerService);
    reset(mockDedicatedConsumerService);
    // Change the AAWC flag
    retValueForIsAAWCStoreFunc.set(true);
    delegator.unSubscribe(versionTopic, topicPartitionForRT);
    verify(mockDefaultConsumerService).unSubscribe(versionTopic, topicPartitionForRT);
    verify(mockDedicatedConsumerService, never()).unSubscribe(versionTopic, topicPartitionForRT);
  }

  @Test
  public void startConsumptionIntoDataReceiverTest() {
    KafkaConsumerService mockDefaultConsumerService = mock(KafkaConsumerService.class);
    KafkaConsumerService mockDedicatedConsumerService = mock(KafkaConsumerService.class);
    VeniceServerConfig mockConfig = mock(VeniceServerConfig.class);
    doReturn(true).when(mockConfig).isDedicatedConsumerPoolForAAWCLeaderEnabled();

    Function<String, Boolean> isAAWCStoreFunc = vt -> true;
    BiFunction<Integer, String, KafkaConsumerService> consumerServiceConstructor =
        (ignored, statSuffix) -> statSuffix.isEmpty() ? mockDefaultConsumerService : mockDedicatedConsumerService;

    KafkaConsumerServiceDelegator delegator =
        new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);
    PubSubTopic versionTopic = TOPIC_REPOSITORY.getTopic(VERSION_TOPIC_NAME);
    PubSubTopic rtTopic = TOPIC_REPOSITORY.getTopic(RT_TOPIC_NAME);
    PubSubTopicPartition topicPartitionForVT = new PubSubTopicPartitionImpl(versionTopic, PARTITION_ID);
    PubSubTopicPartition topicPartitionForRT = new PubSubTopicPartitionImpl(rtTopic, PARTITION_ID);

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
    delegator = new KafkaConsumerServiceDelegator(mockConfig, consumerServiceConstructor, isAAWCStoreFunc);

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
  }
}
