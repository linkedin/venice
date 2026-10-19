package com.linkedin.venice.vpj.pubsub.input;

import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_DICTIONARY_MAX_MESSAGES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_MAX_SPLITS_PER_PARTITION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SPLIT_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SPLIT_TIME_WINDOW_IN_MINUTES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.PubSubEncryptionUtilsTest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PubSubSplitPlannerTest {
  private static final String TEST_TOPIC_NAME = "test_topic_v1";
  private static final String TEST_BROKER_URL = "localhost:9092";
  private static final PubSubTopicRepository TOPIC_REPO = new PubSubTopicRepository();

  private PubSubSplitPlanner planner;
  private TopicManager mockTopicManager;

  @BeforeMethod
  public void setUp() {
    this.mockTopicManager = mock(TopicManager.class);
    when(mockTopicManager.getTopicRepository()).thenReturn(TOPIC_REPO);
    this.planner = spy(new PubSubSplitPlanner());
  }

  @Test(dataProvider = "encryptionConfigurations", dataProviderClass = PubSubEncryptionUtilsTest.class)
  public void testEncryptionIsValidatedBeforeTopicManagerCreation(Boolean enabled, String keyUrn, String expectedUrn) {
    Properties properties = PubSubEncryptionUtilsTest.encryptionProperties(enabled, keyUrn);
    properties.setProperty(KAFKA_INPUT_TOPIC, TEST_TOPIC_NAME);
    properties.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, TEST_BROKER_URL);
    VeniceProperties config = new VeniceProperties(properties);
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());
    when(mockTopicManager.getPartitionCount(any(PubSubTopic.class))).thenReturn(0);

    if (Boolean.TRUE.equals(enabled) && expectedUrn == null) {
      VeniceException error = expectThrows(VeniceException.class, () -> planner.plan(config));
      assertTrue(error.getMessage().contains("missing or blank"));
      verify(planner, never()).createTopicManager(any(VeniceProperties.class), anyString());
      verifyNoInteractions(mockTopicManager);
    } else {
      assertTrue(planner.plan(config).isEmpty());
      verify(planner).createTopicManager(config, TEST_BROKER_URL);
    }
  }

  @DataProvider
  public Object[][] planningConfigurations() {
    return new Object[][] { { 4, 3000L, null, 0L, 0, 0L },
        { 6, 4000L, PartitionSplitStrategy.FIXED_RECORD_COUNT, 2000L, 8, 120L }, { 1, 500L, null, 0L, 0, 0L },
        { 1, 1000L, null, 0L, 0, 0L }, { 50, 2000L, null, 0L, 0, 0L }, { 0, 0L, null, 0L, 0, 0L },
        { 3, 1500L, PartitionSplitStrategy.FIXED_RECORD_COUNT, 1L, 1, 1L },
        { 3, 1500L, PartitionSplitStrategy.CAPPED_SPLIT_COUNT, 1000000L, 10000, 525600L },
        { 3, 1500L, PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION, 0L, 0, 0L } };
  }

  @Test(dataProvider = "planningConfigurations")
  public void testPlanWithConfigurations(
      int partitions,
      long recordsPerPartition,
      PartitionSplitStrategy strategy,
      long recordsPerSplit,
      int maxSplits,
      long timeWindowMinutes) {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());
    setupTopicManagerMocks(partitions, recordsPerPartition);
    VeniceProperties properties = strategy == null
        ? createVeniceProperties(TEST_TOPIC_NAME, TEST_BROKER_URL)
        : createVenicePropertiesWithValues(
            TEST_TOPIC_NAME,
            TEST_BROKER_URL,
            strategy.name(),
            recordsPerSplit,
            maxSplits,
            timeWindowMinutes);
    List<PubSubPartitionSplit> splits = planner.plan(properties);
    assertNotNull(splits);
    assertEquals(splits.isEmpty(), partitions == 0);
    assertTrue(splits.size() >= partitions);
    verify(mockTopicManager).getPartitionCount(any(PubSubTopic.class));
  }

  @Test
  public void testPlanWithErrorHandlingScenarios() {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());

    // Case 1: Missing KAFKA_INPUT_TOPIC in VeniceProperties
    Map<String, String> propsMap1 = new HashMap<>();
    propsMap1.put(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, TEST_BROKER_URL);
    VeniceProperties props1 = new VeniceProperties(propsMap1);

    expectThrows(UndefinedPropertyException.class, () -> planner.plan(props1));

    /* Case 2: Missing VENICE_REPUSH_SOURCE_PUBSUB_BROKER in VeniceProperties */
    Map<String, String> propsMap2 = new HashMap<>();
    propsMap2.put(KAFKA_INPUT_TOPIC, TEST_TOPIC_NAME);
    VeniceProperties props2 = new VeniceProperties(propsMap2);

    expectThrows(UndefinedPropertyException.class, () -> planner.plan(props2));

    // Case 3: Invalid split type
    VeniceProperties props3 =
        createVenicePropertiesWithValues(TEST_TOPIC_NAME, TEST_BROKER_URL, "INVALID_SPLIT_TYPE", 1000L, 5, 60L);

    expectThrows(IllegalArgumentException.class, () -> planner.plan(props3));
  }

  @Test(dataProvider = "splitTypeProvider")
  public void testPlanWithAllSplitTypes(PartitionSplitStrategy partitionSplitStrategy) {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());

    VeniceProperties props = createVenicePropertiesWithValues(
        TEST_TOPIC_NAME,
        TEST_BROKER_URL,
        partitionSplitStrategy.name(),
        1000L,
        5,
        180L);

    setupTopicManagerMocks(3, 3000L);
    List<PubSubPartitionSplit> splits = planner.plan(props);

    assertNotNull(splits);
    assertFalse(splits.isEmpty());

    // Verify all splits have the correct partition information
    for (PubSubPartitionSplit split: splits) {
      assertNotNull(split.getPubSubTopicPartition());
      assertEquals(split.getTopicName(), TEST_TOPIC_NAME);
      assertTrue(split.getPartitionNumber() >= 0);
      assertTrue(split.getPartitionNumber() < 3);
    }
  }

  @Test
  public void testPlanResourceManagementAndCleanup() {
    TopicManager spyTopicManager = spy(mockTopicManager);
    when(spyTopicManager.getTopicRepository()).thenReturn(TOPIC_REPO);
    doReturn(spyTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());

    // Case 1: TopicManager cleanup after VeniceProperties plan
    setupTopicManagerMocks(spyTopicManager, 2, 1000L);
    VeniceProperties props1 = createVeniceProperties(TEST_TOPIC_NAME, TEST_BROKER_URL);

    List<PubSubPartitionSplit> splits1 = planner.plan(props1);

    assertNotNull(splits1);
    verify(spyTopicManager, times(1)).getPartitionCount(any(PubSubTopic.class));

    // Case 2: Multiple consecutive calls handling
    setupTopicManagerMocks(spyTopicManager, 3, 1500L);
    VeniceProperties props2 = createVeniceProperties(TEST_TOPIC_NAME, TEST_BROKER_URL);

    List<PubSubPartitionSplit> splits2 = planner.plan(props2);

    assertNotNull(splits2);
    verify(spyTopicManager, times(2)).getPartitionCount(any(PubSubTopic.class));

    // Case 3: Different configuration calls
    setupTopicManagerMocks(spyTopicManager, 1, 800L);
    VeniceProperties props3 = createVenicePropertiesWithValues(
        TEST_TOPIC_NAME,
        TEST_BROKER_URL,
        PartitionSplitStrategy.FIXED_RECORD_COUNT.name(),
        500L,
        3,
        90L);

    List<PubSubPartitionSplit> splits3 = planner.plan(props3);

    assertNotNull(splits3);
    verify(spyTopicManager, times(3)).getPartitionCount(any(PubSubTopic.class));
  }

  @DataProvider(name = "splitTypeProvider")
  public Object[][] splitTypeProvider() {
    return new Object[][] { { PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION },
        { PartitionSplitStrategy.FIXED_RECORD_COUNT }, { PartitionSplitStrategy.CAPPED_SPLIT_COUNT },
        { PartitionSplitStrategy.CAPPED_SINGLE_SPLIT_PER_PARTITION } };
  }

  @DataProvider
  public Object[][] dictionaryScanBudgets() {
    return new Object[][] { { null, 4, 1_000_000L, 250_000L }, { "8000", 4, 1_000_000L, 2000L },
        { "1", 4, 1_000_000L, 1000L }, { "8000", 4, 2L, 2L }, { "8000", 4, 0L, 0L } };
  }

  @Test(dataProvider = "dictionaryScanBudgets")
  public void testDictionaryScanBudgetIsDividedAcrossPartitions(
      String budget,
      int partitions,
      long records,
      long expectedLimit) {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());
    setupTopicManagerMocks(partitions, records);
    Properties properties = new Properties();
    properties.setProperty(KAFKA_INPUT_TOPIC, TEST_TOPIC_NAME);
    properties.setProperty(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, TEST_BROKER_URL);
    properties
        .setProperty(PUBSUB_INPUT_SPLIT_STRATEGY, PartitionSplitStrategy.CAPPED_SINGLE_SPLIT_PER_PARTITION.name());
    properties.setProperty(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "1");
    if (budget != null) {
      properties.setProperty(COMPRESSION_DICTIONARY_MAX_MESSAGES, budget);
    }

    List<PubSubPartitionSplit> splits = planner.plan(new VeniceProperties(properties));
    assertEquals(splits.size(), partitions);
    for (PubSubPartitionSplit split: splits) {
      assertEquals(split.getNumberOfRecords(), expectedLimit);
      assertEquals(split.getEndPubSubPosition(), ApacheKafkaOffsetPosition.of(expectedLimit));
    }
    properties.setProperty(COMPRESSION_DICTIONARY_MAX_MESSAGES, "0");
    expectThrows(IllegalArgumentException.class, () -> planner.plan(new VeniceProperties(properties)));
    properties.setProperty(COMPRESSION_DICTIONARY_MAX_MESSAGES, "-1");
    expectThrows(IllegalArgumentException.class, () -> planner.plan(new VeniceProperties(properties)));
  }

  // Helper methods
  private VeniceProperties createVeniceProperties(String topicName, String brokerUrl) {
    Map<String, String> props = new HashMap<>();
    props.put(KAFKA_INPUT_TOPIC, topicName);
    props.put(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, brokerUrl);
    return new VeniceProperties(props);
  }

  private VeniceProperties createVenicePropertiesWithValues(
      String topicName,
      String brokerUrl,
      String splitType,
      long recordsPerSplit,
      int maxSplits,
      long timeWindowMinutes) {
    Map<String, String> props = new HashMap<>();
    props.put(KAFKA_INPUT_TOPIC, topicName);
    props.put(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, brokerUrl);
    props.put(PUBSUB_INPUT_SPLIT_STRATEGY, splitType);
    props.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, String.valueOf(recordsPerSplit));
    props.put(PUBSUB_INPUT_MAX_SPLITS_PER_PARTITION, String.valueOf(maxSplits));
    props.put(PUBSUB_INPUT_SPLIT_TIME_WINDOW_IN_MINUTES, String.valueOf(timeWindowMinutes));
    return new VeniceProperties(props);
  }

  private void setupTopicManagerMocks(int partitionCount, long recordsPerPartition) {
    setupTopicManagerMocks(mockTopicManager, partitionCount, recordsPerPartition);
  }

  private void setupTopicManagerMocks(TopicManager topicManager, int partitionCount, long recordsPerPartition) {
    PubSubTopic topic = TOPIC_REPO.getTopic(TEST_TOPIC_NAME);
    when(topicManager.getPartitionCount(any(PubSubTopic.class))).thenReturn(partitionCount);

    // Set up batch position fetching (used by PubSubSplitPlanner)
    Map<PubSubTopicPartition, PubSubPosition> startPositions = new HashMap<>();
    Map<PubSubTopicPartition, PubSubPosition> endPositions = new HashMap<>();

    for (int i = 0; i < partitionCount; i++) {
      PubSubTopicPartition tp = new PubSubTopicPartitionImpl(topic, i);
      PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0L);
      PubSubPosition endPos = ApacheKafkaOffsetPosition.of(recordsPerPartition);
      startPositions.put(tp, startPos);
      endPositions.put(tp, endPos);
    }

    when(topicManager.getStartPositionsForTopicWithRetries(any(PubSubTopic.class))).thenReturn(startPositions);
    when(topicManager.getEndPositionsForTopicWithRetries(any(PubSubTopic.class))).thenReturn(endPositions);

    // diffPosition is called by the planner to compute record counts
    when(
        topicManager
            .diffPosition(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(PubSubPosition.class)))
                .thenReturn(recordsPerPartition);

    // advancePosition is still called by strategies (pure math)
    when(topicManager.advancePosition(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(Long.class)))
        .thenAnswer(invocation -> {
          PubSubPosition start = invocation.getArgument(1);
          long n = invocation.getArgument(2);
          return ApacheKafkaOffsetPosition.of(start.getNumericOffset() + n);
        });
  }
}
