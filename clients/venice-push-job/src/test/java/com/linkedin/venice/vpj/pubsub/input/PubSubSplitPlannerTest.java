package com.linkedin.venice.vpj.pubsub.input;

import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_MAX_SPLITS_PER_PARTITION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SPLIT_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SPLIT_TIME_WINDOW_IN_MINUTES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  @Test
  public void testPlanWithVenicePropertiesNormalScenarios() {
    // Case 1: Default values scenario
    VeniceProperties props1 = createVeniceProperties(TEST_TOPIC_NAME, TEST_BROKER_URL);
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());
    setupTopicManagerMocks(4, 3000L);

    List<PubSubPartitionSplit> splits1 = planner.plan(props1);

    assertNotNull(splits1);
    assertFalse(splits1.isEmpty());
    verify(mockTopicManager, times(1)).getPartitionCount(any(PubSubTopic.class));

    // Case 2: Custom properties configuration
    Map<String, String> customProps = new HashMap<>();
    customProps.put(KAFKA_INPUT_TOPIC, TEST_TOPIC_NAME);
    customProps.put(KAFKA_INPUT_BROKER_URL, TEST_BROKER_URL);
    customProps.put(PUBSUB_INPUT_SPLIT_STRATEGY, PartitionSplitStrategy.FIXED_RECORD_COUNT.name());
    customProps.put(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, "2000");
    customProps.put(PUBSUB_INPUT_MAX_SPLITS_PER_PARTITION, "8");
    customProps.put(PUBSUB_INPUT_SPLIT_TIME_WINDOW_IN_MINUTES, "120");
    VeniceProperties props2 = new VeniceProperties(customProps);
    setupTopicManagerMocks(6, 4000L);

    List<PubSubPartitionSplit> splits2 = planner.plan(props2);

    assertNotNull(splits2);
    assertFalse(splits2.isEmpty());

    // Case 3: Minimal required properties
    Map<String, String> minimalProps = new HashMap<>();
    minimalProps.put(KAFKA_INPUT_TOPIC, TEST_TOPIC_NAME);
    minimalProps.put(KAFKA_INPUT_BROKER_URL, TEST_BROKER_URL);
    VeniceProperties props3 = new VeniceProperties(minimalProps);
    setupTopicManagerMocks(1, 500L);

    List<PubSubPartitionSplit> splits3 = planner.plan(props3);

    assertNotNull(splits3);
    assertFalse(splits3.isEmpty());
  }

  @Test
  public void testPlanWithEdgeCaseScenarios() {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());

    // Case 1: Single partition topic
    VeniceProperties props1 = createVeniceProperties(TEST_TOPIC_NAME, TEST_BROKER_URL);
    setupTopicManagerMocks(1, 1000L);

    List<PubSubPartitionSplit> splits1 = planner.plan(props1);

    assertNotNull(splits1);
    assertFalse(splits1.isEmpty());

    // Case 2: Many partitions (50) topic
    VeniceProperties props2 = createVeniceProperties(TEST_TOPIC_NAME, TEST_BROKER_URL);
    setupTopicManagerMocks(50, 2000L);

    List<PubSubPartitionSplit> splits2 = planner.plan(props2);

    assertNotNull(splits2);
    assertFalse(splits2.isEmpty());
    assertTrue(splits2.size() >= 50); // At least one split per partition

    // Case 3: Zero partition topic
    VeniceProperties props3 = createVeniceProperties(TEST_TOPIC_NAME, TEST_BROKER_URL);
    setupTopicManagerMocks(0, 0L);

    List<PubSubPartitionSplit> splits3 = planner.plan(props3);

    assertNotNull(splits3);
    assertTrue(splits3.isEmpty()); // No partitions means no splits
  }

  @Test
  public void testPlanWithErrorHandlingScenarios() {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());

    // Case 1: Missing KAFKA_INPUT_TOPIC in VeniceProperties
    Map<String, String> propsMap1 = new HashMap<>();
    propsMap1.put(KAFKA_INPUT_BROKER_URL, TEST_BROKER_URL);
    VeniceProperties props1 = new VeniceProperties(propsMap1);

    expectThrows(UndefinedPropertyException.class, () -> planner.plan(props1));

    // Case 2: Missing KAFKA_INPUT_BROKER_URL in VeniceProperties
    Map<String, String> propsMap2 = new HashMap<>();
    propsMap2.put(KAFKA_INPUT_TOPIC, TEST_TOPIC_NAME);
    VeniceProperties props2 = new VeniceProperties(propsMap2);

    expectThrows(UndefinedPropertyException.class, () -> planner.plan(props2));

    // Case 3: Invalid split type
    VeniceProperties props3 =
        createVenicePropertiesWithValues(TEST_TOPIC_NAME, TEST_BROKER_URL, "INVALID_SPLIT_TYPE", 1000L, 5, 60L);

    expectThrows(IllegalArgumentException.class, () -> planner.plan(props3));
  }

  @Test
  public void testPlanWithConfigurationVariations() {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());
    setupTopicManagerMocks(3, 1500L);

    // Case 1: Extreme values testing (very small/large)
    VeniceProperties props1 = createVenicePropertiesWithValues(
        TEST_TOPIC_NAME,
        TEST_BROKER_URL,
        PartitionSplitStrategy.FIXED_RECORD_COUNT.name(),
        1L,
        1,
        1L);

    List<PubSubPartitionSplit> splits1 = planner.plan(props1);

    assertNotNull(splits1);
    assertFalse(splits1.isEmpty());

    // Case 2: Large values testing (use reasonable large values to avoid overflow)
    VeniceProperties props2 = createVenicePropertiesWithValues(
        TEST_TOPIC_NAME,
        TEST_BROKER_URL,
        PartitionSplitStrategy.CAPPED_SPLIT_COUNT.name(),
        1000000L,
        10000,
        525600L);

    List<PubSubPartitionSplit> splits2 = planner.plan(props2);

    assertNotNull(splits2);
    assertFalse(splits2.isEmpty());

    // Case 3: Zero values testing (should use defaults)
    VeniceProperties props3 = createVenicePropertiesWithValues(
        TEST_TOPIC_NAME,
        TEST_BROKER_URL,
        PartitionSplitStrategy.SINGLE_SPLIT_PER_PARTITION.name(),
        0L,
        0,
        0L);

    List<PubSubPartitionSplit> splits3 = planner.plan(props3);

    assertNotNull(splits3);
    assertFalse(splits3.isEmpty());
  }

  @Test(dataProvider = "splitTypeProvider")
  public void testPlanWithAllSplitTypes(PartitionSplitStrategy partitionSplitStrategy) {
    doReturn(mockTopicManager).when(planner).createTopicManager(any(VeniceProperties.class), anyString());
    doReturn(3).when(mockTopicManager).getPartitionCount(any(PubSubTopic.class));

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
        { PartitionSplitStrategy.FIXED_RECORD_COUNT }, { PartitionSplitStrategy.CAPPED_SPLIT_COUNT } };
  }

  // Helper methods
  private VeniceProperties createVeniceProperties(String topicName, String brokerUrl) {
    Map<String, String> props = new HashMap<>();
    props.put(KAFKA_INPUT_TOPIC, topicName);
    props.put(KAFKA_INPUT_BROKER_URL, brokerUrl);
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
    props.put(KAFKA_INPUT_BROKER_URL, brokerUrl);
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
    when(topicManager.getPartitionCount(any(PubSubTopic.class))).thenReturn(partitionCount);

    for (int i = 0; i < partitionCount; i++) {
      PubSubPosition startPos = ApacheKafkaOffsetPosition.of(0L);
      PubSubPosition endPos = ApacheKafkaOffsetPosition.of(recordsPerPartition);

      when(topicManager.getStartPositionsForPartitionWithRetries(any(PubSubTopicPartition.class))).thenReturn(startPos);
      when(topicManager.getEndPositionsForPartitionWithRetries(any(PubSubTopicPartition.class))).thenReturn(endPos);
      when(topicManager.getNumRecordsInPartition(any(PubSubTopicPartition.class))).thenReturn(recordsPerPartition);
      when(
          topicManager
              .diffPosition(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(PubSubPosition.class)))
                  .thenReturn(recordsPerPartition);
      when(topicManager.advancePosition(any(PubSubTopicPartition.class), any(PubSubPosition.class), any(Long.class)))
          .thenReturn(endPos);
    }
  }
}
