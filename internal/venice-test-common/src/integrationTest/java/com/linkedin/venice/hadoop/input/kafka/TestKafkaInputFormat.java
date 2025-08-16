package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.annotation.PubSubAgnosticTest;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubProducerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@PubSubAgnosticTest
public class TestKafkaInputFormat {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private TopicManager topicManager;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    topicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE,
                100L,
                24 * Time.MS_PER_HOUR,
                pubSubBrokerWrapper,
                pubSubTopicRepository)
            .getLocalTopicManager();
  }

  @AfterClass
  public void cleanUp() throws IOException {
    topicManager.close();
    pubSubBrokerWrapper.close();
  }

  public PubSubTopic getTopic(int numRecord, int numPartition) {
    PubSubTopic topic = pubSubTopicRepository.getTopic(Utils.getUniqueString("test_kafka_input_format") + "_v1");
    topicManager.createTopic(topic, numPartition, 1, true);
    PubSubProducerAdapterFactory pubSubProducerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getProducerAdapterFactory();
    VeniceWriterFactory veniceWriterFactory =
        IntegrationTestPushUtils.getVeniceWriterFactory(pubSubBrokerWrapper, pubSubProducerAdapterFactory);
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topic.getName()).build())) {
      for (int i = 0; i < numRecord; ++i) {
        veniceWriter.put((KAFKA_MESSAGE_KEY_PREFIX + i).getBytes(), (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes(), -1);
      }
    }

    return topic;
  }

  /**
   * Each element in {@param expectedSplit} should contain 3 objects:
   * int, start_offset, end_offset
   */
  private void getSplitAndValidate(
      KafkaInputFormat kafkaInputFormat,
      JobConf conf,
      long maxRecordsPerMapper,
      List<Object[]> expectedSplit) {
    if (maxRecordsPerMapper > 0) {
      conf.set(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, Long.toString(maxRecordsPerMapper));
    }
    InputSplit[] splits = kafkaInputFormat.getSplits(conf, 100);
    assertEquals(splits.length, expectedSplit.size());
    Arrays.stream(splits).forEach(split -> Assert.assertTrue(split instanceof KafkaInputSplit));
    KafkaInputSplit[] kafkaInputSplits = (KafkaInputSplit[]) splits;

    Arrays.sort(kafkaInputSplits, (o1, o2) -> {
      if (o1.getTopicPartition().getPartitionNumber() > o2.getTopicPartition().getPartitionNumber()) {
        return 1;
      } else if (o1.getTopicPartition().getPartitionNumber() < o2.getTopicPartition().getPartitionNumber()) {
        return -1;
      } else {
        // same partition, compare starting offset
        return (int) (topicManager
            .comparePosition(o1.getTopicPartition(), o1.getStartingOffset(), o2.getStartingOffset()));
      }
    });

    for (int i = 0; i < kafkaInputSplits.length; ++i) {
      KafkaInputSplit split = kafkaInputSplits[i];
      Object[] expected = expectedSplit.get(i);
      if (expected.length != 3) {
        throw new RuntimeException(
            "Invalid length of element in param: expectedSplit, and it should be 3, but got: " + expected.length);
      }
      int expectedPartition = (int) expected[0];
      PubSubPosition expectedStartOffset = (PubSubPosition) expected[1];
      PubSubPosition expectedEndOffset = (PubSubPosition) expected[2];
      assertEquals(split.getTopicPartition().getPartitionNumber(), expectedPartition);
      assertEquals(
          topicManager.comparePosition(split.getTopicPartition(), split.getStartingOffset(), expectedStartOffset),
          0,
          "Expected starting offset: " + expectedStartOffset + ", but got: " + split.getStartingOffset());
      assertEquals(
          topicManager.comparePosition(split.getTopicPartition(), split.getEndingOffset(), expectedEndOffset),
          0,
          "Expected ending offset: " + expectedEndOffset + ", but got: " + split.getEndingOffset());
    }
  }

  @Test
  public void testGetSplits() {
    KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();
    PubSubTopic topic = getTopic(1000, 3);
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, pubSubBrokerWrapper.getAddress());
    conf.set(KAFKA_INPUT_TOPIC, topic.getName());

    Map<PubSubTopicPartition, PubSubPosition> earliestOffsets =
        topicManager.getStartPositionsForTopicWithRetries(topic);
    Map<PubSubTopicPartition, PubSubPosition> latestOffsets = topicManager.getEndPositionsForTopicWithRetries(topic);
    PubSubTopicPartition partition0 = new PubSubTopicPartitionImpl(topic, 0);
    PubSubTopicPartition partition1 = new PubSubTopicPartitionImpl(topic, 1);
    PubSubTopicPartition partition2 = new PubSubTopicPartitionImpl(topic, 2);
    assertEquals(topicManager.getNumRecordsInPartition(partition0), 300);
    assertEquals(topicManager.getNumRecordsInPartition(partition1), 356);
    assertEquals(topicManager.getNumRecordsInPartition(partition2), 350);

    // Using Object[][] to hold { partitionId, startPosition, endPosition }
    List<Object[]> splitInfo = Arrays.asList(
        new Object[][] { { 0, earliestOffsets.get(partition0), latestOffsets.get(partition0) },
            { 1, earliestOffsets.get(partition1), latestOffsets.get(partition1) },
            { 2, earliestOffsets.get(partition2), latestOffsets.get(partition2) } });

    getSplitAndValidate(kafkaInputFormat, conf, -1, splitInfo);

    // Test with 100 records per split using advancePosition API
    List<Object[]> splitInfoWith100RecordsPerSplit = Arrays.asList(
        // Partition 0: 300 records -> 3 splits of 100 each
        new Object[] { 0, earliestOffsets.get(partition0),
            topicManager.advancePosition(partition0, earliestOffsets.get(partition0), 100) },
        new Object[] { 0, topicManager.advancePosition(partition0, earliestOffsets.get(partition0), 100),
            topicManager.advancePosition(partition0, earliestOffsets.get(partition0), 200) },
        new Object[] { 0, topicManager.advancePosition(partition0, earliestOffsets.get(partition0), 200),
            latestOffsets.get(partition0) },

        // Partition 1: 356 records -> 3 splits of 100 and 1 split of 56
        new Object[] { 1, earliestOffsets.get(partition1),
            topicManager.advancePosition(partition1, earliestOffsets.get(partition1), 100) },
        new Object[] { 1, topicManager.advancePosition(partition1, earliestOffsets.get(partition1), 100),
            topicManager.advancePosition(partition1, earliestOffsets.get(partition1), 200) },
        new Object[] { 1, topicManager.advancePosition(partition1, earliestOffsets.get(partition1), 200),
            topicManager.advancePosition(partition1, earliestOffsets.get(partition1), 300) },
        new Object[] { 1, topicManager.advancePosition(partition1, earliestOffsets.get(partition1), 300),
            latestOffsets.get(partition1) },

        // Partition 2: 350 records -> 3 splits of 100 and 1 split of 50
        new Object[] { 2, earliestOffsets.get(partition2),
            topicManager.advancePosition(partition2, earliestOffsets.get(partition2), 100) },
        new Object[] { 2, topicManager.advancePosition(partition2, earliestOffsets.get(partition2), 100),
            topicManager.advancePosition(partition2, earliestOffsets.get(partition2), 200) },
        new Object[] { 2, topicManager.advancePosition(partition2, earliestOffsets.get(partition2), 200),
            topicManager.advancePosition(partition2, earliestOffsets.get(partition2), 300) },
        new Object[] { 2, topicManager.advancePosition(partition2, earliestOffsets.get(partition2), 300),
            latestOffsets.get(partition2) });

    getSplitAndValidate(kafkaInputFormat, conf, 100, splitInfoWith100RecordsPerSplit);
  }
}
