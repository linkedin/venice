package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_OPERATION_TIMEOUT_MS_DEFAULT_VALUE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestKafkaInputFormat {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";

  private PubSubBrokerWrapper pubSubBrokerWrapper;
  private TopicManager manager;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
    manager =
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
    manager.close();
    pubSubBrokerWrapper.close();
  }

  public PubSubTopic getTopic(int numRecord, int numPartition) {
    PubSubTopic topic = pubSubTopicRepository.getTopic(Utils.getUniqueString("test_kafka_input_format") + "_v1");
    manager.createTopic(topic, numPartition, 1, true);
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
   * Each element in {@param expectedSplit} should contain 3 longs:
   * partition, start_offset, end_offset
   */
  private void getSplitAndValidate(
      KafkaInputFormat kafkaInputFormat,
      JobConf conf,
      long maxRecordsPerMapper,
      List<long[]> expectedSplit) throws IOException {
    if (maxRecordsPerMapper > 0) {
      conf.set(KAFKA_INPUT_MAX_RECORDS_PER_MAPPER, Long.toString(maxRecordsPerMapper));
    }
    InputSplit[] splits = kafkaInputFormat.getSplits(conf, 100);
    Assert.assertEquals(splits.length, expectedSplit.size());
    Arrays.stream(splits).forEach(split -> Assert.assertTrue(split instanceof KafkaInputSplit));
    KafkaInputSplit[] kafkaInputSplits = (KafkaInputSplit[]) splits;
    // todo(sushantmane): Update this test
    Arrays.sort(kafkaInputSplits, new Comparator<KafkaInputSplit>() {
      @Override
      public int compare(KafkaInputSplit o1, KafkaInputSplit o2) {
        if (o1.getTopicPartition().getPartitionNumber() > o2.getTopicPartition().getPartitionNumber()) {
          return 1;
        } else if (o1.getTopicPartition().getPartitionNumber() < o2.getTopicPartition().getPartitionNumber()) {
          return -1;
        } else {
          return (int) (o1.getStartingOffset().getNumericOffset() - o2.getStartingOffset().getNumericOffset());
        }
      }
    });
    for (int i = 0; i < kafkaInputSplits.length; ++i) {
      KafkaInputSplit split = kafkaInputSplits[i];
      long[] expected = expectedSplit.get(i);
      if (expected.length != 3) {
        throw new RuntimeException(
            "Invalid length of element in param: expectedSplit, and it should be 3, but got: " + expected.length);
      }
      long expectedPartition = expected[0];
      long expectedStartOffset = expected[1];
      long expectedEndOffset = expected[2];
      Assert.assertEquals(split.getTopicPartition().getPartitionNumber(), expectedPartition);
      Assert.assertEquals(split.getStartingOffset().getNumericOffset(), expectedStartOffset);
      Assert.assertEquals(split.getEndingOffset().getNumericOffset(), expectedEndOffset);
    }
  }

  @Test
  public void testGetSplits() throws IOException {
    KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();
    PubSubTopic topic = getTopic(1000, 3);
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, pubSubBrokerWrapper.getAddress());
    conf.set(KAFKA_INPUT_TOPIC, topic.getName());
    Map<PubSubTopicPartition, PubSubPosition> latestOffsets = kafkaInputFormat.getEndPositions(manager, topic);
    PubSubTopicPartition partition0 = new PubSubTopicPartitionImpl(topic, 0);
    PubSubTopicPartition partition1 = new PubSubTopicPartitionImpl(topic, 1);
    PubSubTopicPartition partition2 = new PubSubTopicPartitionImpl(topic, 2);
    Assert.assertEquals(latestOffsets.get(partition0).getNumericOffset(), 300);
    Assert.assertEquals(latestOffsets.get(partition1).getNumericOffset(), 356);
    Assert.assertEquals(latestOffsets.get(partition2).getNumericOffset(), 350);

    // Try to get splits with the default max records per mapper
    getSplitAndValidate(
        kafkaInputFormat,
        conf,
        -1,
        Arrays.asList(new long[][] { { 0, 0, 300 }, { 1, 0, 356 }, { 2, 0, 350 } }));
    // max records per mapper: 100
    getSplitAndValidate(
        kafkaInputFormat,
        conf,
        100,
        Arrays.asList(
            new long[][] { { 0, 0, 100 }, { 0, 100, 200 }, { 0, 200, 300 }, { 1, 0, 100 }, { 1, 100, 200 },
                { 1, 200, 300 }, { 1, 300, 356 }, { 2, 0, 100 }, { 2, 100, 200 }, { 2, 200, 300 }, { 2, 300, 350 } }));

  }
}
