package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_MAX_RECORDS_PER_MAPPER;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
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
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestKafkaInputFormat {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";

  private PubSubBrokerWrapper kafka;
  private TopicManager manager;
  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    kafka = ServiceFactory.getPubSubBroker();
    manager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
                100L,
                24 * Time.MS_PER_HOUR,
                kafka.getAddress(),
                pubSubTopicRepository)
            .getTopicManager();
  }

  @AfterClass
  public void cleanUp() throws IOException {
    manager.close();
    kafka.close();
  }

  public String getTopic(int numRecord, int numPartition) {
    String topicName = Utils.getUniqueString("test_kafka_input_format") + "_v1";
    manager.createTopic(pubSubTopicRepository.getTopic(topicName), numPartition, 1, true);
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafka.getAddress());
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter =
        veniceWriterFactory.createVeniceWriter(new VeniceWriterOptions.Builder(topicName).build())) {
      for (int i = 0; i < numRecord; ++i) {
        veniceWriter.put((KAFKA_MESSAGE_KEY_PREFIX + i).getBytes(), (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes(), -1);
      }
    }

    return topicName;
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
    Arrays.sort(kafkaInputSplits, new Comparator<KafkaInputSplit>() {
      @Override
      public int compare(KafkaInputSplit o1, KafkaInputSplit o2) {
        if (o1.getTopicPartition().partition() > o2.getTopicPartition().partition()) {
          return 1;
        } else if (o1.getTopicPartition().partition() < o2.getTopicPartition().partition()) {
          return -1;
        } else {
          return (int) (o1.getStartingOffset() - o2.getStartingOffset());
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
      Assert.assertEquals(split.getTopicPartition().partition(), expectedPartition);
      Assert.assertEquals(split.getStartingOffset(), expectedStartOffset);
      Assert.assertEquals(split.getEndingOffset(), expectedEndOffset);
    }
  }

  @Test
  public void testGetSplits() throws IOException {
    KafkaInputFormat kafkaInputFormat = new KafkaInputFormat();
    String topic = getTopic(1000, 3);
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, kafka.getAddress());
    conf.set(KAFKA_INPUT_TOPIC, topic);
    Map<TopicPartition, Long> latestOffsets = kafkaInputFormat.getLatestOffsets(conf);
    TopicPartition partition0 = new TopicPartition(topic, 0);
    TopicPartition partition1 = new TopicPartition(topic, 1);
    TopicPartition partition2 = new TopicPartition(topic, 2);
    Assert.assertEquals(latestOffsets.get(partition0).longValue(), 300);
    Assert.assertEquals(latestOffsets.get(partition1).longValue(), 356);
    Assert.assertEquals(latestOffsets.get(partition2).longValue(), 350);

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
