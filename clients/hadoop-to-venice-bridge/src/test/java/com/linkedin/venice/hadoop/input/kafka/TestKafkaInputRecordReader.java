package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.hadoop.VenicePushJob.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestKafkaInputRecordReader {
  private static final String KAFKA_MESSAGE_KEY_PREFIX = "key_";
  private static final String KAFKA_MESSAGE_VALUE_PREFIX = "value_";

  private KafkaBrokerWrapper kafka;
  private TopicManager manager;
  private ZkServerWrapper zkServer;

  @BeforeClass
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    kafka = ServiceFactory.getKafkaBroker(zkServer);
    manager = new TopicManager(
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        100,
        24 * Time.MS_PER_HOUR,
        TestUtils.getVeniceConsumerFactory(kafka));
  }

  @AfterClass
  public void cleanUp() throws IOException {
    manager.close();
    kafka.close();
    zkServer.close();
  }

  public String getTopic(int numRecord, Pair<Integer, Integer> updateRange, Pair<Integer, Integer> deleteRange) {
    String topicName = Utils.getUniqueString("test_kafka_input_format");
    manager.createTopic(topicName, 1, 1, true);
    VeniceWriterFactory veniceWriterFactory = TestUtils.getVeniceWriterFactory(kafka.getAddress());
    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter = veniceWriterFactory.createBasicVeniceWriter(topicName)) {
      for (int i = 0; i < numRecord; ++i) {
        byte[] keyBytes = (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes();
        byte[] valueBytes = (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes();
        if (i >= updateRange.getFirst() && i <= updateRange.getSecond()) {
          veniceWriter.update(keyBytes, valueBytes, -1, -1, null);
        } else if (i >= deleteRange.getFirst() && i <= deleteRange.getSecond()) {
          veniceWriter.delete(keyBytes, null);
        } else {
          veniceWriter.put(keyBytes, valueBytes, -1);
        }
      }
    }

    return topicName;
  }

  @Test
  public void testNext() throws IOException {
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, kafka.getAddress());
    conf.set(VenicePushJob.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    String topic = getTopic(100, new Pair<>(-1, -1), new Pair<>(-1, -1));
    conf.set(KAFKA_INPUT_TOPIC, topic);

    KafkaInputRecordReader reader = new KafkaInputRecordReader(new KafkaInputSplit(topic, 0, 0, 102), conf, null);
    for (int i = 0; i < 100; ++i) {
      BytesWritable key = new BytesWritable();
      KafkaInputMapperValue value = new KafkaInputMapperValue();
      reader.next(key, value);
      Assert.assertEquals(key.copyBytes(), (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes());
      Assert.assertEquals(value.offset, i + 1);
      Assert.assertEquals(value.schemaId, -1);
      Assert.assertEquals(value.valueType, MapperValueType.PUT);
      Assert.assertEquals(ByteUtils.extractByteArray(value.value), (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes());
    }
  }

  @Test
  public void testNextWithDeleteMessage() throws IOException {
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, kafka.getAddress());
    String topic = getTopic(100, new Pair<>(-1, -1), new Pair<>(0, 10));
    conf.set(KAFKA_INPUT_TOPIC, topic);
    conf.set(VenicePushJob.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    KafkaInputRecordReader reader = new KafkaInputRecordReader(new KafkaInputSplit(topic, 0, 0, 102), conf, null);
    for (int i = 0; i < 100; ++i) {
      BytesWritable key = new BytesWritable();
      KafkaInputMapperValue value = new KafkaInputMapperValue();
      reader.next(key, value);
      Assert.assertEquals(key.copyBytes(), (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes());
      Assert.assertEquals(value.offset, i + 1);
      Assert.assertEquals(value.schemaId, -1);
      if (i <= 10) {
        // DELETE
        Assert.assertEquals(value.valueType, MapperValueType.DELETE);
      } else {
        // PUT
        Assert.assertEquals(value.valueType, MapperValueType.PUT);
        Assert.assertEquals(ByteUtils.extractByteArray(value.value), (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes());
      }
    }
  }

  @Test
  public void testNextWithUpdateMessage() throws IOException {
    JobConf conf = new JobConf();
    conf.set(KAFKA_INPUT_BROKER_URL, kafka.getAddress());
    String topic = getTopic(100, new Pair<>(21, 30), new Pair<>(11, 20));
    conf.set(KAFKA_INPUT_TOPIC, topic);
    conf.set(VenicePushJob.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, ChunkedKeySuffix.SCHEMA$.toString());
    KafkaInputRecordReader reader = new KafkaInputRecordReader(new KafkaInputSplit(topic, 0, 0, 102), conf, null);
    for (int i = 0; i < 100; ++i) {
      BytesWritable key = new BytesWritable();
      KafkaInputMapperValue value = new KafkaInputMapperValue();
      if (i == 21) {
        try {
          reader.next(key, value);
          Assert.fail("An IOException should be thrown here");
        } catch (IOException e) {
          Assert.assertTrue(e.getMessage().contains("Unexpected 'UPDATE' message"));
        }
        break;
      } else {
        reader.next(key, value);
      }
      Assert.assertEquals(key.copyBytes(), (KAFKA_MESSAGE_KEY_PREFIX + i).getBytes());
      Assert.assertEquals(value.offset, i + 1);
      Assert.assertEquals(value.schemaId, -1);
      if (i <= 10) {
        // PUT
        Assert.assertEquals(value.valueType, MapperValueType.PUT);
        Assert.assertEquals(ByteUtils.extractByteArray(value.value), (KAFKA_MESSAGE_VALUE_PREFIX + i).getBytes());
      } else if (i >= 11 && i <= 20) {
        // DELETE
        Assert.assertEquals(value.valueType, MapperValueType.DELETE);
      }
    }
  }
}
