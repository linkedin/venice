package com.linkedin.venice.replication;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.BrooklinWrapper;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBrooklin {

  @Test
  public void canReplicateKafkaWithBrooklinTopicReplicator() throws InterruptedException {
    String dummyVeniceClusterName = TestUtils.getUniqueString("venice");
    KafkaBrokerWrapper kafka = ServiceFactory.getKafkaBroker();
    BrooklinWrapper brooklin = ServiceFactory.getBrooklinWrapper(kafka);
    TopicManager topicManager = new TopicManager(kafka.getZkAddress());
    TopicReplicator
        replicator = new BrooklinTopicReplicator(brooklin.getBrooklinDmsUri(), kafka.getAddress(), topicManager, dummyVeniceClusterName, "venice-test-service");

    //Create topics
    int partitionCount = 1;
    String sourceTopic = TestUtils.getUniqueString("source");
    String destinationTopic = TestUtils.getUniqueString("destination");
    topicManager.createTopic(sourceTopic, partitionCount, 1, true);
    topicManager.createTopic(destinationTopic, partitionCount, 1, true);

    byte[] key = TestUtils.getUniqueString("key").getBytes(StandardCharsets.UTF_8);
    byte[] value = TestUtils.getUniqueString("value").getBytes(StandardCharsets.UTF_8);

    //Produce in source topic
    Producer<byte[], byte[]> producer = getKafkaProducer(kafka);
    producer.send(new ProducerRecord<>(sourceTopic, key, value));
    producer.flush();
    producer.close();

    //get starting offsets
    String brokerConnection = kafka.getAddress();
    Map<Integer, Long> startingSourceOffsets = new HashMap<>();
    for (int p=0;p<partitionCount;p++) {
      startingSourceOffsets.put(p, 0L);
    }

    //create replication stream
    try {
      replicator.beginReplication(sourceTopic, destinationTopic, Optional.of(startingSourceOffsets));
    } catch (TopicReplicator.TopicException e) {
      throw new VeniceException(e);
    }

    //check destination topic for records
    String consumeTopic = destinationTopic;
    KafkaConsumer<byte[],byte[]> consumer = getKafkaConsumer(kafka);
    List<TopicPartition> allPartitions = new ArrayList<>();
    for (int p=0;p<consumer.partitionsFor(consumeTopic).size();p++){
      allPartitions.add(new TopicPartition(consumeTopic, p));
    }
    consumer.assign(allPartitions);
    consumer.seekToBeginning(allPartitions);
    List<ConsumerRecord<byte[], byte[]>> buffer = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
      if (records.isEmpty() && buffer.size() > 0){
        break;
      }
      if (System.currentTimeMillis() - startTime > TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS)){
        throw new RuntimeException("Test timed out waiting for messages to appear in destination topic after 30 seconds");
      }
      for (ConsumerRecord<byte[], byte[]> record : records) {
        buffer.add(record);
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e){
        break;
      }
    }

    Assert.assertEquals(buffer.get(0).key(), key);
    Assert.assertEquals(buffer.get(0).value(), value);
  }

  private static Producer<byte[], byte[]> getKafkaProducer(KafkaBrokerWrapper kafka){
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    Producer<byte[], byte[]> producer = new KafkaProducer<>(props);
    return producer;
  }

  private static KafkaConsumer<byte[],byte[]> getKafkaConsumer(KafkaBrokerWrapper kafka){
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getAddress());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, TestUtils.getUniqueString("test"));
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[],byte[]> consumer = new KafkaConsumer<>(props);
    return consumer;
  }


}
