package com.linkedin.venice.kafka;

import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapter;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestMockTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class TopicManagerIntegrationTest extends TopicManagerTest {
  private PubSubBrokerWrapper pubSubBrokerWrapper;

  @AfterClass
  public void tearDown() {
    pubSubBrokerWrapper.close();
    topicManager.close();
  }

  @Override
  protected void createTopicManager() {
    TestMockTime mockTime = new TestMockTime();
    pubSubBrokerWrapper =
        ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setMockTime(mockTime).build());
    topicManager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                500L,
                100L,
                MIN_COMPACTION_LAG,
                pubSubBrokerWrapper.getAddress(),
                new PubSubTopicRepository())
            .getTopicManager();
  }

  protected PubSubProducerAdapter createPubSubProducerAdapter() {
    Properties props = new Properties();
    props.put(ApacheKafkaProducerConfig.KAFKA_KEY_SERIALIZER, KafkaKeySerializer.class.getName());
    props.put(ApacheKafkaProducerConfig.KAFKA_VALUE_SERIALIZER, KafkaValueSerializer.class.getName());
    props.put(ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    return new ApacheKafkaProducerAdapter(new ApacheKafkaProducerConfig(props));
  }

  @Test
  public void testRaceCondition() throws ExecutionException, InterruptedException {
    final PubSubTopic topic = getTopic();
    final PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 0);

    long timestamp = System.currentTimeMillis();
    produceToKafka(topic, true, timestamp); // This timestamp is expected to be retrieved
    produceToKafka(topic, false, timestamp + 1000L); // produce a control message

    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataRecord(pubSubTopicPartition, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);

    // Produce more data records to this topic partition
    for (int i = 0; i < 100; i++) {
      timestamp += 1000L;
      produceToKafka(topic, true, timestamp);
    }
    // Produce several control messages at the end
    for (int i = 1; i <= 3; i++) {
      produceToKafka(topic, false, timestamp + i * 1000L);
    }
    long checkTimestamp = timestamp - 1000L;
    int numberOfThreads = 50;
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    Future[] vwFutures = new Future[numberOfThreads];
    // Put all topic manager calls related to partition offset fetcher with admin and consumer here.
    Runnable[] tasks = { () -> topicManager.getPartitionOffsetByTime(pubSubTopicPartition, checkTimestamp),
        () -> topicManager.getProducerTimestampOfLastDataRecord(pubSubTopicPartition, 1),
        () -> topicManager.partitionsFor(topic),
        () -> topicManager.getPartitionEarliestOffsetAndRetry(pubSubTopicPartition, 1),
        () -> topicManager.getPartitionLatestOffsetAndRetry(pubSubTopicPartition, 1),
        () -> topicManager.getTopicLatestOffsets(topic) };

    for (int i = 0; i < numberOfThreads; i++) {
      vwFutures[i] = executorService.submit(tasks[i % tasks.length]);
    }
    for (int i = 0; i < numberOfThreads; i++) {
      vwFutures[i].get();
    }
  }

}
