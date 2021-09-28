package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static org.mockito.Mockito.*;


public class KafkaClusterBasedRecordThrottlerTest {
  @Test
  public void testRecordsCanBeThrottledPerRegion() throws ExecutionException, InterruptedException {
    String topic = TestUtils.getUniqueString("topic");
    InMemoryKafkaBroker inMemoryLocalKafkaBroker = new InMemoryKafkaBroker();
    inMemoryLocalKafkaBroker.createTopic(topic, 2);
    InMemoryKafkaBroker inMemoryRemoteKafkaBroker = new InMemoryKafkaBroker();
    inMemoryRemoteKafkaBroker.createTopic(topic, 2);

    Map<String, Object> extraServerProperties = new HashMap<>();
    extraServerProperties.put(SERVER_ENABLE_LIVE_CONFIG_BASED_KAFKA_THROTTLING, true);

    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, inMemoryLocalKafkaBroker.getKafkaBootstrapServer());

    AtomicLong remoteKafkaQuota = new AtomicLong(10);

    MockTime testTime = new MockTime();
    long timeWindowMS = 1000L;
    // Unlimited
    EventThrottler localThrottler = new EventThrottler(testTime, -1, timeWindowMS, "local_throttler", true,
        EventThrottler.REJECT_STRATEGY);

    // Modifiable remote throttler
    EventThrottler remoteThrottler = new EventThrottler(testTime, remoteKafkaQuota::get, timeWindowMS, "remote_throttler", true,
        EventThrottler.REJECT_STRATEGY);

    Map<String, EventThrottler> kafkaUrlToRecordsThrottler = new HashMap<>();
    kafkaUrlToRecordsThrottler.put(inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), localThrottler);
    kafkaUrlToRecordsThrottler.put(inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), remoteThrottler);

    KafkaClusterBasedRecordThrottler kafkaClusterBasedRecordThrottler = new KafkaClusterBasedRecordThrottler(kafkaUrlToRecordsThrottler);

    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = mock(ConsumerRecords.class);
    doReturn(10).when(consumerRecords).count();

    KafkaConsumerWrapper localConsumer = mock(KafkaConsumerWrapper.class);
    KafkaConsumerWrapper remoteConsumer = mock(KafkaConsumerWrapper.class);

    // Assume consumer.poll always returns some records
    doReturn(consumerRecords).when(localConsumer).poll(anyLong());
    doReturn(consumerRecords).when(remoteConsumer).poll(anyLong());

    // Verify can ingest at least some record from local and remote Kafka
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> localConsumerRecords = kafkaClusterBasedRecordThrottler.poll(localConsumer, inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), 1 * Time.MS_PER_SECOND);
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> remoteConsumerRecords = kafkaClusterBasedRecordThrottler.poll(remoteConsumer, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), 1 * Time.MS_PER_SECOND);
    Assert.assertSame(localConsumerRecords, consumerRecords);
    Assert.assertSame(remoteConsumerRecords, consumerRecords);

    // Pause remote kafka consumption
    remoteKafkaQuota.set(0);

    // Verify does not ingest from remote Kafka
    localConsumerRecords = kafkaClusterBasedRecordThrottler.poll(localConsumer, inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), 1 * Time.MS_PER_SECOND);
    remoteConsumerRecords = kafkaClusterBasedRecordThrottler.poll(remoteConsumer, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), 1 * Time.MS_PER_SECOND);
    Assert.assertSame(localConsumerRecords, consumerRecords);
    Assert.assertNotSame(remoteConsumerRecords, consumerRecords);
    Assert.assertTrue(remoteConsumerRecords.isEmpty());

    // Resume remote Kafka consumption
    remoteKafkaQuota.set(10);
    testTime.sleep(timeWindowMS); // sleep so throttling window is reset and we don't run into race conditions

    // Verify resumes ingestion from remote Kafka
    localConsumerRecords = kafkaClusterBasedRecordThrottler.poll(localConsumer, inMemoryLocalKafkaBroker.getKafkaBootstrapServer(), 1 * Time.MS_PER_SECOND);
    remoteConsumerRecords = kafkaClusterBasedRecordThrottler.poll(remoteConsumer, inMemoryRemoteKafkaBroker.getKafkaBootstrapServer(), 1 * Time.MS_PER_SECOND);
    Assert.assertSame(localConsumerRecords, consumerRecords);
    Assert.assertSame(remoteConsumerRecords, consumerRecords);
  }
}
