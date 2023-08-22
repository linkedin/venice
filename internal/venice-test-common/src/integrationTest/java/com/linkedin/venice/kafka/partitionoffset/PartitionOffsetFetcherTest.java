package com.linkedin.venice.kafka.partitionoffset;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import java.util.Optional;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PartitionOffsetFetcherTest {
  private PubSubBrokerWrapper pubSubBrokerWrapper;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeClass
  public void setUp() {
    this.pubSubBrokerWrapper = ServiceFactory.getPubSubBroker();
  }

  @AfterClass
  public void close() {
    this.pubSubBrokerWrapper.close();
  }

  private final PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
      new KafkaValueSerializer(),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new),
      new LandFillObjectPool<>(KafkaMessageEnvelope::new));

  @Test
  public void testGetPartitionLatestOffsetAndRetry() {

    PubSubAdminAdapterFactory pubSubAdminAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getAdminAdapterFactory();
    PubSubConsumerAdapterFactory pubSubConsumerAdapterFactory =
        pubSubBrokerWrapper.getPubSubClientsFactory().getConsumerAdapterFactory();
    Properties properties = new Properties();
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try (PartitionOffsetFetcher fetcher = PartitionOffsetFetcherFactory.createDefaultPartitionOffsetFetcher(
        (c) -> pubSubConsumerAdapterFactory.create(
            new VeniceProperties(properties),
            false,
            pubSubMessageDeserializer,
            pubSubBrokerWrapper.getAddress()),
        (c) -> pubSubAdminAdapterFactory.create(new VeniceProperties(properties), pubSubTopicRepository),
        pubSubBrokerWrapper.getAddress(),
        Time.MS_PER_SECOND,
        Optional.empty())) {
      String topic = Utils.getUniqueString("topic") + "_v1";
      PubSubTopicPartition pubSubTopicPartition =
          new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), 0);
      Assert.assertThrows(
          PubSubTopicDoesNotExistException.class,
          () -> fetcher.getPartitionLatestOffsetAndRetry(pubSubTopicPartition, 1));
    }
  }
}
