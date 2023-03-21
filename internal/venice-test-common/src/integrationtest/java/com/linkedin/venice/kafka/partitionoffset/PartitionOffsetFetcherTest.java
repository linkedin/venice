package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Optional;
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

  @Test
  public void testGetPartitionLatestOffsetAndRetry() {

    PubSubAdminAdapterFactory pubSubAdminAdapterFactory = IntegrationTestPushUtils.getVeniceAdminFactory();
    try (PartitionOffsetFetcher fetcher = PartitionOffsetFetcherFactory.createDefaultPartitionOffsetFetcher(
        new ApacheKafkaConsumerAdapterFactory(),
        new VeniceProperties(),
        pubSubBrokerWrapper.getAddress(),
        Lazy.of(
            () -> pubSubAdminAdapterFactory.create(
                new VeniceProperties(),
                Optional.empty(),
                "admin_stats",
                pubSubTopicRepository,
                pubSubBrokerWrapper.getAddress())),
        Time.MS_PER_SECOND,
        Optional.empty())) {
      String topic = Utils.getUniqueString("topic") + "_v1";
      PubSubTopicPartition pubSubTopicPartition =
          new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), 0);
      Assert.assertThrows(
          TopicDoesNotExistException.class,
          () -> fetcher.getPartitionLatestOffsetAndRetry(pubSubTopicPartition, 1));
    }
  }
}
