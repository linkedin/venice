package com.linkedin.venice.kafka.partitionoffset;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
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

  @Test
  public void testGetPartitionLatestOffsetAndRetry() {

    PubSubAdminAdapterFactory pubSubAdminAdapterFactory = IntegrationTestPushUtils.getVeniceAdminFactory();
    Properties properties = new Properties();
    properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getAddress());
    try (PartitionOffsetFetcher fetcher = PartitionOffsetFetcherFactory.createDefaultPartitionOffsetFetcher(
        IntegrationTestPushUtils.getVeniceConsumerFactory(),
        new VeniceProperties(properties),
        pubSubBrokerWrapper.getAddress(),
        Lazy.of(() -> pubSubAdminAdapterFactory.create(new VeniceProperties(properties), pubSubTopicRepository)),
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
