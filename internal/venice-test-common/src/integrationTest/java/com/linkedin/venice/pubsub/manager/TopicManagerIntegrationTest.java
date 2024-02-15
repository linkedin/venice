package com.linkedin.venice.pubsub.manager;

import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.STANDALONE_REGION_NAME;

import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.VeniceProperties;
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
    pubSubBrokerWrapper = ServiceFactory.getPubSubBroker(
        new PubSubBrokerConfigs.Builder().setMockTime(mockTime).setRegionName(STANDALONE_REGION_NAME).build());
    topicManager = IntegrationTestPushUtils
        .getTopicManagerRepo(500L, 100L, MIN_COMPACTION_LAG, pubSubBrokerWrapper, new PubSubTopicRepository())
        .getLocalTopicManager();
  }

  protected PubSubProducerAdapter createPubSubProducerAdapter() {
    return pubSubBrokerWrapper.getPubSubClientsFactory()
        .getProducerAdapterFactory()
        .create(VeniceProperties.empty(), "topicManagerTestProducer", pubSubBrokerWrapper.getAddress());
  }

  @Test
  public void testRaceCondition() throws ExecutionException, InterruptedException {
    final PubSubTopic topic = getTopic();
    final PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(topic, 0);

    long timestamp = System.currentTimeMillis();
    produceRandomPubSubMessage(topic, true, timestamp); // This timestamp is expected to be retrieved
    produceRandomPubSubMessage(topic, false, timestamp + 1000L); // produce a control message

    long retrievedTimestamp = topicManager.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, 1);
    Assert.assertEquals(retrievedTimestamp, timestamp);

    // Produce more data records to this topic partition
    for (int i = 0; i < 100; i++) {
      timestamp += 1000L;
      produceRandomPubSubMessage(topic, true, timestamp);
    }
    // Produce several control messages at the end
    for (int i = 1; i <= 3; i++) {
      produceRandomPubSubMessage(topic, false, timestamp + i * 1000L);
    }
    long checkTimestamp = timestamp - 1000L;
    int numberOfThreads = 50;
    ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
    Future[] vwFutures = new Future[numberOfThreads];
    // Put all topic manager calls related to partition offset fetcher with admin and consumer here.
    Runnable[] tasks = { () -> topicManager.getOffsetByTime(pubSubTopicPartition, checkTimestamp),
        () -> topicManager.getProducerTimestampOfLastDataMessageWithRetries(pubSubTopicPartition, 1),
        () -> topicManager.getPartitionCount(topic),
        () -> topicManager.getLatestOffsetWithRetries(pubSubTopicPartition, 1),
        () -> topicManager.getTopicLatestOffsets(topic) };

    for (int i = 0; i < numberOfThreads; i++) {
      vwFutures[i] = executorService.submit(tasks[i % tasks.length]);
    }
    for (int i = 0; i < numberOfThreads; i++) {
      vwFutures[i].get();
    }
  }

}
