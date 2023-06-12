package com.linkedin.venice.controller.kafka;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestTopicCleanupServiceForMultiKafkaClusters {
  private Admin admin;
  private TopicManager topicManager1;
  private TopicManager topicManager2;
  private TopicCleanupServiceForParentController topicCleanupService;

  private PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeTest
  public void setUp() {
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(1000l).when(config).getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    doReturn(2).when(config).getTopicCleanupDelayFactor();

    String kafkaClusterKey1 = "fabric1";
    String kafkaClusterKey2 = "fabric2";
    String kafkaClusterServerUrl1 = "host1";
    String kafkaClusterServerUrl2 = "host2";
    String kafkaClusterServerZk1 = "zk1";
    String kafkaClusterServerZk2 = "zk2";
    Set<String> parentFabrics = new HashSet<>();
    parentFabrics.add(kafkaClusterKey1);
    parentFabrics.add(kafkaClusterKey2);
    Map<String, String> kafkaUrlMap = new HashMap<>();
    kafkaUrlMap.put(kafkaClusterKey1, kafkaClusterServerUrl1);
    kafkaUrlMap.put(kafkaClusterKey2, kafkaClusterServerUrl2);
    doReturn(parentFabrics).when(config).getParentFabrics();
    doReturn(kafkaUrlMap).when(config).getChildDataCenterKafkaUrlMap();

    admin = mock(Admin.class);
    topicManager1 = mock(TopicManager.class);
    doReturn(kafkaClusterServerUrl1).when(topicManager1).getKafkaBootstrapServers();
    doReturn(topicManager1).when(admin).getTopicManager(kafkaClusterServerUrl1);
    topicManager2 = mock(TopicManager.class);
    doReturn(kafkaClusterServerUrl2).when(topicManager2).getKafkaBootstrapServers();
    doReturn(topicManager2).when(admin).getTopicManager(kafkaClusterServerUrl2);

    topicCleanupService = new TopicCleanupServiceForParentController(admin, config, pubSubTopicRepository);
  }

  @Test
  public void testCleanupVeniceTopics() throws ExecutionException {
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(pubSubTopicRepository.getTopic("store1_v1"), 1000l);
    storeTopics.put(pubSubTopicRepository.getTopic("store1_v2"), 1000l);
    storeTopics.put(pubSubTopicRepository.getTopic("store1_v3"), Long.MAX_VALUE);
    storeTopics.put(pubSubTopicRepository.getTopic("store1_rt"), 1000l);
    // storeTopics.put("non_venice_topic1", 1000l);

    doReturn(storeTopics).when(topicManager1).getAllTopicRetentions();
    doReturn(storeTopics).when(topicManager2).getAllTopicRetentions();
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);

    /**
     * Truncated topics in parent fabrics will not be deleted in the first 2 iterations.
     */
    topicCleanupService.cleanupVeniceTopics();

    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_rt", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v1", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v2", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v3", false);

    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_rt", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v1", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v2", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v3", false);

    topicCleanupService.cleanupVeniceTopics();

    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_rt", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v1", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v2", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v3", false);

    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_rt", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v1", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v2", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v3", false);

    topicCleanupService.cleanupVeniceTopics();
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_rt", true);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v1", true);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v2", true);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager1, "store1_v3", false);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_rt", true);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v1", true);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v2", true);
    verifyEnsureTopicIsDeletedAndBlockWithRetry(topicManager2, "store1_v3", false);
  }

  private void verifyEnsureTopicIsDeletedAndBlockWithRetry(
      TopicManager topicManager,
      String topicName,
      boolean happened) throws ExecutionException {
    if (happened) {
      verify(topicManager).ensureTopicIsDeletedAndBlockWithRetry(pubSubTopicRepository.getTopic(topicName));
    } else {
      verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(pubSubTopicRepository.getTopic(topicName));
    }
  }
}
