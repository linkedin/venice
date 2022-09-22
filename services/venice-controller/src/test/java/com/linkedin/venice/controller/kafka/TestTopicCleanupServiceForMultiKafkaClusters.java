package com.linkedin.venice.controller.kafka;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.Pair;
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
    Map<String, String> kafkaZkMap = new HashMap<>();
    kafkaZkMap.put(kafkaClusterKey1, kafkaClusterServerZk1);
    kafkaZkMap.put(kafkaClusterKey2, kafkaClusterServerZk2);
    doReturn(parentFabrics).when(config).getParentFabrics();
    doReturn(kafkaUrlMap).when(config).getChildDataCenterKafkaUrlMap();
    doReturn(kafkaZkMap).when(config).getChildDataCenterKafkaZkMap();

    admin = mock(Admin.class);
    topicManager1 = mock(TopicManager.class);
    doReturn(kafkaClusterServerUrl1).when(topicManager1).getKafkaBootstrapServers();
    doReturn(topicManager1).when(admin).getTopicManager(Pair.create(kafkaClusterServerUrl1, kafkaClusterServerZk1));
    topicManager2 = mock(TopicManager.class);
    doReturn(kafkaClusterServerUrl2).when(topicManager2).getKafkaBootstrapServers();
    doReturn(topicManager2).when(admin).getTopicManager(Pair.create(kafkaClusterServerUrl2, kafkaClusterServerZk2));

    topicCleanupService = new TopicCleanupServiceForParentController(admin, config);
  }

  @Test
  public void testCleanupVeniceTopics() throws ExecutionException {
    Map<String, Long> storeTopics = new HashMap<>();
    storeTopics.put("store1_v1", 1000l);
    storeTopics.put("store1_v2", 1000l);
    storeTopics.put("store1_v3", Long.MAX_VALUE);
    storeTopics.put("store1_rt", 1000l);
    storeTopics.put("non_venice_topic1", 1000l);

    doReturn(storeTopics).when(topicManager1).getAllTopicRetentions();
    doReturn(storeTopics).when(topicManager2).getAllTopicRetentions();
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);

    /**
     * Truncated topics in parent fabrics will not be deleted in the first 2 iterations.
     */
    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager1).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager1).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager1).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager1, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");
    verify(topicManager2).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager2).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager2).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager2, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");
  }
}
