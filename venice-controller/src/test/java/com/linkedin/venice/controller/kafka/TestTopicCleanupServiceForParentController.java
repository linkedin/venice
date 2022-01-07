package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.kafka.TopicManager;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestTopicCleanupServiceForParentController {
  private Admin admin;
  private TopicManager topicManager;
  private TopicCleanupService topicCleanupService;

  @BeforeTest
  public void setUp() {
    admin = mock(Admin.class);
    topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(admin).getTopicManager();
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(1000l).when(config).getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    doReturn(2).when(config).getTopicCleanupDelayFactor();
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

    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager).ensureTopicIsDeletedAndBlockWithRetry("store1_rt");
    verify(topicManager).ensureTopicIsDeletedAndBlockWithRetry("store1_v1");
    verify(topicManager).ensureTopicIsDeletedAndBlockWithRetry("store1_v2");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("store1_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");
  }
}
