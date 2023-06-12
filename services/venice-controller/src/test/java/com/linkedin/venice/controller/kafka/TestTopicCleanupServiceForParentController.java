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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestTopicCleanupServiceForParentController {
  private Admin admin;
  private TopicManager topicManager;
  private TopicCleanupService topicCleanupService;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeTest
  public void setUp() {
    admin = mock(Admin.class);
    topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(admin).getTopicManager();
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(1000l).when(config).getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    doReturn(2).when(config).getTopicCleanupDelayFactor();
    topicCleanupService = new TopicCleanupServiceForParentController(admin, config, pubSubTopicRepository);
  }

  @Test
  public void testCleanupVeniceTopics() throws ExecutionException {
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    PubSubTopic store1V1 = pubSubTopicRepository.getTopic("store1_v1");
    PubSubTopic store1V2 = pubSubTopicRepository.getTopic("store1_v2");
    PubSubTopic store1V3 = pubSubTopicRepository.getTopic("store1_v3");
    PubSubTopic store1RT = pubSubTopicRepository.getTopic("store1_rt");
    PubSubTopic nonVeniceTopic1 = pubSubTopicRepository.getTopic("non_venice_topic1_v1");
    storeTopics.put(store1V1, 1000l);
    storeTopics.put(store1V2, 1000l);
    storeTopics.put(store1V3, Long.MAX_VALUE);
    storeTopics.put(store1RT, 1000l);

    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1RT);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1V1);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1V2);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1V3);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(nonVeniceTopic1);

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1RT);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1V1);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1V2);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1V3);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(nonVeniceTopic1);

    topicCleanupService.cleanupVeniceTopics();
    verify(topicManager).ensureTopicIsDeletedAndBlockWithRetry(store1RT);
    verify(topicManager).ensureTopicIsDeletedAndBlockWithRetry(store1V1);
    verify(topicManager).ensureTopicIsDeletedAndBlockWithRetry(store1V2);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(store1V3);
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(nonVeniceTopic1);
  }
}
