package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestTopicCleanupService {
  private Admin admin;
  private TopicManager topicManager;
  private TopicCleanupService topicCleanupService;

  @BeforeMethod
  public void setup() {
    admin = mock(Admin.class);
    topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(admin).getTopicManager();
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(10l).when(config).getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    doReturn(1).when(config).getMinNumberOfUnusedKafkaTopicsToPreserve();
    topicCleanupService = new TopicCleanupService(admin, config);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    topicCleanupService.stop();
  }

  @Test
  public void testGetAllVeniceStoreTopics() {
    Map<String, Long> storeTopics = new HashMap<>();
    storeTopics.put("store1_v1", 1000l);
    storeTopics.put("store1_v2", 5000l);
    storeTopics.put("store1_v3", Long.MAX_VALUE);
    storeTopics.put("store1_rt", Long.MAX_VALUE);
    storeTopics.put("store2_v10", 5000l);
    storeTopics.put("store2_v11", Long.MAX_VALUE);
    storeTopics.put("non_venice_topic1", Long.MAX_VALUE);

    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();

    Map<String, Map<String, Long>> filteredStoreTopics = topicCleanupService.getAllVeniceStoreTopics();
    Assert.assertEquals(filteredStoreTopics.size(), 2);
    Assert.assertEquals(filteredStoreTopics.get("store1").size(), 4);
    Assert.assertEquals(filteredStoreTopics.get("store2").size(), 2);
  }

  @Test
  public void testExtractVeniceTopicsToCleanup() {
    // minNumberOfUnusedKafkaTopicsToPreserve = 1
    final long LOW_RETENTION_POLICY = 1000l;
    final long HIGH_RETENTION_POLICY = Long.MAX_VALUE;
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(LOW_RETENTION_POLICY);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(HIGH_RETENTION_POLICY);
    List<String> topics = Arrays.asList(
        "store1_v1",
        "store1_v2",
        "store1_v3",
        "store1_v4");
    Map<String, Long> topicRetentions1 = new HashMap<>();
    topicRetentions1.put("store1_v1", LOW_RETENTION_POLICY);
    topicRetentions1.put("store1_v2", LOW_RETENTION_POLICY);
    topicRetentions1.put("store1_v3", HIGH_RETENTION_POLICY);
    topicRetentions1.put("store1_v4", HIGH_RETENTION_POLICY);
    List<String> expectedResult1 = Arrays.asList("store1_v1", "store1_v2");
    List<String> actualResult1 = topicCleanupService.extractVeniceTopicsToCleanup(topicRetentions1);
    actualResult1.sort(String::compareTo);
    Assert.assertEquals(actualResult1, expectedResult1);

    Map<String, Long> topicRetentions2 = new HashMap<>();
    topicRetentions2.put("store1_v1", HIGH_RETENTION_POLICY);
    topicRetentions2.put("store1_v2", HIGH_RETENTION_POLICY);
    topicRetentions2.put("store1_v3", LOW_RETENTION_POLICY);
    topicRetentions2.put("store1_v4", LOW_RETENTION_POLICY);
    List<String> expectedResult2 = Arrays.asList("store1_v3");
    List<String> actualResult2 = topicCleanupService.extractVeniceTopicsToCleanup(topicRetentions2);
    actualResult2.sort(String::compareTo);
    Assert.assertEquals(actualResult2, expectedResult2);

    Map<String, Long> topicRetentions3 = new HashMap<>();
    topicRetentions3.put("store1_v1", LOW_RETENTION_POLICY);
    topicRetentions3.put("store1_v2", HIGH_RETENTION_POLICY);
    topicRetentions3.put("store1_v3", LOW_RETENTION_POLICY);
    topicRetentions3.put("store1_v4", HIGH_RETENTION_POLICY);
    List<String> expectedResult3 = Arrays.asList("store1_v1", "store1_v3");
    List<String> actualResult3 = topicCleanupService.extractVeniceTopicsToCleanup(topicRetentions3);
    actualResult3.sort(String::compareTo);
    Assert.assertEquals(actualResult3, expectedResult3);
  }

  @Test
  public void testCleanupVeniceTopics() {
    String storeName1 = TestUtils.getUniqueString("store1");
    Map<String, Long> storeTopics = new HashMap<>();
    storeTopics.put(storeName1 + "_v1", 1000l);
    storeTopics.put(storeName1 + "_v2", 1000l);
    storeTopics.put(storeName1 + "_v3", Long.MAX_VALUE);
    storeTopics.put(storeName1 + "_v4", 1000l);
    storeTopics.put(storeName1 + "_rt", Long.MAX_VALUE);
    storeTopics.put("non_venice_topic1", Long.MAX_VALUE);

    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_rt");
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v1");
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v2");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v4");

    // Updated real-time topic to use low retention policy
    storeTopics.put(storeName1 + "_rt", 1000l);
    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_rt");
  }

  @Test
  public void testRun() throws Exception {
    String storeName1 = TestUtils.getUniqueString("store1");
    String storeName2 = TestUtils.getUniqueString("store2");
    String storeName3 = TestUtils.getUniqueString("store3");
    Map<String, Long> storeTopics1 = new HashMap<>();
    storeTopics1.put(storeName1 + "_v1", 1000l);
    storeTopics1.put(storeName1 + "_v2", 1000l);
    storeTopics1.put(storeName1 + "_v3", Long.MAX_VALUE);
    storeTopics1.put(storeName1 + "_v4", 1000l);
    storeTopics1.put(storeName1 + "_rt", Long.MAX_VALUE);
    storeTopics1.put("non_venice_topic1", Long.MAX_VALUE);

    Map<String, Long> storeTopics2 = new HashMap<>();
    storeTopics2.put(storeName2 + "_v1", 1000l);
    storeTopics2.put(storeName2 + "_v2", 1000l);
    storeTopics2.put(storeName2 + "_v3", Long.MAX_VALUE);
    storeTopics2.put(storeName3 + "_v4", 1000l);
    storeTopics2.put(storeName3 + "_rt", 1000l);

    Map<String, Long> storeTopics3 = new HashMap<>();

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1).thenReturn(storeTopics2)
        .thenReturn(storeTopics3).thenReturn(new HashMap<>());

    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);
    doReturn(true).when(admin).isMasterControllerOfControllerCluster();
    // Resource is still alive
    doReturn(true).when(admin).isResourceStillAlive(storeName2 + "_v2");

    topicCleanupService.start();
    final int TOPIC_CLEANUP_TIMEOUT_IN_MS = 2000; // 2 seconds

    verify(topicManager, timeout(TOPIC_CLEANUP_TIMEOUT_IN_MS)).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v1");
    verify(topicManager, timeout(TOPIC_CLEANUP_TIMEOUT_IN_MS)).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v2");
    verify(topicManager, timeout(TOPIC_CLEANUP_TIMEOUT_IN_MS)).ensureTopicIsDeletedAndBlockWithRetry(storeName2 + "_v1");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName2 + "_v2");
    verify(topicManager, timeout(TOPIC_CLEANUP_TIMEOUT_IN_MS)).ensureTopicIsDeletedAndBlockWithRetry(storeName3 + "_rt");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v4");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_rt");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName2 + "_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName3 + "_v4");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");
  }

  @Test
  public void testRunWhenCurrentControllerChangeFromMasterToSlave() throws Exception {
    String storeName1 = TestUtils.getUniqueString("store1");
    Map<String, Long> storeTopics1 = new HashMap<>();
    storeTopics1.put(storeName1 + "_v1", 1000l);
    storeTopics1.put(storeName1 + "_v2", 1000l);
    storeTopics1.put(storeName1 + "_v3", Long.MAX_VALUE);
    storeTopics1.put(storeName1 + "_v4", 1000l);
    storeTopics1.put(storeName1 + "_rt", Long.MAX_VALUE);
    storeTopics1.put("non_venice_topic1", Long.MAX_VALUE);

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1);

    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);
    when(admin.isMasterControllerOfControllerCluster()).thenReturn(true).thenReturn(false);

    topicCleanupService.start();
    final int TOPIC_CLEANUP_TIMEOUT_IN_MS = 200; // 200 ms

    verify(topicManager, after(TOPIC_CLEANUP_TIMEOUT_IN_MS).never()).ensureTopicIsDeletedAndBlock(storeName1 + "_v1");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(storeName1 + "_v2");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(storeName1 + "_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(storeName1 + "_v4");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(storeName1 + "_rt");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock("non_venice_topic1");
  }

  @Test
  public void testRunWhenCurrentControllerChangeFromSlaveToMaster() throws Exception {
    String storeName1 = TestUtils.getUniqueString("store1");
    Map<String, Long> storeTopics1 = new HashMap<>();
    storeTopics1.put(storeName1 + "_v1", 1000l);
    storeTopics1.put(storeName1 + "_v2", 1000l);
    storeTopics1.put(storeName1 + "_v3", Long.MAX_VALUE);
    storeTopics1.put(storeName1 + "_v4", 1000l);
    storeTopics1.put(storeName1 + "_rt", Long.MAX_VALUE);
    storeTopics1.put("non_venice_topic1", Long.MAX_VALUE);

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1).thenReturn(new HashMap<>());

    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000l);
    when(admin.isMasterControllerOfControllerCluster()).thenReturn(false).thenReturn(true);

    topicCleanupService.start();
    final int TOPIC_CLEANUP_TIMEOUT_IN_MS = 200; // 200 ms

    verify(topicManager, timeout(TOPIC_CLEANUP_TIMEOUT_IN_MS)).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v1");
    verify(topicManager, timeout(TOPIC_CLEANUP_TIMEOUT_IN_MS)).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v2");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v3");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_v4");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(storeName1 + "_rt");
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry("non_venice_topic1");
  }
}
