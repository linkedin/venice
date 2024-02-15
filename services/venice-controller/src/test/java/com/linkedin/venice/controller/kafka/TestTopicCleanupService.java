package com.linkedin.venice.controller.kafka;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.stats.TopicCleanupServiceStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestTopicCleanupService {
  private Admin admin;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;
  private TopicManager topicManager;
  private TopicManager remoteTopicManager;
  private TopicCleanupService topicCleanupService;
  private VeniceControllerMultiClusterConfig veniceControllerMultiClusterConfig;
  private TopicCleanupServiceStats topicCleanupServiceStats;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    storeConfigRepository = mock(HelixReadOnlyStoreConfigRepository.class);
    doReturn(storeConfigRepository).when(admin).getStoreConfigRepo();
    StoreConfig mockExistentStoreConfig = mock(StoreConfig.class);
    doReturn(Optional.of(mockExistentStoreConfig)).when(storeConfigRepository).getStoreConfig("existent_store");
    topicManager = mock(TopicManager.class);
    doReturn(topicManager).when(admin).getTopicManager();
    veniceControllerMultiClusterConfig = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(0L).when(veniceControllerMultiClusterConfig).getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    doReturn(1).when(veniceControllerMultiClusterConfig).getMinNumberOfUnusedKafkaTopicsToPreserve();
    doReturn(1).when(admin).getMinNumberOfUnusedKafkaTopicsToPreserve();

    VeniceControllerConfig veniceControllerConfig = mock(VeniceControllerConfig.class);
    doReturn(veniceControllerConfig).when(veniceControllerMultiClusterConfig).getCommonConfig();
    doReturn("local,remote").when(veniceControllerConfig).getChildDatacenters();
    Map<String, String> dataCenterToBootstrapServerMap = new HashMap<>();
    dataCenterToBootstrapServerMap.put("local", "local");
    dataCenterToBootstrapServerMap.put("remote", "remote");
    doReturn(dataCenterToBootstrapServerMap).when(veniceControllerMultiClusterConfig).getChildDataCenterKafkaUrlMap();
    doReturn("local").when(topicManager).getPubSubClusterAddress();
    remoteTopicManager = mock(TopicManager.class);
    doReturn(remoteTopicManager).when(admin).getTopicManager("remote");
    doReturn(Collections.emptyMap()).when(remoteTopicManager).getAllTopicRetentions();
    topicCleanupServiceStats = mock(TopicCleanupServiceStats.class);

    topicCleanupService = new TopicCleanupService(
        admin,
        veniceControllerMultiClusterConfig,
        pubSubTopicRepository,
        topicCleanupServiceStats);
  }

  @AfterMethod
  public void cleanUp() throws Exception {
    topicCleanupService.stop();
  }

  @Test
  public void testGetAllVeniceStoreTopics() {
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(getPubSubTopic("store1_v1", ""), 1000L);
    storeTopics.put(getPubSubTopic("store1_v2", ""), 5000L);
    storeTopics.put(getPubSubTopic("store1_v3", ""), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic("store1_rt", ""), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic("store2_v10", ""), 5000L);
    storeTopics.put(getPubSubTopic("store2_v11", ""), Long.MAX_VALUE);

    Map<String, Map<PubSubTopic, Long>> filteredStoreTopics =
        TopicCleanupService.getAllVeniceStoreTopicsRetentions(storeTopics);
    Assert.assertEquals(filteredStoreTopics.size(), 2);
    Assert.assertEquals(filteredStoreTopics.get("store1").size(), 4);
    Assert.assertEquals(filteredStoreTopics.get("store2").size(), 2);
  }

  @Test
  public void testExtractVeniceTopicsToCleanup() {
    final long LOW_RETENTION_POLICY = 1000L;
    final long HIGH_RETENTION_POLICY = Long.MAX_VALUE;
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(LOW_RETENTION_POLICY);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(HIGH_RETENTION_POLICY);
    Map<PubSubTopic, Long> topicRetentions1 = new HashMap<>();
    topicRetentions1.put(pubSubTopicRepository.getTopic("store1_v1"), LOW_RETENTION_POLICY);
    topicRetentions1.put(pubSubTopicRepository.getTopic("store1_v2"), LOW_RETENTION_POLICY);
    topicRetentions1.put(pubSubTopicRepository.getTopic("store1_v3"), HIGH_RETENTION_POLICY);
    topicRetentions1.put(pubSubTopicRepository.getTopic("store1_v4"), HIGH_RETENTION_POLICY);
    List<String> expectedResult1 = Arrays.asList("store1_v1", "store1_v2");
    List<String> actualResult1 = TopicCleanupService
        .extractVersionTopicsToCleanup(admin, topicRetentions1, admin.getMinNumberOfUnusedKafkaTopicsToPreserve(), 0)
        .stream()
        .map(PubSubTopic::getName)
        .collect(Collectors.toList());
    actualResult1.sort(String::compareTo);
    Assert.assertEquals(actualResult1, expectedResult1);

    Map<PubSubTopic, Long> topicRetentions2 = new HashMap<>();
    topicRetentions2.put(pubSubTopicRepository.getTopic("store1_v1"), HIGH_RETENTION_POLICY);
    topicRetentions2.put(pubSubTopicRepository.getTopic("store1_v2"), HIGH_RETENTION_POLICY);
    topicRetentions2.put(pubSubTopicRepository.getTopic("store1_v3"), LOW_RETENTION_POLICY);
    topicRetentions2.put(pubSubTopicRepository.getTopic("store1_v4"), LOW_RETENTION_POLICY);
    List<String> expectedResult2 = Arrays.asList("store1_v3", "store1_v4");
    List<String> actualResult2 = TopicCleanupService
        .extractVersionTopicsToCleanup(admin, topicRetentions2, admin.getMinNumberOfUnusedKafkaTopicsToPreserve(), 0)
        .stream()
        .map(PubSubTopic::getName)
        .collect(Collectors.toList());
    actualResult2.sort(String::compareTo);
    Assert.assertEquals(actualResult2, expectedResult2);

    Map<PubSubTopic, Long> topicRetentions3 = new HashMap<>();
    topicRetentions3.put(pubSubTopicRepository.getTopic("store1_v1"), LOW_RETENTION_POLICY);
    topicRetentions3.put(pubSubTopicRepository.getTopic("store1_v2"), HIGH_RETENTION_POLICY);
    topicRetentions3.put(pubSubTopicRepository.getTopic("store1_v3"), LOW_RETENTION_POLICY);
    topicRetentions3.put(pubSubTopicRepository.getTopic("store1_v4"), HIGH_RETENTION_POLICY);
    List<String> expectedResult3 = Arrays.asList("store1_v1", "store1_v3");
    List<String> actualResult3 = TopicCleanupService
        .extractVersionTopicsToCleanup(admin, topicRetentions3, admin.getMinNumberOfUnusedKafkaTopicsToPreserve(), 0)
        .stream()
        .map(PubSubTopic::getName)
        .collect(Collectors.toList());
    actualResult3.sort(String::compareTo);
    Assert.assertEquals(actualResult3, expectedResult3);

    // Test minNumberOfUnusedKafkaTopicsToPreserve = 1 for regular store topics and zk shared system store topics
    Map<PubSubTopic, Long> topicRetentions4 = new HashMap<>();
    topicRetentions4.put(pubSubTopicRepository.getTopic("existent_store_v1"), LOW_RETENTION_POLICY);
    topicRetentions4.put(pubSubTopicRepository.getTopic("existent_store_v2"), LOW_RETENTION_POLICY);
    topicRetentions4.put(pubSubTopicRepository.getTopic("existent_store_v3"), LOW_RETENTION_POLICY);
    topicRetentions4.put(pubSubTopicRepository.getTopic("existent_store_v4"), LOW_RETENTION_POLICY);
    List<String> expectedResult4 = Arrays.asList("existent_store_v1", "existent_store_v2", "existent_store_v3");
    List<String> actualResult4 = TopicCleanupService
        .extractVersionTopicsToCleanup(admin, topicRetentions4, admin.getMinNumberOfUnusedKafkaTopicsToPreserve(), 0)
        .stream()
        .map(PubSubTopic::getName)
        .collect(Collectors.toList());
    actualResult4.sort(String::compareTo);
    Assert.assertEquals(actualResult4, expectedResult4);
  }

  @Test
  public void testCleanupVeniceTopics() throws ExecutionException {
    String storeName1 = Utils.getUniqueString("store1");
    String storeName2 = Utils.getUniqueString("store2");
    String storeName3 = Utils.getUniqueString("store3");
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(getPubSubTopic(storeName1, "_v1"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_v2"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName1, "_v4"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName2, "_rt"), 1000L);
    storeTopics.put(getPubSubTopic(storeName2, "_v1"), 1000L);
    storeTopics.put(getPubSubTopic(storeName3, "_rt"), 1000L);

    Map<PubSubTopic, Long> storeTopics2 = new HashMap<>();
    storeTopics2.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics2.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);
    storeTopics2.put(getPubSubTopic(storeName2, "_rt"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName3, "_rt"), 1000L);

    Map<PubSubTopic, Long> remoteTopics = new HashMap<>();
    remoteTopics.put(getPubSubTopic(storeName2, "_rt"), 1000L);
    remoteTopics.put(getPubSubTopic(storeName3, "_rt"), 1000L);
    remoteTopics.put(getPubSubTopic(storeName3, "_v1"), 1000L);

    Map<PubSubTopic, Long> remoteTopics2 = new HashMap<>();
    remoteTopics2.put(getPubSubTopic(storeName2, "_rt"), 1000L);
    remoteTopics2.put(getPubSubTopic(storeName3, "_rt"), 1000L);

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics).thenReturn(storeTopics2);
    when(remoteTopicManager.getAllTopicRetentions()).thenReturn(remoteTopics).thenReturn(remoteTopics2);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v1"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v2"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v4"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_v1"));
    // Delete should be blocked by local VT
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_rt"));
    // Delete should be blocked by remote VT
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_rt"));
    verify(topicCleanupServiceStats, atLeastOnce()).recordDeletableTopicsCount(5);
    verify(topicCleanupServiceStats, never()).recordTopicDeletionError();
    verify(topicCleanupServiceStats, atLeastOnce()).recordTopicDeleted();

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_rt"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_rt"));
    verify(topicCleanupServiceStats, atLeastOnce()).recordDeletableTopicsCount(2);
    verify(topicCleanupServiceStats, never()).recordTopicDeletionError();
  }

  private PubSubTopic getPubSubTopic(String storeName, String suffix) {
    return pubSubTopicRepository.getTopic(storeName + suffix);
  }

  @Test
  public void testRun() throws Exception {
    String storeName1 = Utils.getUniqueString("store1");
    String storeName2 = Utils.getUniqueString("store2");
    String storeName3 = Utils.getUniqueString("store3");
    String storeName4 = Utils.getUniqueString("store4");
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);
    doReturn(Optional.of(new StoreConfig(storeName2))).when(storeConfigRepository).getStoreConfig(storeName2);
    doReturn(Optional.of(new StoreConfig(storeName3))).when(storeConfigRepository).getStoreConfig(storeName3);

    Map<PubSubTopic, Long> storeTopics1 = new HashMap<>();
    storeTopics1.put(getPubSubTopic(storeName1, "_v1"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v2"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics1.put(getPubSubTopic(storeName1, "_v4"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);
    storeTopics1.put(getPubSubTopic(storeName4, "_rt"), 1000L);

    Map<PubSubTopic, Long> storeTopics2 = new HashMap<>();
    storeTopics2.put(getPubSubTopic(storeName2, "_v1"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName2, "_v2"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName2, "_v3"), Long.MAX_VALUE);
    storeTopics2.put(getPubSubTopic(storeName3, "_v4"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName3, "_rt"), 1000L);

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1)
        .thenReturn(storeTopics2)
        .thenReturn(new HashMap<>());

    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(true).when(admin).isLeaderControllerOfControllerCluster();
    // Resource is still alive
    doReturn(true).when(admin).isResourceStillAlive(storeName2 + "_v2");

    topicCleanupService.start();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      // As long as topicManager#getAllTopicRetentions has been invoked 4 times, all the store cleanup logic should be
      // done already
      verify(topicManager, atLeast(4)).getAllTopicRetentions();
    });

    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v1"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v2"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_v1"));
    // If we are truncating the RT then all version topics need to be deleted (no min number of version topics to keep)
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_v4"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_v2"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_rt"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v4"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic("non_venice_topic1_rt", ""));
  }

  @Test
  public void testRunWhenCurrentControllerChangeFromLeaderToFollower() throws Exception {
    String storeName1 = Utils.getUniqueString("store1");
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);
    Map<PubSubTopic, Long> storeTopics1 = new HashMap<>();
    storeTopics1.put(getPubSubTopic(storeName1, "_v1"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v2"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics1.put(getPubSubTopic(storeName1, "_v4"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1);

    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    when(admin.isLeaderControllerOfControllerCluster()).thenReturn(true).thenReturn(false);

    topicCleanupService.start();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      // As long as admin#isLeaderControllerOfControllerCluster has been invoked 3 times, all the store cleanup logic
      // should be done already
      verify(admin, atLeast(3)).isLeaderControllerOfControllerCluster();
    });
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(getPubSubTopic(storeName1, "_v1"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(getPubSubTopic(storeName1, "_v2"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(getPubSubTopic(storeName1, "_v4"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlock(getPubSubTopic(storeName1, "_rt"));
  }

  @Test
  public void testRunWhenCurrentControllerChangeFromFollowerToLeader() throws Exception {
    String storeName1 = Utils.getUniqueString("store1");
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);
    Map<PubSubTopic, Long> storeTopics1 = new HashMap<>();
    storeTopics1.put(getPubSubTopic(storeName1, "_v1"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v2"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics1.put(getPubSubTopic(storeName1, "_v4"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);
    // storeTopics1.put(getPubSubTopic("non_venice_topic1", ""), Long.MAX_VALUE);

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1).thenReturn(new HashMap<>());

    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    when(admin.isLeaderControllerOfControllerCluster()).thenReturn(false).thenReturn(true);

    topicCleanupService.start();
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      // As long as topicManager#getAllTopicRetentions has been invoked 2 times, all the store cleanup logic should be
      // done already
      verify(topicManager, atLeast(2)).getAllTopicRetentions();
    });

    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v1"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v2"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v4"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
    // verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic("non_venice_topic1", ""));
  }

  @Test
  public void testExtractVersionTopicsToCleanupIgnoresInputWithNonVersionTopics() {
    String storeName = Utils.getUniqueString("test_store");
    Map<PubSubTopic, Long> topicRetentions = new HashMap<>();
    topicRetentions.put(pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(storeName)), Long.MAX_VALUE);
    topicRetentions
        .put(pubSubTopicRepository.getTopic(Version.composeStreamReprocessingTopic(storeName, 1)), Long.MAX_VALUE);
    topicRetentions.put(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1)), 1000L);
    topicRetentions.put(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2)), Long.MAX_VALUE);
    topicRetentions.put(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 3)), Long.MAX_VALUE);

    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000);
    doReturn(false).when(admin).isResourceStillAlive(anyString());

    List<PubSubTopic> deletableTopics = TopicCleanupService.extractVersionTopicsToCleanup(admin, topicRetentions, 2, 0);
    assertEquals(deletableTopics.size(), 1, "There should only be one deletable topic");
    assertTrue(deletableTopics.contains(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1))));
  }

  @Test
  public void testCleanVeniceTopicsBlockRTTopicDeletionWhenMisconfigured() {
    // RT topic deletion should be blocked when controller is misconfigured
    // Mis-configured where local data center is not in the child data centers list
    VeniceControllerConfig veniceControllerConfig = mock(VeniceControllerConfig.class);
    doReturn(veniceControllerConfig).when(veniceControllerMultiClusterConfig).getCommonConfig();
    doReturn("remote").when(veniceControllerConfig).getChildDatacenters();
    TopicCleanupService blockedTopicCleanupService = new TopicCleanupService(
        admin,
        veniceControllerMultiClusterConfig,
        pubSubTopicRepository,
        topicCleanupServiceStats);
    String storeName = Utils.getUniqueString("testStore");
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(getPubSubTopic(storeName, "_rt"), 1000L);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();
    doReturn(storeTopics).when(remoteTopicManager).getAllTopicRetentions();
    doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    blockedTopicCleanupService.cleanupVeniceTopics();
    verify(topicManager, atLeastOnce()).getPubSubClusterAddress();
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName, "_rt"));
    verify(topicCleanupServiceStats, atLeastOnce()).recordDeletableTopicsCount(1);
    verify(topicCleanupServiceStats, atLeastOnce()).recordTopicDeletionError();
  }

  @Test
  public void testCleanVeniceTopicRTTopicDeletionWithErrorFetchingVT() {
    // RT topic deletion should be blocked when version topic cannot be fetched due to error
    String storeName = Utils.getUniqueString("testStore");
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(getPubSubTopic(storeName, "_rt"), 1000L);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();
    doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    when(remoteTopicManager.getAllTopicRetentions()).thenThrow(new VeniceException("test")).thenReturn(storeTopics);

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName, "_rt"));
    verify(remoteTopicManager, atLeastOnce()).getAllTopicRetentions();
    verify(topicCleanupServiceStats, atLeastOnce()).recordDeletableTopicsCount(1);
    verify(topicCleanupServiceStats, atLeastOnce()).recordTopicDeletionError();

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName, "_rt"));
  }

  @Test
  public void testCleanVeniceTopicOnlyFetchVTOnRTTopicDeletion() {
    String storeName = Utils.getUniqueString("testStore");
    Map<PubSubTopic, Long> storeTopics1 = new HashMap<>();
    Map<PubSubTopic, Long> storeTopics2 = new HashMap<>();
    storeTopics1.put(getPubSubTopic(storeName, "_rt"), Long.MAX_VALUE);
    storeTopics1.put(getPubSubTopic(storeName, "_v1"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName, "_v2"), Long.MAX_VALUE);
    storeTopics2.put(getPubSubTopic(storeName, "_rt"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName, "_v2"), 1000L);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1).thenReturn(storeTopics2);
    doReturn(storeTopics2).when(remoteTopicManager).getAllTopicRetentions();

    topicCleanupService.cleanupVeniceTopics();

    verify(remoteTopicManager, never()).getAllTopicRetentions();

    topicCleanupService.cleanupVeniceTopics();

    verify(remoteTopicManager, atLeastOnce()).getAllTopicRetentions();
  }
}
