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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.PartitionInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestTopicCleanupService {
  private Admin admin;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;
  private TopicManager topicManager;
  private TopicCleanupService topicCleanupService;
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
    VeniceControllerMultiClusterConfig config = mock(VeniceControllerMultiClusterConfig.class);
    doReturn(0L).when(config).getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    doReturn(1).when(config).getMinNumberOfUnusedKafkaTopicsToPreserve();
    doReturn(1).when(admin).getMinNumberOfUnusedKafkaTopicsToPreserve();
    topicCleanupService = new TopicCleanupService(admin, config, pubSubTopicRepository);
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

    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();

    Map<String, Map<PubSubTopic, Long>> filteredStoreTopics =
        TopicCleanupService.getAllVeniceStoreTopicsRetentions(admin.getTopicManager());
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
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(getPubSubTopic(storeName1, "_v1"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_v2"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName1, "_v4"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);

    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v1"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v2"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v4"));

    // Updated real-time topic to use low retention policy
    storeTopics.put(getPubSubTopic(storeName1, "_rt"), 1000L);
    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
  }

  private PubSubTopic getPubSubTopic(String storeName, String suffix) {
    return pubSubTopicRepository.getTopic(storeName + suffix);
  }

  @Test
  public void testRun() throws Exception {
    String storeName1 = Utils.getUniqueString("store1");
    String storeName2 = Utils.getUniqueString("store2");
    String storeName3 = Utils.getUniqueString("store3");
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);
    doReturn(Optional.of(new StoreConfig(storeName2))).when(storeConfigRepository).getStoreConfig(storeName2);
    doReturn(Optional.of(new StoreConfig(storeName3))).when(storeConfigRepository).getStoreConfig(storeName3);

    Map<PubSubTopic, Long> storeTopics1 = new HashMap<>();
    storeTopics1.put(getPubSubTopic(storeName1, "_v1"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v2"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics1.put(getPubSubTopic(storeName1, "_v4"), 1000L);
    storeTopics1.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);
    // storeTopics1.put(getPubSubTopic("non_venice_topic1", ""), Long.MAX_VALUE);

    Map<PubSubTopic, Long> storeTopics2 = new HashMap<>();
    storeTopics2.put(getPubSubTopic(storeName2, "_v1"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName2, "_v2"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName2, "_v3"), Long.MAX_VALUE);
    storeTopics2.put(getPubSubTopic(storeName3, "_v4"), 1000L);
    storeTopics2.put(getPubSubTopic(storeName3, "_rt"), 1000L);

    Map<PubSubTopic, Long> storeTopics3 = new HashMap<>();

    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1)
        .thenReturn(storeTopics2)
        .thenReturn(storeTopics3)
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
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_v2"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_rt"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v4"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_v4"));
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
  public void testCleanupReplicaStatusesFromMetaSystemStoreInParent() {
    doReturn(true).when(admin).isParent();
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic("test", 1));
    assertFalse(topicCleanupService.cleanupReplicaStatusesFromMetaSystemStore(versionTopic));
  }

  @Test
  public void testCleanupReplicaStatusesFromMetaSystemStoreWithRTTopic() {
    doReturn(false).when(admin).isParent();
    PubSubTopic rtTopic = pubSubTopicRepository.getTopic(Version.composeRealTimeTopic("test"));
    assertFalse(topicCleanupService.cleanupReplicaStatusesFromMetaSystemStore(rtTopic));
  }

  @Test
  public void testCleanupReplicaStatusesFromMetaSystemStoreWhenMetaSystemStoreRTTopicNotExist() {
    doReturn(false).when(admin).isParent();
    String storeName = Utils.getUniqueString("test_store");
    doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    int version = 1;
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, version));
    HelixReadOnlyStoreConfigRepository repository = mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig(storeName);
    String cluster = "test_cluster";
    storeConfig.setCluster(cluster);
    doReturn(Optional.of(storeConfig)).when(repository).getStoreConfig(storeName);
    doReturn(repository).when(admin).getStoreConfigRepo();
    PubSubTopic rtTopicForMetaSystemStore = pubSubTopicRepository
        .getTopic(Version.composeRealTimeTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName)));
    TopicManager topicManager = mock(TopicManager.class);
    doReturn(false).when(topicManager).containsTopic(rtTopicForMetaSystemStore);
    doReturn(topicManager).when(admin).getTopicManager();
    assertFalse(topicCleanupService.cleanupReplicaStatusesFromMetaSystemStore(versionTopic));
  }

  @Test
  public void testCleanupReplicaStatusesFromMetaSystemStoreWhenMetaSystemStoreRTTopicExist() {
    doReturn(false).when(admin).isParent();
    String storeName = Utils.getUniqueString("test_store");
    doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);
    int version = 1;
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, version));
    HelixReadOnlyStoreConfigRepository repository = mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig(storeName);
    String cluster = "test_cluster";
    storeConfig.setCluster(cluster);
    doReturn(Optional.of(storeConfig)).when(repository).getStoreConfig(storeName);
    doReturn(repository).when(admin).getStoreConfigRepo();
    PubSubTopic rtTopicForMetaSystemStore = pubSubTopicRepository
        .getTopic(Version.composeRealTimeTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName)));
    TopicManager topicManager = mock(TopicManager.class);
    doReturn(true).when(topicManager).containsTopic(rtTopicForMetaSystemStore);
    doReturn(topicManager).when(admin).getTopicManager();

    // Topic is with partition count: 3
    int partitionCnt = 3;
    List<PartitionInfo> partitionInfoList = new ArrayList<>();
    for (int i = 0; i < partitionCnt; ++i) {
      partitionInfoList.add(new PartitionInfo(versionTopic.getName(), i, null, null, null));
    }
    doReturn(partitionInfoList).when(topicManager).partitionsFor(versionTopic);

    MetaStoreWriter metaStoreWriter = mock(MetaStoreWriter.class);
    doReturn(metaStoreWriter).when(admin).getMetaStoreWriter();

    assertTrue(topicCleanupService.cleanupReplicaStatusesFromMetaSystemStore(versionTopic));
    for (int i = 0; i < partitionCnt; ++i) {
      verify(metaStoreWriter).deleteStoreReplicaStatus(cluster, storeName, version, i);
    }
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
}
