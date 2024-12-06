package com.linkedin.venice.controller.kafka;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.stats.TopicCleanupServiceStats;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapter;
import com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
  private final PubSubClientsFactory pubSubClientsFactory = mock(PubSubClientsFactory.class);

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
    doReturn(new ApacheKafkaAdminAdapterFactory()).when(veniceControllerMultiClusterConfig)
        .getSourceOfTruthAdminAdapterFactory();
    doReturn(1).when(admin).getMinNumberOfUnusedKafkaTopicsToPreserve();

    VeniceControllerClusterConfig controllerConfig = mock(VeniceControllerClusterConfig.class);
    doReturn(controllerConfig).when(veniceControllerMultiClusterConfig).getCommonConfig();
    doReturn(Utils.setOf("local", "remote")).when(controllerConfig).getChildDatacenters();
    Map<String, String> dataCenterToBootstrapServerMap = new HashMap<>();
    dataCenterToBootstrapServerMap.put("local", "local");
    dataCenterToBootstrapServerMap.put("remote", "remote");
    doReturn(dataCenterToBootstrapServerMap).when(veniceControllerMultiClusterConfig).getChildDataCenterKafkaUrlMap();
    doReturn(1).when(veniceControllerMultiClusterConfig).getDanglingTopicOccurrenceThresholdForCleanup();
    doReturn("local").when(topicManager).getPubSubClusterAddress();
    remoteTopicManager = mock(TopicManager.class);
    doReturn(remoteTopicManager).when(admin).getTopicManager("remote");
    doReturn(Collections.emptyMap()).when(remoteTopicManager).getAllTopicRetentions();
    topicCleanupServiceStats = mock(TopicCleanupServiceStats.class);

    doReturn(new ApacheKafkaAdminAdapterFactory()).when(pubSubClientsFactory).getAdminAdapterFactory();
    topicCleanupService = new TopicCleanupService(
        admin,
        veniceControllerMultiClusterConfig,
        pubSubTopicRepository,
        topicCleanupServiceStats,
        pubSubClientsFactory);

    when(admin.getStore(any(), anyString())).thenAnswer(invocation -> {
      String requestedStoreName = invocation.getArgument(1); // Capture the storeName argument
      Store mockStore = mock(Store.class, RETURNS_DEEP_STUBS);
      when(mockStore.getHybridStoreConfig().getRealTimeTopicName())
          .thenReturn(Utils.composeRealTimeTopic(requestedStoreName));
      return mockStore;
    });
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
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(LOW_RETENTION_POLICY));
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(HIGH_RETENTION_POLICY));
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
  public void testCleanupVeniceTopics() {
    String clusterName = "clusterName";
    String storeName1 = Utils.getUniqueString("store1");
    String storeName2 = Utils.getUniqueString("store2");
    String storeName3 = Utils.getUniqueString("store3");
    String storeName4 = Utils.getUniqueString("store4");
    String storeName5 = Utils.getUniqueString("store5");
    String storeName6 = Utils.getUniqueString("store6");

    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(getPubSubTopic(storeName1, "_v1"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_v2"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_v3"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName1, "_v4"), 1000L);
    storeTopics.put(getPubSubTopic(storeName1, "_rt"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName2, "_rt"), 1000L);
    storeTopics.put(getPubSubTopic(storeName2, "_v1"), 1000L);
    storeTopics.put(getPubSubTopic(storeName3, "_rt"), 1000L);
    storeTopics.put(getPubSubTopic(storeName3, "_v100"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName4, "_rt"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName5, "_v1"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName6, "_rt"), 1000L);
    storeTopics.put(getPubSubTopic(storeName6, "_v1"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(storeName6, "_v2"), Long.MAX_VALUE);
    storeTopics.put(getPubSubTopic(PubSubTopicType.ADMIN_TOPIC_PREFIX, "_cluster"), Long.MAX_VALUE);

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
    when(remoteTopicManager.listTopics()).thenReturn(remoteTopics.keySet()).thenReturn(remoteTopics2.keySet());
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(Long.MAX_VALUE));
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(1000L));
    doReturn(Pair.create(clusterName, null)).when(admin).discoverCluster(anyString());
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), any());
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);

    Set<PubSubTopic> pubSubTopicSet = new HashSet<>();
    pubSubTopicSet.addAll(storeTopics.keySet());
    pubSubTopicSet.remove(getPubSubTopic(storeName3, "_v100"));
    pubSubTopicSet.remove(getPubSubTopic(storeName4, "_rt"));
    pubSubTopicSet.remove(getPubSubTopic(storeName5, "_v1"));
    pubSubTopicSet.remove(getPubSubTopic(PubSubTopicType.ADMIN_TOPIC_PREFIX, "_cluster"));

    ApacheKafkaAdminAdapterFactory apacheKafkaAdminAdapterFactory = mock(ApacheKafkaAdminAdapterFactory.class);
    ApacheKafkaAdminAdapter apacheKafkaAdminAdapter = mock(ApacheKafkaAdminAdapter.class);
    doReturn(apacheKafkaAdminAdapter).when(apacheKafkaAdminAdapterFactory).create(any(), eq(pubSubTopicRepository));

    topicCleanupService.setSourceOfTruthPubSubAdminAdapter(apacheKafkaAdminAdapter);
    Pair<String, String> pair = new Pair<>(clusterName, "");
    doReturn(pair).when(admin).discoverCluster(anyString());
    doThrow(new VeniceNoStoreException(storeName5)).when(admin).discoverCluster(storeName5);

    Store store2 = mock(Store.class);
    doReturn(storeName2).when(store2).getName();

    Store store3 = mock(Store.class);
    doReturn(storeName3).when(store3).getName();

    doReturn(false).when(store3).containsVersion(100);

    Store store4 = mock(Store.class);
    doReturn(storeName4).when(store4).getName();
    doReturn(false).when(store4).isHybrid();

    Store store5 = mock(Store.class);

    Store store6 = mock(Store.class);
    doReturn(storeName6).when(store6).getName();

    Version batchVersion = mock(Version.class);
    doReturn(null).when(batchVersion).getHybridStoreConfig();

    doReturn(store2).when(admin).getStore(clusterName, storeName2);
    doReturn(store3).when(admin).getStore(clusterName, storeName3);
    doReturn(store4).when(admin).getStore(clusterName, storeName4);
    doReturn(store5).when(admin).getStore(clusterName, storeName5);
    doReturn(store6).when(admin).getStore(clusterName, storeName6);
    doReturn(pubSubTopicSet).when(apacheKafkaAdminAdapter).listAllTopics();

    Version hybridVersion = mock(Version.class);
    doReturn(mock(HybridStoreConfig.class)).when(hybridVersion).getHybridStoreConfig();

    doReturn(Collections.singletonList(hybridVersion)).when(store2).getVersions();
    doReturn(Collections.singletonList(hybridVersion)).when(store3).getVersions();
    doReturn(Collections.singletonList(batchVersion)).when(store6).getVersions();
    // simulating blocked delete
    doReturn(false).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName1));
    doReturn(false).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName2));
    doReturn(false).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName3));
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName4));
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName5));
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName6));

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_rt"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v1"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v2"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v3"));
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName1, "_v4"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_v1"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName6, "_rt"));
    // Delete should be blocked by local VT
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName2, "_rt"));
    // Delete should be blocked by remote VT
    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_rt"));
    verify(topicCleanupServiceStats, atLeastOnce()).recordDeletableTopicsCount(9);
    verify(topicCleanupServiceStats, never()).recordTopicDeletionError();
    verify(topicCleanupServiceStats, atLeastOnce()).recordTopicDeleted();

    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName3, "_v100"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName4, "_rt"));
    verify(topicManager, atLeastOnce()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName5, "_v1"));

    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName2));
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), eq(storeName3));
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
  public void testRun() {
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
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(Long.MAX_VALUE));
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(1000L));
    doReturn(true).when(admin).isLeaderControllerOfControllerCluster();
    doReturn(Pair.create("cluster0", null)).when(admin).discoverCluster(any());
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
  public void testRunWhenCurrentControllerChangeFromLeaderToFollower() {
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
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(Long.MAX_VALUE));
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(1000L));
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
  public void testRunWhenCurrentControllerChangeFromFollowerToLeader() {
    String storeName1 = Utils.getUniqueString("store1");
    doReturn(Optional.of(new StoreConfig(storeName1))).when(storeConfigRepository).getStoreConfig(storeName1);
    doReturn(new Pair<>("clusterName", "")).when(admin).discoverCluster(anyString());
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
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(Long.MAX_VALUE));
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(1000L));
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
    topicRetentions.put(pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(storeName)), Long.MAX_VALUE);
    topicRetentions
        .put(pubSubTopicRepository.getTopic(Version.composeStreamReprocessingTopic(storeName, 1)), Long.MAX_VALUE);
    topicRetentions.put(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1)), 1000L);
    topicRetentions.put(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 2)), Long.MAX_VALUE);
    topicRetentions.put(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 3)), Long.MAX_VALUE);

    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(1000L));
    doReturn(false).when(admin).isResourceStillAlive(anyString());

    List<PubSubTopic> deletableTopics = TopicCleanupService.extractVersionTopicsToCleanup(admin, topicRetentions, 2, 0);
    assertEquals(deletableTopics.size(), 1, "There should only be one deletable topic");
    assertTrue(deletableTopics.contains(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1))));
  }

  @Test
  public void testCleanVeniceTopicRTTopicDeletionWithErrorFetchingVT() {
    // RT topic deletion should be blocked when version topic cannot be fetched due to error
    String storeName = Utils.getUniqueString("testStore");
    Map<PubSubTopic, Long> storeTopics = new HashMap<>();
    storeTopics.put(getPubSubTopic(storeName, "_rt"), 1000L);
    doReturn(Pair.create("cluster0", null)).when(admin).discoverCluster(any());
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(Long.MAX_VALUE);
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(1000L);
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(Long.MAX_VALUE));
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(1000L));
    doReturn(storeTopics).when(topicManager).getAllTopicRetentions();
    doReturn(false).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), any());
    doReturn(Optional.of(new StoreConfig(storeName))).when(storeConfigRepository).getStoreConfig(storeName);

    topicCleanupService.cleanupVeniceTopics();

    verify(topicManager, never()).ensureTopicIsDeletedAndBlockWithRetry(getPubSubTopic(storeName, "_rt"));
    verify(topicCleanupServiceStats, atLeastOnce()).recordDeletableTopicsCount(1);

    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), any());
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
    doReturn(false).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(Long.MAX_VALUE));
    doReturn(true).when(admin).isTopicTruncatedBasedOnRetention(any(), eq(1000L));
    doReturn(Pair.create("cluster0", null)).when(admin).discoverCluster(any());
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(anyString(), any());
    when(topicManager.getAllTopicRetentions()).thenReturn(storeTopics1).thenReturn(storeTopics2);
    doReturn(storeTopics2).when(remoteTopicManager).getAllTopicRetentions();

    topicCleanupService.cleanupVeniceTopics();

    verify(remoteTopicManager, never()).getAllTopicRetentions();

    topicCleanupService.cleanupVeniceTopics();
  }

  @Test
  public void testStopViaFlag() {
    testStop(service -> service.stopViaFlag());
  }

  @Test
  public void testStopViaInterrupt() {
    testStop(service -> service.stopViaInterrupt());
  }

  /**
   * Test that the {@link TopicCleanupService} can close either via interruption or via its
   * {@link java.util.concurrent.atomic.AtomicBoolean}, thus providing redundant stop mechanisms.
   */
  private void testStop(Consumer<TopicCleanupService> stopMechanism) {
    topicCleanupService.start();
    TestUtils
        .waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, true, () -> assertTrue(topicCleanupService.isRunning()));
    assertFalse(topicCleanupService.isStopped());
    stopMechanism.accept(topicCleanupService);
    TestUtils
        .waitForNonDeterministicAssertion(1, TimeUnit.SECONDS, true, () -> assertTrue(topicCleanupService.isStopped()));
  }
}
