package com.linkedin.venice.controller;

import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.meta.ZKStore;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHelixAdminWithoutCluster {
  @Test
  public void canFindStartedVersionInStore() {
    String storeName = Utils.getUniqueString("store");
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    store.increaseVersion("123");
    Version version = store.getVersions().get(0);
    Optional<Version> returnedVersion = VeniceHelixAdmin.getStartedVersion(store);
    Assert.assertEquals(returnedVersion.get(), version);
  }

  /**
   * This test should fail and be removed when we revert to using push ID to guarantee idempotence.
   */
  @Test
  public void findStartedVersionIgnoresPushId() {
    String pushId = Utils.getUniqueString("pushid");
    String storeName = Utils.getUniqueString("store");
    Store store = new ZKStore(
        storeName,
        "owner",
        System.currentTimeMillis(),
        PersistenceType.IN_MEMORY,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION,
        1);
    store.increaseVersion(pushId);
    Version version = store.getVersions().get(0);
    store.updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    store.increaseVersion(pushId);
    Version startedVersion = store.getVersions().get(1);
    Optional<Version> returnedVersion = VeniceHelixAdmin.getStartedVersion(store);
    Assert.assertEquals(returnedVersion.get(), startedVersion);
  }

  @Test
  public void canMergeNewHybridConfigValuesToOldStore() {
    String storeName = Utils.getUniqueString("storeName");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    Assert.assertFalse(store.isHybrid());

    Optional<Long> rewind = Optional.of(123L);
    Optional<Long> lagOffset = Optional.of(1500L);
    Optional<Long> timeLag = Optional.of(300L);
    Optional<DataReplicationPolicy> dataReplicationPolicy = Optional.of(DataReplicationPolicy.AGGREGATE);
    Optional<BufferReplayPolicy> bufferReplayPolicy = Optional.of(BufferReplayPolicy.REWIND_FROM_EOP);
    HybridStoreConfig hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    Assert.assertNull(
        hybridStoreConfig,
        "passing empty optionals and a non-hybrid store should generate a null hybrid config");

    hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        rewind,
        lagOffset,
        timeLag,
        dataReplicationPolicy,
        bufferReplayPolicy);
    Assert.assertNotNull(hybridStoreConfig, "specifying rewind and lagOffset should generate a valid hybrid config");
    Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1500L);
    Assert.assertEquals(hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(), 300L);
    Assert.assertEquals(hybridStoreConfig.getDataReplicationPolicy(), DataReplicationPolicy.AGGREGATE);

    // It's okay that time lag threshold or data replication policy is not specified
    hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        rewind,
        lagOffset,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
    Assert.assertNotNull(hybridStoreConfig, "specifying rewind and lagOffset should generate a valid hybrid config");
    Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1500L);
    Assert.assertEquals(
        hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(),
        HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD);
    Assert.assertEquals(hybridStoreConfig.getDataReplicationPolicy(), DataReplicationPolicy.NON_AGGREGATE);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*still exists in cluster.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenExistsInOtherCluster() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    StoreConfig storeConfig = new StoreConfig(storeName);
    storeConfig.setCluster("cluster2");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.of(storeConfig),
        Optional.empty(),
        Collections.emptySet(),
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*still exists in cluster.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenExistsInTheSameCluster() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Store store = TestUtils.getRandomStore();
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.of(store),
        Collections.emptySet(),
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  /**
   * Right now, version topic check is ignored since today, Venice is still keeping a couple of latest deprecated
   * version topics to avoid SNs failure due to UNKNOWN_TOPIC_OR_PARTITION errors.
   */
  @Test
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeVersionTopicStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Set<String> topics = new HashSet<>();
    topics.add(Version.composeKafkaTopic(storeName, 1));
    topics.add("unknown_store_v1");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        topics,
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Topic.*still exists for store.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenRTTopicStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Set<String> topics = new HashSet<>();
    topics.add(Version.composeRealTimeTopic(storeName));
    topics.add("unknown_store_v1");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        topics,
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Topic.*still exists for store.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeSystemStoreTopicStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    Set<String> topics = new HashSet<>();
    topics.add(Version.composeRealTimeTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName)));
    topics.add("unknown_store_v1");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        topics,
        Collections.emptyList(),
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Helix Resource.*still exists for store.*")
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeSystemStoreHelixResourceStillExists() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    List<String> resources = new LinkedList<>();
    resources.add(Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1));
    resources.add("unknown_store_v1");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        Collections.emptySet(),
        resources,
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName));
  }

  @Test
  public void testCheckResourceCleanupBeforeStoreCreationWhenSomeSystemStoreHelixResourceStillExistsButHelixResourceSkipped() {
    String clusterName = "cluster1";
    String storeName = Utils.getUniqueString("test_store_recreation");
    List<String> resources = new LinkedList<>();
    resources.add(Version.composeKafkaTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName), 1));
    resources.add("unknown_store_v1");
    testCheckResourceCleanupBeforeStoreCreationWithParams(
        clusterName,
        storeName,
        Optional.empty(),
        Optional.empty(),
        Collections.emptySet(),
        resources,
        admin -> admin.checkResourceCleanupBeforeStoreCreation(clusterName, storeName, false));
  }

  private void testCheckResourceCleanupBeforeStoreCreationWithParams(
      String clusterName,
      String storeName,
      Optional<StoreConfig> storeConfig,
      Optional<Store> store,
      Set<String> topics,
      List<String> helixResources,
      Consumer<VeniceHelixAdmin> testExecution) {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);

    ZkStoreConfigAccessor storeConfigAccessor = mock(ZkStoreConfigAccessor.class);
    doReturn(storeConfig.isPresent() ? storeConfig.get() : null).when(storeConfigAccessor).getStoreConfig(storeName);
    doReturn(storeConfigAccessor).when(admin).getStoreConfigAccessor(clusterName);

    ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    doReturn(store.isPresent() ? store.get() : null).when(storeRepository).getStore(storeName);
    doReturn(storeRepository).when(admin).getMetadataRepository(clusterName);

    TopicManager topicManager = mock(TopicManager.class);
    doReturn(topics).when(topicManager).listTopics();
    doReturn(topicManager).when(admin).getTopicManager();

    doReturn(helixResources).when(admin).getAllLiveHelixResources(clusterName);

    doCallRealMethod().when(admin).checkResourceCleanupBeforeStoreCreation(anyString(), anyString());
    doCallRealMethod().when(admin).checkResourceCleanupBeforeStoreCreation(anyString(), anyString(), anyBoolean());
    doCallRealMethod().when(admin)
        .checkKafkaTopicAndHelixResource(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean());

    testExecution.accept(admin);
  }
}
