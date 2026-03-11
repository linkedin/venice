package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceHelixAdminWithoutCluster {
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void canMergeNewHybridConfigValuesToOldStore() {
    String storeName = Utils.getUniqueString("storeName");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    Assert.assertFalse(store.isHybrid());

    Optional<Long> rewind = Optional.of(123L);
    Optional<Long> lagOffset = Optional.of(1500L);
    Optional<Long> timeLag = Optional.of(300L);
    Optional<DataReplicationPolicy> dataReplicationPolicy = Optional.of(DataReplicationPolicy.NON_AGGREGATE);
    Optional<BufferReplayPolicy> bufferReplayPolicy = Optional.of(BufferReplayPolicy.REWIND_FROM_EOP);
    Optional<String> realTimeTopicName = Optional.of("storeName_rt");
    HybridStoreConfig hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        Optional.empty(),
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
        bufferReplayPolicy,
        realTimeTopicName);
    Assert.assertNotNull(hybridStoreConfig, "specifying rewind and lagOffset should generate a valid hybrid config");
    Assert.assertEquals(hybridStoreConfig.getRewindTimeInSeconds(), 123L);
    Assert.assertEquals(hybridStoreConfig.getOffsetLagThresholdToGoOnline(), 1500L);
    Assert.assertEquals(hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds(), 300L);
    Assert.assertEquals(hybridStoreConfig.getDataReplicationPolicy(), DataReplicationPolicy.NON_AGGREGATE);

    // It's okay that time lag threshold or data replication policy is not specified
    hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
        store,
        rewind,
        lagOffset,
        Optional.empty(),
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
    Set<PubSubTopic> topics = new HashSet<>();

    topics.add(pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1)));
    topics.add(pubSubTopicRepository.getTopic("unknown_store_v1"));
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
    Set<PubSubTopic> topics = new HashSet<>();
    topics.add(pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(storeName)));
    topics.add(pubSubTopicRepository.getTopic("unknown_store_v1"));
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
    Set<PubSubTopic> topics = new HashSet<>();
    topics.add(
        pubSubTopicRepository
            .getTopic(Utils.composeRealTimeTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName))));
    topics.add(pubSubTopicRepository.getTopic("unknown_store_v1"));
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
      Set<PubSubTopic> topics,
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
    doReturn(store.orElse(null)).when(admin).getStore(clusterName, storeName);

    doReturn(helixResources).when(admin).getAllLiveHelixResources(clusterName);

    doCallRealMethod().when(admin).checkResourceCleanupBeforeStoreCreation(anyString(), anyString());
    doCallRealMethod().when(admin).checkResourceCleanupBeforeStoreCreation(anyString(), anyString(), anyBoolean());
    doCallRealMethod().when(admin)
        .checkKafkaTopicAndHelixResource(anyString(), anyString(), anyBoolean(), anyBoolean(), anyBoolean());

    testExecution.accept(admin);
  }

  @Test
  public void testSourceRegionSelectionForTargetedRegionPush() {
    // cluster config setup
    VeniceControllerMultiClusterConfig multiClusterConfigs = mock(VeniceControllerMultiClusterConfig.class);
    VeniceControllerClusterConfig config = mock(VeniceControllerClusterConfig.class);
    doReturn(config).when(multiClusterConfigs).getControllerConfig("test_cluster");
    doReturn("dc-4").when(config).getNativeReplicationSourceFabric();

    // store setup
    Store store = mock(Store.class);
    doReturn("dc-3").when(store).getNativeReplicationSourceFabric();

    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
    doCallRealMethod().when(admin).getNativeReplicationSourceFabric(anyString(), any(), any(), any(), any());
    doReturn(multiClusterConfigs).when(admin).getMultiClusterConfigs();

    // Note that for some weird reasons, if this test case is moved below the store cannot return mocked response
    // even if the reference doesn't change.
    // store config (dc-3) is specified as 4th priority
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.empty(), Optional.empty(), null),
        "dc-3");

    // emergencySourceRegion (dc-0) is specified as 1st priority
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.of("dc-2"), Optional.of("dc-0"), "dc-1"),
        "dc-0");

    // VPJ plugin targeted region config (dc-1) is specified as 2nd priority
    doReturn(null).when(store).getNativeReplicationSourceFabric();
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.of("dc-2"), Optional.empty(), "dc-1"),
        "dc-1");

    // VPJ source fabric (dc-2) is specified as 3rd priority
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.of("dc-2"), Optional.empty(), null),
        "dc-2");

    // cluster config (dc-4) is specified as 5th priority
    doReturn(null).when(store).getNativeReplicationSourceFabric();
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric("test_cluster", store, Optional.empty(), Optional.empty(), null),
        "dc-4");

    /**
     * When we have the following setup:
     * source fabric is dc-1,
     * store config is dc-3,
     * cluster config is dc-4,
     * targeted regions is dc-0, dc-2, dc-4, dc-99
     *
     * we should pick dc-4 as the source fabric even though it has lower priority than dc-3, but it's in the targeted list
     */
    doReturn("dc-3").when(store).getNativeReplicationSourceFabric();
    Assert.assertEquals(
        admin.getNativeReplicationSourceFabric(
            "test_cluster",
            store,
            Optional.of("dc-1"),
            Optional.empty(),
            "dc-99, dc-0, dc-4, dc-2"),
        "dc-4");
  }

  @Test
  public void testChildControllerSetterValidation() throws Exception {
    VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);

    // --- setBackupVersionRetentionMs (private) ---
    Method setBackupRetention =
        VeniceHelixAdmin.class.getDeclaredMethod("setBackupVersionRetentionMs", String.class, String.class, long.class);
    setBackupRetention.setAccessible(true);

    // 0 ms should throw INVALID_CONFIG
    try {
      setBackupRetention.invoke(admin, "cluster", "store", 0L);
      Assert.fail("Expected VeniceHttpException for retention 0");
    } catch (InvocationTargetException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceHttpException);
      VeniceHttpException ex = (VeniceHttpException) e.getCause();
      Assert.assertEquals(ex.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
      Assert.assertEquals(ex.getErrorType(), ErrorType.INVALID_CONFIG);
    }

    // 23 hours (below 1-day minimum) should throw
    try {
      setBackupRetention.invoke(admin, "cluster", "store", TimeUnit.HOURS.toMillis(23));
      Assert.fail("Expected VeniceHttpException for retention below minimum");
    } catch (InvocationTargetException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceHttpException);
    }

    // -1 (cluster default) should NOT throw (no call to storeMetadataUpdate expected since admin is a mock)
    // The validation passes; storeMetadataUpdate is mocked to do nothing
    setBackupRetention.invoke(admin, "cluster", "store", -1L);

    // Exactly 1 day should NOT throw
    setBackupRetention.invoke(admin, "cluster", "store", Store.MIN_BACKUP_VERSION_RETENTION_MS);

    // --- setReplicationFactor (private) ---
    Method setReplicationFactor =
        VeniceHelixAdmin.class.getDeclaredMethod("setReplicationFactor", String.class, String.class, int.class);
    setReplicationFactor.setAccessible(true);

    try {
      setReplicationFactor.invoke(admin, "cluster", "store", 0);
      Assert.fail("Expected VeniceHttpException for replicationFactor 0");
    } catch (InvocationTargetException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceHttpException);
      Assert.assertEquals(((VeniceHttpException) e.getCause()).getErrorType(), ErrorType.INVALID_CONFIG);
    }

    // --- setBatchGetLimit (private) ---
    Method setBatchGetLimit =
        VeniceHelixAdmin.class.getDeclaredMethod("setBatchGetLimit", String.class, String.class, int.class);
    setBatchGetLimit.setAccessible(true);

    try {
      setBatchGetLimit.invoke(admin, "cluster", "store", 0);
      Assert.fail("Expected VeniceHttpException for batchGetLimit 0");
    } catch (InvocationTargetException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceHttpException);
      Assert.assertEquals(((VeniceHttpException) e.getCause()).getErrorType(), ErrorType.INVALID_CONFIG);
    }

    // --- setNumVersionsToPreserve (private) ---
    Method setNumVersions =
        VeniceHelixAdmin.class.getDeclaredMethod("setNumVersionsToPreserve", String.class, String.class, int.class);
    setNumVersions.setAccessible(true);

    try {
      setNumVersions.invoke(admin, "cluster", "store", -1);
      Assert.fail("Expected VeniceHttpException for numVersionsToPreserve -1");
    } catch (InvocationTargetException e) {
      Assert.assertTrue(e.getCause() instanceof VeniceHttpException);
      Assert.assertEquals(((VeniceHttpException) e.getCause()).getErrorType(), ErrorType.INVALID_CONFIG);
    }

    // 0 is valid (NUM_VERSION_PRESERVE_NOT_SET)
    setNumVersions.invoke(admin, "cluster", "store", 0);

    // --- setBootstrapToOnlineTimeoutInHours (package-private) ---
    doCallRealMethod().when(admin)
        .setBootstrapToOnlineTimeoutInHours(anyString(), anyString(), org.mockito.ArgumentMatchers.anyInt());

    try {
      admin.setBootstrapToOnlineTimeoutInHours("cluster", "store", 0);
      Assert.fail("Expected VeniceHttpException for bootstrapToOnlineTimeoutInHours 0");
    } catch (VeniceHttpException e) {
      Assert.assertEquals(e.getHttpStatusCode(), HttpStatus.SC_BAD_REQUEST);
      Assert.assertEquals(e.getErrorType(), ErrorType.INVALID_CONFIG);
    }
  }

}
