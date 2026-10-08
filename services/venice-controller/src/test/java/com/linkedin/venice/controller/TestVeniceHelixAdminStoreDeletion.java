package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pushmonitor.PushMonitorDelegator;
import com.linkedin.venice.pushstatushelper.PushStatusStoreWriter;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Full deletion retries must use broker inventory, not version metadata removed by an earlier attempt.
 */
public class TestVeniceHelixAdminStoreDeletion {
  private static final String CLUSTER_NAME = "deletion-cluster";
  private static final String STORE_NAME = "deletion_store";
  private static final long CREATED_TIME = 1L;
  private static final long ORIGINAL_RETENTION_MS = 86_400_000L;
  private static final long DEPRECATED_RETENTION_MS = 1_000L;
  private static final long MAX_DEPRECATED_RETENTION_MS = 2_000L;

  @Test
  public void testDeleteVersionlessHybridStoreMarksExistingRealTimeTopic() {
    DeletionFixture fixture = new DeletionFixture();
    PubSubTopic realTimeTopic = fixture.addTopic(STORE_NAME + "_rt_v3");
    assertTrue(fixture.persistedStore.isHybrid());
    assertTrue(fixture.persistedStore.getVersions().isEmpty());

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false);

    assertEquals(fixture.retentionUpdateAttempts, Collections.singletonList(realTimeTopic));
    assertEquals(fixture.retentionByTopic.get(realTimeTopic).longValue(), DEPRECATED_RETENTION_MS);
    fixture.assertDeletionCompleted();
    assertTrue(fixture.graveyardStore.isHybrid());
    assertTrue(fixture.graveyardStore.getVersions().isEmpty());
  }

  @Test
  public void testDeleteStoreMarksAllRealTimeTopicsBeforeWaitingAndRetriesWithNewAdmin() {
    DeletionFixture fixture = new DeletionFixture();
    fixture.addHybridVersion(1, STORE_NAME + "_rt_v1");
    fixture.addHybridVersion(2, STORE_NAME + "_rt_v2");
    Set<PubSubTopic> targets = new LinkedHashSet<>(
        Arrays.asList(
            fixture.addTopic(STORE_NAME + "_rt"),
            fixture.addTopic(STORE_NAME + "_rt_v1"),
            fixture.addTopic(STORE_NAME + "_rt_v2"),
            fixture.addTopic(STORE_NAME + "_rt_v2_sep")));
    PubSubTopic separateTopic = fixture.topicRepository.getTopic(STORE_NAME + "_rt_v2_sep");
    PubSubTopic otherStoreTopic = fixture.addTopic(STORE_NAME + "_other_rt");
    PubSubTopic versionTopic = fixture.addTopic(STORE_NAME + "_v1");
    VeniceHelixAdmin firstAdmin = fixture.newAdmin();

    expectThrows(
        VeniceRetriableException.class,
        () -> firstAdmin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true));

    assertEquals(new HashSet<>(fixture.retentionUpdateAttempts), targets);
    assertEquals(fixture.retentionUpdateAttempts.size(), targets.size());
    for (PubSubTopic topic: targets) {
      assertEquals(fixture.retentionByTopic.get(topic).longValue(), DEPRECATED_RETENTION_MS);
    }
    assertEquals(fixture.deletingFlagsAtVersionRemoval, Arrays.asList(true, true));
    fixture.assertDeletionPending();
    assertEquals(fixture.persistedStore.getCurrentVersion(), Store.NON_EXISTING_VERSION);
    assertEquals(fixture.retentionByTopic.get(otherStoreTopic).longValue(), ORIGINAL_RETENTION_MS);
    assertEquals(fixture.retentionByTopic.get(versionTopic).longValue(), ORIGINAL_RETENTION_MS);

    VeniceHelixAdmin recoveredAdmin = fixture.newAdmin();
    assertNotSame(recoveredAdmin, firstAdmin);
    expectThrows(
        VeniceRetriableException.class,
        () -> recoveredAdmin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true));
    fixture.assertDeletionPending();
    assertEquals(fixture.retentionUpdateAttempts.size(), targets.size(), "Already marked topics need no new update");

    // Even an already-truncated _sep topic must disappear from inventory before metadata can be removed.
    targets.stream().filter(topic -> !topic.equals(separateTopic)).forEach(fixture.retentionByTopic::remove);
    expectThrows(
        VeniceRetriableException.class,
        () -> recoveredAdmin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true));
    fixture.assertDeletionPending();

    fixture.retentionByTopic.remove(separateTopic);
    recoveredAdmin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true);

    fixture.assertDeletionCompleted();
    assertEquals(
        fixture.retentionByTopic.keySet(),
        new HashSet<>(Arrays.asList(otherStoreTopic, versionTopic)),
        "Unrelated topics must neither block deletion nor be truncated");
    assertEquals(fixture.graveyardStore.getLargestUsedVersionNumber(), 2);
    assertTrue(fixture.graveyardStore.getVersions().isEmpty());
  }

  @DataProvider(name = "retentionFailureModes")
  public Object[][] retentionFailureModes() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "retentionFailureModes")
  public void testRetentionFailureAttemptsRemainingTopicsAndRetrySucceedsWithoutVersions(boolean throwOnFailure) {
    DeletionFixture fixture = new DeletionFixture();
    fixture.addHybridVersion(1, STORE_NAME + "_rt_v1");
    PubSubTopic failedTopic = fixture.addTopic(STORE_NAME + "_rt_v1");
    PubSubTopic healthyTopic = fixture.addTopic(STORE_NAME + "_rt_v2");
    PubSubTopic separateTopic = fixture.addTopic(STORE_NAME + "_rt_v2_sep");
    fixture.failedRetentionUpdates.add(failedTopic);
    fixture.throwOnRetentionUpdateFailure = throwOnFailure;

    expectThrows(
        VeniceRetriableException.class,
        () -> fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false));

    assertEquals(
        new HashSet<>(fixture.retentionUpdateAttempts),
        new HashSet<>(Arrays.asList(failedTopic, healthyTopic, separateTopic)));
    assertEquals(fixture.retentionUpdateAttempts.size(), 3);
    assertEquals(fixture.retentionByTopic.get(failedTopic).longValue(), ORIGINAL_RETENTION_MS);
    assertEquals(fixture.retentionByTopic.get(healthyTopic).longValue(), DEPRECATED_RETENTION_MS);
    assertEquals(fixture.retentionByTopic.get(separateTopic).longValue(), DEPRECATED_RETENTION_MS);
    assertEquals(fixture.deletingFlagsAtVersionRemoval, Collections.singletonList(true));
    fixture.assertDeletionPending();

    fixture.failedRetentionUpdates.clear();
    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false);

    assertEquals(fixture.retentionUpdateAttempts.size(), 4);
    assertEquals(Collections.frequency(fixture.retentionUpdateAttempts, failedTopic), 2);
    assertEquals(fixture.retentionByTopic.get(failedTopic).longValue(), DEPRECATED_RETENTION_MS);
    fixture.assertDeletionCompleted();
    assertTrue(fixture.graveyardStore.getVersions().isEmpty());
  }

  @DataProvider(name = "systemStoreRetentionFailures")
  public Object[][] systemStoreRetentionFailures() {
    return new Object[][] { { VeniceSystemStoreType.META_STORE, true }, { VeniceSystemStoreType.META_STORE, false },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, true },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, false } };
  }

  @Test(dataProvider = "systemStoreRetentionFailures")
  public void testSystemStoreRetentionFailureKeepsUserStoreDeletionPending(
      VeniceSystemStoreType failedStoreType,
      boolean throwOnFailure) throws ReflectiveOperationException {
    DeletionFixture fixture = new DeletionFixture();
    fixture.addHybridVersion(1, STORE_NAME + "_rt_v1");
    fixture.persistedStore.setStoreMetaSystemStoreEnabled(true);
    fixture.persistedStore.setDaVinciPushStatusStoreEnabled(true);
    PubSubTopic userTopic = fixture.addTopic(STORE_NAME + "_rt_v1");
    PubSubTopic metaTopic = fixture.addTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(STORE_NAME) + "_rt");
    PubSubTopic pushStatusTopic =
        fixture.addTopic(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(STORE_NAME) + "_rt");
    PubSubTopic failedTopic = fixture.topicRepository.getTopic(failedStoreType.getSystemStoreName(STORE_NAME) + "_rt");
    fixture.failedRetentionUpdates.add(failedTopic);
    fixture.throwOnRetentionUpdateFailure = throwOnFailure;
    VeniceHelixAdmin firstAdmin = fixture.newAdminWithSystemStores();

    expectThrows(
        VeniceRetriableException.class,
        () -> firstAdmin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false));

    fixture.assertDeletionPending();
    assertEquals(fixture.retentionByTopic.get(userTopic).longValue(), DEPRECATED_RETENTION_MS);
    assertEquals(fixture.retentionByTopic.get(failedTopic).longValue(), ORIGINAL_RETENTION_MS);

    fixture.failedRetentionUpdates.clear();
    fixture.newAdminWithSystemStores().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false);

    fixture.assertDeletionCompleted();
    for (PubSubTopic topic: Arrays.asList(userTopic, metaTopic, pushStatusTopic)) {
      assertEquals(fixture.retentionByTopic.get(topic).longValue(), DEPRECATED_RETENTION_MS);
    }
    assertEquals(Collections.frequency(fixture.retentionUpdateAttempts, failedTopic), 2);
    assertEquals(Collections.frequency(fixture.retentionUpdateAttempts, userTopic), 1);
  }

  @Test
  public void testMissingStoreWithPersistedDeletionIntentKeepsConfigUntilTopicsAreDeleted() {
    DeletionFixture fixture = new DeletionFixture();
    fixture.persistedStore = null;
    fixture.persistedConfig.setDeleting(true);
    PubSubTopic failedTopic = fixture.addTopic(STORE_NAME + "_rt_v1");
    PubSubTopic separateTopic = fixture.addTopic(STORE_NAME + "_rt_v1_sep");
    fixture.failedRetentionUpdates.add(failedTopic);

    expectThrows(
        VeniceRetriableException.class,
        () -> fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true));

    assertEquals(
        new HashSet<>(fixture.retentionUpdateAttempts),
        new HashSet<>(Arrays.asList(failedTopic, separateTopic)));
    assertEquals(fixture.retentionUpdateAttempts.size(), 2);
    assertEquals(fixture.retentionByTopic.get(failedTopic).longValue(), ORIGINAL_RETENTION_MS);
    assertEquals(fixture.retentionByTopic.get(separateTopic).longValue(), DEPRECATED_RETENTION_MS);
    assertTrue(fixture.persistedConfig.isDeleting());
    assertEquals(fixture.persistedConfig.getCluster(), CLUSTER_NAME);
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);

    fixture.failedRetentionUpdates.clear();
    VeniceHelixAdmin recoveredAdmin = fixture.newAdmin();
    expectThrows(
        VeniceRetriableException.class,
        () -> recoveredAdmin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true));
    assertEquals(fixture.retentionByTopic.get(failedTopic).longValue(), DEPRECATED_RETENTION_MS);
    assertTrue(fixture.persistedConfig.isDeleting());
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);

    fixture.retentionByTopic.clear();
    recoveredAdmin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true);

    assertNull(fixture.persistedConfig);
    assertNull(fixture.persistedStore);
    assertNull(fixture.graveyardStore, "Missing metadata must not be written to the graveyard");
    verify(fixture.storeRepository, never()).deleteStore(STORE_NAME);
  }

  @DataProvider(name = "migrationCleanupModes")
  public Object[][] migrationCleanupModes() {
    return new Object[][] { { false, false }, { false, true }, { true, false }, { true, true } };
  }

  @Test(dataProvider = "migrationCleanupModes")
  public void testMigrationCleanupPreservesSharedTopicsAndConfig(boolean abortMigration, boolean alreadyDeleting) {
    DeletionFixture fixture = new DeletionFixture();
    fixture.addHybridVersion(1, STORE_NAME + "_rt_v1");
    fixture.persistedStore.setMigrating(true);
    fixture.persistedConfig.setDeleting(alreadyDeleting);
    StoreConfig originalConfig = fixture.persistedConfig.cloneStoreConfig();
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, abortMigration, Store.IGNORE_VERSION, true);

    fixture.assertNoRealTimeSweep(topic);
    assertNull(fixture.persistedStore, "Local migrated metadata should still be removed");
    assertEquals(fixture.persistedConfig, originalConfig, "Shared discovery config must survive migration cleanup");
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);
  }

  @Test
  public void testAbortMigrationWithoutMigrationFlagDoesNotDeleteStoreOrTopics() {
    DeletionFixture fixture = new DeletionFixture();
    fixture.addHybridVersion(1, STORE_NAME + "_rt_v1");
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, true, Store.IGNORE_VERSION, true);

    fixture.assertNoRealTimeSweep(topic);
    assertEquals(fixture.persistedStore.getVersions().size(), 1);
    assertFalse(fixture.persistedConfig.isDeleting());
    assertNull(fixture.graveyardStore);
    verify(fixture.storeRepository, never()).deleteStore(STORE_NAME);
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);
  }

  @DataProvider(name = "foreignConfigStates")
  public Object[][] foreignConfigStates() {
    return new Object[][] { { true, false }, { true, true }, { false, false }, { false, true } };
  }

  @Test(dataProvider = "foreignConfigStates")
  public void testForeignClusterConfigPreventsRealTimeSweepAndConfigRemoval(
      boolean storePresent,
      boolean alreadyDeleting) {
    DeletionFixture fixture = new DeletionFixture();
    fixture.persistedConfig.setCluster("other-owning-cluster");
    fixture.persistedConfig.setDeleting(alreadyDeleting);
    StoreConfig originalConfig = fixture.persistedConfig.cloneStoreConfig();
    if (!storePresent) {
      fixture.persistedStore = null;
    }
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true);

    fixture.assertNoRealTimeSweep(topic);
    assertNull(fixture.persistedStore);
    assertEquals(fixture.persistedConfig, originalConfig);
    verify(fixture.configAccessor, never()).updateConfig(any(StoreConfig.class), anyBoolean());
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);
  }

  @DataProvider(name = "configPresence")
  public Object[][] configPresence() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "configPresence")
  public void testMissingStoreWithoutPriorDeletionIntentDoesNotSweepTopics(boolean configPresent) {
    DeletionFixture fixture = new DeletionFixture();
    fixture.persistedStore = null;
    if (!configPresent) {
      fixture.persistedConfig = null;
    }
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true);

    fixture.assertNoRealTimeSweep(topic);
    assertNull(fixture.graveyardStore);
    verify(fixture.storeRepository, never()).deleteStore(STORE_NAME);
  }

  @Test
  public void testExistingStoreWithoutConfigStillCleansUpRealTimeTopics() {
    DeletionFixture fixture = new DeletionFixture();
    fixture.persistedConfig = null;
    fixture.addHybridVersion(1, STORE_NAME + "_rt_v1");
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false);

    assertEquals(fixture.retentionUpdateAttempts, Collections.singletonList(topic));
    assertEquals(fixture.retentionByTopic.get(topic).longValue(), DEPRECATED_RETENTION_MS);
    fixture.assertDeletionCompleted();
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);
  }

  @Test
  public void testLeadershipLossAfterRealTimeCleanupPreservesMetadataUntilNewLeaderRetries() {
    DeletionFixture fixture = new DeletionFixture();
    fixture.addHybridVersion(1, STORE_NAME + "_rt_v1");
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");
    VeniceHelixAdmin admin = fixture.newAdmin();
    // Leadership changes when the broker accepts the retention update, not after a brittle call count.
    doAnswer(invocation -> fixture.retentionByTopic.get(topic).longValue() == ORIGINAL_RETENTION_MS).when(admin)
        .isLeaderControllerFor(CLUSTER_NAME);

    expectThrows(VeniceException.class, () -> admin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false));

    assertFalse(admin.isLeaderControllerFor(CLUSTER_NAME));
    assertEquals(fixture.retentionUpdateAttempts, Collections.singletonList(topic));
    assertEquals(fixture.retentionByTopic.get(topic).longValue(), DEPRECATED_RETENTION_MS);
    fixture.assertDeletionPending();

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false);

    fixture.assertDeletionCompleted();
    assertTrue(fixture.graveyardStore.getVersions().isEmpty());
    assertEquals(fixture.retentionUpdateAttempts, Collections.singletonList(topic));
  }

  @Test
  public void testLeadershipLossAfterMetadataRemovalPreservesDeletingConfigForNewLeader() {
    DeletionFixture fixture = new DeletionFixture();
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");
    VeniceHelixAdmin admin = fixture.newAdmin();
    doAnswer(invocation -> fixture.persistedStore != null).when(admin).isLeaderControllerFor(CLUSTER_NAME);

    expectThrows(VeniceException.class, () -> admin.deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false));

    assertNull(fixture.persistedStore);
    assertEquals(fixture.graveyardStore.getName(), STORE_NAME);
    assertTrue(fixture.persistedConfig.isDeleting());
    assertEquals(fixture.persistedConfig.getCluster(), CLUSTER_NAME);
    assertEquals(fixture.retentionByTopic.get(topic).longValue(), DEPRECATED_RETENTION_MS);
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);

    fixture.newAdmin().deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, false);

    assertNull(fixture.persistedConfig);
    assertEquals(fixture.retentionUpdateAttempts, Collections.singletonList(topic));
  }

  @Test
  public void testCreateStoreResumesDeletionWithMissingMetadataBeforeRecreation() throws ReflectiveOperationException {
    DeletionFixture fixture = new DeletionFixture();
    fixture.persistedStore = null;
    fixture.persistedConfig.setDeleting(true);
    PubSubTopic topic = fixture.addTopic(STORE_NAME + "_rt_v1");
    String owner = "replacement-owner";
    String keySchema = "\"string\"";
    String valueSchema = "\"long\"";
    ReadWriteSchemaRepository schemaRepository = mock(ReadWriteSchemaRepository.class);
    doReturn(schemaRepository).when(fixture.resources).getSchemaRepository();
    doReturn(5).when(fixture.graveyard).getLargestUsedVersionNumber(STORE_NAME);
    doReturn(3).when(fixture.graveyard).getLargestUsedRTVersionNumber(STORE_NAME);
    doAnswer(invocation -> {
      assertNull(fixture.persistedConfig, "Pending deletion must finish before replacement metadata is published");
      Store store = invocation.getArgument(0);
      fixture.persistedStore = store.cloneStore();
      return null;
    }).when(fixture.storeRepository).addStore(any(Store.class));
    doAnswer(invocation -> {
      fixture.persistedConfig = new StoreConfig(STORE_NAME);
      fixture.persistedConfig.setCluster(invocation.getArgument(1));
      return null;
    }).when(fixture.configAccessor).createConfig(STORE_NAME, CLUSTER_NAME);
    VeniceHelixAdmin admin = fixture.newAdminForCreation();

    expectThrows(
        VeniceRetriableException.class,
        () -> admin.createStore(CLUSTER_NAME, STORE_NAME, owner, keySchema, valueSchema, false, Optional.empty()));

    assertEquals(fixture.retentionUpdateAttempts, Collections.singletonList(topic));
    assertEquals(fixture.retentionByTopic.get(topic).longValue(), DEPRECATED_RETENTION_MS);
    assertNull(fixture.persistedStore);
    assertTrue(fixture.persistedConfig.isDeleting());
    verify(admin).deleteStore(CLUSTER_NAME, STORE_NAME, Store.IGNORE_VERSION, true);
    verify(fixture.storeRepository, never()).addStore(any(Store.class));
    verify(fixture.configAccessor, never()).deleteConfig(STORE_NAME);
    verify(schemaRepository, never()).initKeySchema(STORE_NAME, keySchema);
    verify(schemaRepository, never()).addValueSchema(eq(STORE_NAME), eq(valueSchema), anyInt());

    fixture.retentionByTopic.remove(topic);
    admin.createStore(CLUSTER_NAME, STORE_NAME, owner, keySchema, valueSchema, false, Optional.empty());

    assertEquals(fixture.persistedStore.getName(), STORE_NAME);
    assertEquals(fixture.persistedStore.getOwner(), owner);
    assertEquals(fixture.persistedStore.getLargestUsedVersionNumber(), 5);
    assertEquals(fixture.persistedStore.getLargestUsedRTVersionNumber(), 4);
    assertEquals(fixture.persistedConfig.getCluster(), CLUSTER_NAME);
    assertFalse(fixture.persistedConfig.isDeleting());
    assertNull(fixture.graveyardStore, "Resuming an absent store must not overwrite its graveyard entry");
    verify(schemaRepository).initKeySchema(STORE_NAME, keySchema);
    verify(schemaRepository).addValueSchema(STORE_NAME, valueSchema, 1);
  }

  /**
   * Clone on repository reads/writes so mutating a fetched object does not silently persist deletion intent.
   * New admin instances share only repository/config/broker state, never an in-memory RT topic snapshot.
   */
  private static final class DeletionFixture {
    private final ReadWriteStoreRepository storeRepository = mock(ReadWriteStoreRepository.class);
    private final ZkStoreConfigAccessor configAccessor = mock(ZkStoreConfigAccessor.class);
    private final StoreGraveyard graveyard = mock(StoreGraveyard.class);
    private final HelixVeniceClusterResources resources = mock(HelixVeniceClusterResources.class);
    private final VeniceControllerMultiClusterConfig configs = mock(VeniceControllerMultiClusterConfig.class);
    private final TopicManager topicManager = mock(TopicManager.class);
    private final PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    private final Map<PubSubTopic, Long> retentionByTopic = new LinkedHashMap<>();
    private final Set<PubSubTopic> failedRetentionUpdates = new HashSet<>();
    private final List<PubSubTopic> retentionUpdateAttempts = new ArrayList<>();
    private final List<Boolean> deletingFlagsAtVersionRemoval = new ArrayList<>();
    private Store persistedStore = TestUtils.createTestStore(STORE_NAME, "owner", CREATED_TIME);
    private StoreConfig persistedConfig = new StoreConfig(STORE_NAME);
    private Store graveyardStore;
    private boolean throwOnRetentionUpdateFailure = true;

    private DeletionFixture() {
      persistedStore.setHybridStoreConfig(new HybridStoreConfigImpl(60, 10, -1, BufferReplayPolicy.REWIND_FROM_EOP));
      persistedStore.setSeparateRealTimeTopicEnabled(true);
      persistedStore.setEnableReads(false);
      persistedStore.setEnableWrites(false);
      persistedStore.setStoreMetaSystemStoreEnabled(false);
      persistedStore.setDaVinciPushStatusStoreEnabled(false);
      persistedConfig.setCluster(CLUSTER_NAME);

      doReturn(new ClusterLockManager(CLUSTER_NAME)).when(resources).getClusterLockManager();
      doReturn(storeRepository).when(resources).getStoreMetadataRepository();
      doReturn(configAccessor).when(resources).getStoreConfigAccessor();
      doReturn(mock(PushMonitorDelegator.class)).when(resources).getPushMonitor();
      doReturn(Collections.emptySet()).when(configs).getParentFabrics();
      doReturn(DEPRECATED_RETENTION_MS).when(configs).getDeprecatedJobTopicRetentionMs();
      doReturn(MAX_DEPRECATED_RETENTION_MS).when(configs).getDeprecatedJobTopicMaxRetentionMs();

      doAnswer(invocation -> persistedStore == null ? null : persistedStore.cloneStore()).when(storeRepository)
          .getStore(STORE_NAME);
      doAnswer(invocation -> {
        Store store = invocation.getArgument(0);
        persistedStore = store.cloneStore();
        return null;
      }).when(storeRepository).updateStore(any(Store.class));
      doAnswer(invocation -> {
        persistedStore = null;
        return null;
      }).when(storeRepository).deleteStore(STORE_NAME);
      doAnswer(invocation -> persistedConfig == null ? null : persistedConfig.cloneStoreConfig()).when(configAccessor)
          .getStoreConfig(STORE_NAME);
      doAnswer(invocation -> persistedConfig != null).when(configAccessor).containsConfig(STORE_NAME);
      doAnswer(invocation -> {
        StoreConfig config = invocation.getArgument(0);
        persistedConfig = config.cloneStoreConfig();
        return null;
      }).when(configAccessor).updateConfig(any(StoreConfig.class), anyBoolean());
      doAnswer(invocation -> {
        persistedConfig = null;
        return null;
      }).when(configAccessor).deleteConfig(STORE_NAME);
      doAnswer(invocation -> {
        Store store = invocation.getArgument(1);
        graveyardStore = store.cloneStore();
        return null;
      }).when(graveyard).putStoreIntoGraveyard(eq(CLUSTER_NAME), any(Store.class));

      doAnswer(invocation -> new LinkedHashSet<>(retentionByTopic.keySet())).when(topicManager).listTopics();
      doAnswer(invocation -> {
        PubSubTopic topic = invocation.getArgument(0);
        long thresholdMs = invocation.getArgument(1);
        Long retentionMs = retentionByTopic.get(topic);
        return retentionMs == null || retentionMs <= thresholdMs;
      }).when(topicManager).isTopicTruncated(any(PubSubTopic.class), anyLong());
      doAnswer(invocation -> {
        PubSubTopic topic = invocation.getArgument(0);
        long retentionMs = invocation.getArgument(1);
        retentionUpdateAttempts.add(topic);
        if (failedRetentionUpdates.contains(topic)) {
          if (throwOnRetentionUpdateFailure) {
            throw new VeniceException("Injected retention update failure");
          }
          // A false update result is not itself a failure: the unchanged readback makes this one pending.
          return false;
        }
        Long previousRetentionMs = retentionByTopic.put(topic, retentionMs);
        return previousRetentionMs == null || previousRetentionMs.longValue() != retentionMs;
      }).when(topicManager).updateTopicRetention(any(PubSubTopic.class), anyLong());
    }

    private VeniceHelixAdmin newAdmin() {
      VeniceHelixAdmin admin = mock(VeniceHelixAdmin.class);
      doReturn(resources).when(admin).getHelixVeniceClusterResources(CLUSTER_NAME);
      doReturn(configs).when(admin).getMultiClusterConfigs();
      doReturn(topicManager).when(admin).getTopicManager();
      doReturn(graveyard).when(admin).getStoreGraveyard();
      doReturn(Optional.empty()).when(admin).getExternalETLService();
      doReturn(Optional.empty()).when(admin).getAuthorizerService();
      doReturn(false).when(admin).isParent();
      doReturn(true).when(admin).isLeaderControllerFor(CLUSTER_NAME);
      doCallRealMethod().when(admin).checkControllerLeadershipFor(CLUSTER_NAME);
      doCallRealMethod().when(admin).deleteStore(eq(CLUSTER_NAME), eq(STORE_NAME), anyInt(), anyBoolean());
      doCallRealMethod().when(admin)
          .deleteStore(eq(CLUSTER_NAME), eq(STORE_NAME), anyBoolean(), anyInt(), anyBoolean());
      doCallRealMethod().when(admin).cleanupRealTimeTopicsForStoreDeletion(eq(CLUSTER_NAME), anyString(), anyBoolean());
      doCallRealMethod().when(admin).deleteAllVersionsInStore(CLUSTER_NAME, STORE_NAME);
      doAnswer(invocation -> {
        Store store = storeRepository.getStore(STORE_NAME);
        StoreConfig config = configAccessor.getStoreConfig(STORE_NAME);
        deletingFlagsAtVersionRemoval.add(config != null && config.isDeleting());
        int versionNumber = invocation.getArgument(2);
        store.deleteVersion(versionNumber);
        storeRepository.updateStore(store);
        return null;
      }).when(admin).deleteOneStoreVersion(eq(CLUSTER_NAME), eq(STORE_NAME), anyInt());
      // Resource cleanup, lifecycle hooks and VT truncation stay mocked; the complete RT deletion path is real.
      return admin;
    }

    private VeniceHelixAdmin newAdminWithSystemStores() throws ReflectiveOperationException {
      VeniceHelixAdmin admin = newAdmin();
      Field metaWriterField = VeniceHelixAdmin.class.getDeclaredField("metaStoreWriter");
      metaWriterField.setAccessible(true);
      metaWriterField.set(admin, mock(MetaStoreWriter.class));
      doReturn(mock(PushStatusStoreWriter.class)).when(admin).getPushStatusStoreWriter();
      return admin;
    }

    private VeniceHelixAdmin newAdminForCreation() throws ReflectiveOperationException {
      VeniceHelixAdmin admin = newAdmin();
      doReturn(configAccessor).when(admin).getStoreConfigAccessor(CLUSTER_NAME);
      VeniceControllerClusterConfig clusterConfig = mock(VeniceControllerClusterConfig.class);
      Store defaults = TestUtils.createTestStore(STORE_NAME, "owner", CREATED_TIME);
      doReturn(defaults.getPersistenceType()).when(clusterConfig).getPersistenceType();
      doReturn(defaults.getRoutingStrategy()).when(clusterConfig).getRoutingStrategy();
      doReturn(defaults.getReadStrategy()).when(clusterConfig).getReadStrategy();
      doReturn(defaults.getOffLinePushStrategy()).when(clusterConfig).getOfflinePushStrategy();
      doReturn(defaults.getReplicationFactor()).when(clusterConfig).getReplicationFactor();
      doReturn("source-fabric").when(clusterConfig).getNativeReplicationSourceFabricAsDefaultForBatchOnly();
      doReturn(true).when(clusterConfig).isRealTimeTopicVersioningEnabled();
      doReturn(clusterConfig).when(resources).getConfig();

      // createStore still accesses these two existing fields directly, unlike the deletion path.
      Field schemaManagerField = VeniceHelixAdmin.class.getDeclaredField("storeSchemaManager");
      schemaManagerField.setAccessible(true);
      schemaManagerField.set(admin, new StoreSchemaManager(admin));
      Field graveyardField = VeniceHelixAdmin.class.getDeclaredField("storeGraveyard");
      graveyardField.setAccessible(true);
      graveyardField.set(admin, graveyard);
      doCallRealMethod().when(admin)
          .checkPreConditionForCreateStore(eq(CLUSTER_NAME), eq(STORE_NAME), any(), any(), anyBoolean(), anyBoolean());
      doCallRealMethod().when(admin)
          .createStore(eq(CLUSTER_NAME), eq(STORE_NAME), any(), any(), any(), anyBoolean(), any());
      return admin;
    }

    private void addHybridVersion(int number, String realTimeTopicName) {
      persistedStore.getHybridStoreConfig().setRealTimeTopicName(realTimeTopicName);
      persistedStore.setEnableWrites(true);
      Version version = new VersionImpl(STORE_NAME, number, CREATED_TIME, "push-" + number, 1, null, null);
      persistedStore.addVersion(version);
      persistedStore.setEnableWrites(false);
    }

    private PubSubTopic addTopic(String name) {
      PubSubTopic topic = topicRepository.getTopic(name);
      retentionByTopic.put(topic, ORIGINAL_RETENTION_MS);
      return topic;
    }

    private void assertDeletionPending() {
      assertNotNull(persistedStore);
      assertEquals(persistedStore.getName(), STORE_NAME);
      assertTrue(persistedStore.getVersions().isEmpty(), "Version removal must survive a failed deletion attempt");
      assertTrue(persistedConfig.isDeleting(), "Durable deletion intent must survive a failed attempt");
      assertEquals(persistedConfig.getCluster(), CLUSTER_NAME);
      assertNull(graveyardStore, "Pending RT cleanup must precede graveyard writes");
      verify(storeRepository, never()).deleteStore(STORE_NAME);
      verify(configAccessor, never()).deleteConfig(STORE_NAME);
    }

    private void assertDeletionCompleted() {
      assertNull(persistedStore);
      assertNull(persistedConfig);
      assertNotNull(graveyardStore);
      assertEquals(graveyardStore.getName(), STORE_NAME);
    }

    private void assertNoRealTimeSweep(PubSubTopic topic) {
      assertTrue(retentionUpdateAttempts.isEmpty());
      assertEquals(retentionByTopic.get(topic).longValue(), ORIGINAL_RETENTION_MS);
      verify(topicManager, never()).listTopics();
      verify(topicManager, never()).updateTopicRetention(any(PubSubTopic.class), anyLong());
    }
  }
}
