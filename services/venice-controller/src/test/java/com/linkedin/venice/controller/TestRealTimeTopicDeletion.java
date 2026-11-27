package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreInfoResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.helix.CachedReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.SystemStoreAttributesImpl;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongPredicate;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.mockito.MockedConstruction;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestRealTimeTopicDeletion {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final Optional<String> KNOWN_CLUSTER = Optional.of(CLUSTER);
  private static final long RETENTION = TimeUnit.HOURS.toMillis(12);
  private static final PubSubTopicRepository TOPICS = new PubSubTopicRepository();
  private static final PubSubTopic RT = TOPICS.getTopic(STORE + "_rt_v1");

  private VeniceHelixAdmin admin;
  private VeniceControllerMultiClusterConfig config;
  private TopicManager local;
  private VeniceControllerClusterConfig clusterConfig;
  private HelixReadOnlyStoreConfigRepository storeConfigs;
  private StoreGraveyard graveyard;
  private ZkClient zkClient;

  @BeforeMethod
  public void setUp() {
    admin = mock(VeniceHelixAdmin.class);
    config = mock(VeniceControllerMultiClusterConfig.class);
    local = mock(TopicManager.class);
    doReturn(config).when(admin).getMultiClusterConfigs();
    doReturn(local).when(admin).getTopicManager();
    doReturn(true).when(admin).isLeaderControllerFor(CLUSTER);
    doCallRealMethod().when(admin).checkControllerLeadershipFor(CLUSTER);
    doCallRealMethod().when(admin).cleanupRealTimeTopicsForStoreDeletion(anyString(), anyString(), anyBoolean());
    doReturn(RETENTION).when(config).getDeprecatedRealTimeTopicRetentionMs();
    doReturn(Collections.singleton(RT)).when(local).listTopics();
    doReturn(false, true).when(local).isTopicTruncated(eq(RT), any(LongPredicate.class));
    clusterConfig = mock(VeniceControllerClusterConfig.class);
    storeConfigs = mock(HelixReadOnlyStoreConfigRepository.class);
    graveyard = mock(StoreGraveyard.class);
    zkClient = mock(ZkClient.class);
    doReturn(clusterConfig).when(config).getControllerConfig(anyString());
    doReturn(Collections.singleton(CLUSTER)).when(config).getClusters();
    doReturn("local").when(config).getRegionName();
    doReturn(storeConfigs).when(admin).getStoreConfigRepo();
    doReturn(graveyard).when(admin).getStoreGraveyard();
    doReturn(zkClient).when(admin).getZkClient();
    doReturn(Arrays.asList(CLUSTER, "unled-cluster")).when(zkClient).getChildren("/");
    doCallRealMethod().when(admin).isRTTopicDeletionPermittedByAllControllers(any(), anyString());
    Store deletedStore = TestUtils.createTestStore(STORE, "owner", 1);
    deletedStore.setHybridStoreConfig(hybridConfig(STORE + "_rt"));
    deletedStore.setLargestUsedRTVersionNumber(1);
    deletedStore.putSystemStore(VeniceSystemStoreType.META_STORE, new SystemStoreAttributesImpl());
    deletedStore.putSystemStore(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, new SystemStoreAttributesImpl());
    doReturn(deletedStore).when(graveyard).getStoreFromGraveyard(CLUSTER, STORE, null);
  }

  @Test
  public void testAlreadyMarkedAndAbsentTopicsAreSuccessful() {
    doReturn(true).when(local).isTopicTruncated(eq(RT), any(LongPredicate.class));
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);
    verify(local, never()).updateTopicRetention(RT, RETENTION);

    doReturn(false, true).when(local).isTopicTruncated(eq(RT), any(LongPredicate.class));
    doReturn(false).when(local).updateTopicRetention(RT, RETENTION);
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);

    doReturn(false).when(local).isTopicTruncated(eq(RT), any(LongPredicate.class));
    doThrow(new PubSubTopicDoesNotExistException(RT)).when(local).updateTopicRetention(RT, RETENTION);
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);
  }

  @Test
  public void testUnconfirmedRetentionIsRetried() {
    doReturn(true).when(local).updateTopicRetention(RT, RETENTION);
    doReturn(false).when(local).isTopicTruncated(eq(RT), any(LongPredicate.class));
    VeniceRetriableException failure = expectThrows(
        VeniceRetriableException.class,
        () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
    assertEquals(failure.getSuppressed().length, 1);

    doReturn(true).when(local).isTopicTruncated(eq(RT), any(LongPredicate.class));
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);
  }

  @Test
  public void testReadbackFailureIsNotTreatedAsAbsence() {
    doReturn(false).doThrow(new PubSubClientException("Cannot read retention"))
        .when(local)
        .isTopicTruncated(eq(RT), any(LongPredicate.class));
    expectThrows(
        VeniceRetriableException.class,
        () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
    verify(local).updateTopicRetention(RT, RETENTION);
  }

  @Test
  public void testOnlyExactStoreRealTimeTopicsAreMarked() {
    PubSubTopic separate = TOPICS.getTopic(STORE + "_rt_v2_sep");
    PubSubTopic similarStore = TOPICS.getTopic(STORE + "-other_rt");
    PubSubTopic systemStore = TOPICS.getTopic("venice_system_store_meta_store_" + STORE + "_rt");
    PubSubTopic version = TOPICS.getTopic(STORE + "_v1");
    doReturn(new LinkedHashSet<>(Arrays.asList(RT, separate, similarStore, systemStore, version))).when(local)
        .listTopics();
    doReturn(false, true).when(local).isTopicTruncated(eq(separate), any(LongPredicate.class));

    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);

    verify(local).updateTopicRetention(RT, RETENTION);
    verify(local).updateTopicRetention(separate, RETENTION);
    verify(local, never()).updateTopicRetention(similarStore, RETENTION);
    verify(local, never()).updateTopicRetention(systemStore, RETENTION);
    verify(local, never()).updateTopicRetention(version, RETENTION);
  }

  @Test
  public void testParentMarksEveryFabricBeforeWaiting() {
    TopicManager first = mock(TopicManager.class);
    TopicManager second = mock(TopicManager.class);
    configureParent(first, second);
    doReturn(Collections.singleton(RT)).when(first).listTopics();
    doReturn(Collections.singleton(RT)).when(second).listTopics();
    doReturn(false, true).when(first).isTopicTruncated(eq(RT), any(LongPredicate.class));
    doReturn(false, true).when(second).isTopicTruncated(eq(RT), any(LongPredicate.class));

    expectThrows(
        VeniceRetriableException.class,
        () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, true));

    verify(first).updateTopicRetention(RT, RETENTION);
    verify(second).updateTopicRetention(RT, RETENTION);
    verify(local, never()).containsTopic(any());
    verify(local, never()).listTopics();
  }

  @Test
  public void testRemoteOnlyParentTopicIsMarked() {
    TopicManager first = mock(TopicManager.class);
    TopicManager second = mock(TopicManager.class);
    configureParent(first, second);
    doReturn(Collections.singleton(RT)).when(second).listTopics();
    doReturn(false, true).when(second).isTopicTruncated(eq(RT), any(LongPredicate.class));

    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);

    verify(second).updateTopicRetention(RT, RETENTION);
    verify(first, never()).updateTopicRetention(any(), anyLong());
    verify(local, never()).containsTopic(any());
  }

  @Test
  public void testFabricListingFailureDoesNotSkipOtherFabrics() {
    TopicManager first = mock(TopicManager.class);
    TopicManager second = mock(TopicManager.class);
    configureParent(first, second);
    doThrow(new PubSubClientException("Broker unavailable")).when(first).listTopics();
    doReturn(Collections.singleton(RT)).when(second).listTopics();
    doReturn(false, true).when(second).isTopicTruncated(eq(RT), any(LongPredicate.class));

    expectThrows(
        VeniceRetriableException.class,
        () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
    verify(second).updateTopicRetention(RT, RETENTION);
  }

  @Test
  public void testMissingParentFabricConfigurationFails() {
    doReturn(true).when(admin).isParent();
    doReturn(Collections.singleton("missing")).when(config).getParentFabrics();

    expectThrows(VeniceException.class, () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
    verify(local, never()).updateTopicRetention(any(), anyLong());
  }

  @Test
  public void testParentWithoutFabricListUsesLocalCluster() {
    doReturn(true).when(admin).isParent();
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);
    verify(local).updateTopicRetention(RT, RETENTION);
  }

  @Test
  public void testLeadershipLossStopsFurtherMutations() {
    PubSubTopic second = TOPICS.getTopic(STORE + "_rt_v2");
    doReturn(new LinkedHashSet<>(Arrays.asList(RT, second))).when(local).listTopics();
    doAnswer(invocation -> {
      doReturn(false).when(admin).isLeaderControllerFor(CLUSTER);
      return true;
    }).when(local).updateTopicRetention(RT, RETENTION);

    expectThrows(VeniceException.class, () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
    verify(local, never()).updateTopicRetention(second, RETENTION);
  }

  @DataProvider
  public Object[][] parentDeletionStates() {
    return new Object[][] { { false, CLUSTER, false, false, false, false },
        { true, "another-cluster", false, false, false, false }, { true, CLUSTER, true, false, false, false },
        { true, CLUSTER, false, true, false, false }, { true, CLUSTER, false, false, true, false },
        { true, CLUSTER, false, false, false, true } };
  }

  @Test(dataProvider = "parentDeletionStates")
  public void testParentRequiresOwnedDeletionAndRemovedVersions(
      boolean deleting,
      String owningCluster,
      boolean migrating,
      boolean hasVersion,
      boolean enabled,
      boolean allowed) {
    Store store = TestUtils.createTestStore(STORE, "owner", 1);
    if (hasVersion) {
      store.addVersion(new VersionImpl(STORE, 1, "push"));
    }
    store.setEnableReads(enabled);
    store.setEnableWrites(enabled);
    store.setMigrating(migrating);
    VeniceParentHelixAdmin parent = parentWithStore(store, deleting, owningCluster);
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName());

    assertEquals(parent.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName()), allowed);
    if (!allowed) {
      verify(admin, never()).isRTTopicDeletionPermittedByAllControllers(any(), anyString());
    }
  }

  @Test
  public void testParentCanCleanPendingDeletionWithoutStoreMetadata() {
    VeniceParentHelixAdmin parent = parentWithStore(null, true, CLUSTER);
    doReturn(false).when(admin).isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName());
    assertFalse(parent.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName()));

    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName());
    assertTrue(parent.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName()));
  }

  @Test
  public void testCrossFabricEligibilityPreservesHybridToBatchProtection() {
    ControllerClient client = mock(ControllerClient.class);
    doReturn(Collections.singletonMap("child", client)).when(admin).getControllerClientMap(CLUSTER);
    StoreInfo store = new StoreInfo();
    Version hybrid = new VersionImpl(STORE, 1, "hybrid-push");
    hybrid.setHybridStoreConfig(new HybridStoreConfigImpl(10, 10, -1, BufferReplayPolicy.REWIND_FROM_EOP));
    store.setVersions(Arrays.asList(hybrid, new VersionImpl(STORE, 2, "batch-push")));
    StoreResponse response = new StoreResponse();
    response.setStore(store);
    doReturn(response).when(client).getStore(STORE);

    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName()));
    store
        .setVersions(Arrays.asList(new VersionImpl(STORE, 2, "batch-push"), new VersionImpl(STORE, 3, "batch-push-2")));
    assertTrue(admin.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName()));

    doThrow(new VeniceException("Controller unavailable")).when(client).getStore(STORE);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName()));
    StoreResponse error = new StoreResponse();
    error.setError("Failed to read metadata");
    doReturn(error).when(client).getStore(STORE);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, RT.getName()));
  }

  @Test
  public void testParentDelegatesOrphanChecksWhenStoreConfigIsAbsent() {
    VeniceParentHelixAdmin parent = parentWithStore(null, true, CLUSTER);
    HelixReadOnlyStoreConfigRepository parentConfigs = parent.getStoreConfigRepo();
    doReturn(Optional.empty()).when(parentConfigs).getStoreConfig(STORE);
    assertTrue(parent.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
    verify(admin).isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName());
  }

  @DataProvider
  public Object[][] orphanStoreNames() {
    return new Object[][] { { STORE }, { VeniceSystemStoreType.META_STORE.getSystemStoreName(STORE) },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName(STORE) } };
  }

  @Test(dataProvider = "orphanStoreNames")
  public void testOrphanRequiresStoreMetadataAndConfigToBeAbsentInEveryLocalCluster(String storeName) {
    String rt = storeName + "_rt";
    assertTrue(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), rt));

    String metadataPath = "/unled-cluster/Stores/" + STORE;
    doReturn(true).when(zkClient).exists(metadataPath);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), rt));
    doReturn(false).when(zkClient).exists(metadataPath);

    String configPath = "/storeConfigs/" + STORE;
    doReturn(true).when(zkClient).exists(configPath);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), rt));
    doReturn(false).when(zkClient).exists(configPath);

    doReturn(Optional.of(new StoreConfig(STORE))).when(storeConfigs).getStoreConfig(STORE);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), rt));
    verify(admin, never()).checkControllerLeadershipFor(anyString());
  }

  @Test
  public void testOrphanRequiresPositiveTopicOwnership() {
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), "foreign_rt"));
    assertTrue(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), STORE + "_rt_v2"));
    doReturn(null).when(graveyard).getStoreFromGraveyard(CLUSTER, STORE, null);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
  }

  @Test
  public void testOrphanMetadataLookupFailureDoesNotIndicateAbsence() {
    doThrow(new VeniceException("Metadata unavailable")).when(zkClient).getChildren("/");
    expectThrows(
        VeniceException.class,
        () -> admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
  }

  @Test
  public void testOrphanRejectsIncompleteLocalMetadataSnapshot() {
    doReturn(true).when(zkClient).exists("/unled-cluster/Stores");
    doReturn(Collections.singletonList("unread-store")).when(zkClient).getChildren("/unled-cluster/Stores");
    try (MockedConstruction<CachedReadOnlyStoreRepository> repositories =
        mockConstruction(CachedReadOnlyStoreRepository.class)) {
      expectThrows(
          VeniceException.class,
          () -> admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
      verify(repositories.constructed().get(0)).refresh();
    }
  }

  @DataProvider
  public Object[][] customReferences() {
    return new Object[][] { { false, false, false }, { false, true, false }, { false, true, true },
        { true, false, false }, { true, true, false }, { true, true, true } };
  }

  @Test(dataProvider = "customReferences")
  public void testOrphanChecksProtectExactCustomRTReferences(boolean remote, boolean perVersion, boolean separate) {
    String rt = separate ? RT.getName() + "_sep" : RT.getName();
    Store activeStore = TestUtils.createTestStore("custom-rt-owner", "owner", 1);
    if (perVersion) {
      Version version = new VersionImpl(activeStore.getName(), 1, "hybrid-push");
      version.setHybridStoreConfig(hybridConfig(RT.getName()));
      activeStore.addVersion(version);
    } else {
      activeStore.setHybridStoreConfig(hybridConfig(RT.getName()));
    }
    if (remote) {
      ControllerClient client = configureChildController();
      MultiStoreInfoResponse response = new MultiStoreInfoResponse();
      response.setStoreInfoList(Collections.singletonList(StoreInfo.fromStore(activeStore)));
      doReturn(response).when(client).getClusterStores(CLUSTER);
      assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), rt));
    } else {
      doReturn(true).when(zkClient).exists("/unled-cluster/Stores");
      try (MockedConstruction<CachedReadOnlyStoreRepository> repositories = mockConstruction(
          CachedReadOnlyStoreRepository.class,
          (repository, context) -> doReturn(Collections.singletonList(activeStore)).when(repository).getAllStores())) {
        assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), rt));
        verify(repositories.constructed().get(0)).refresh();
      }
    }
  }

  @Test
  public void testOrphanChecksProtectBatchStoreMetadataInChild() {
    ControllerClient client = configureChildController();
    MultiStoreInfoResponse response = new MultiStoreInfoResponse();
    response
        .setStoreInfoList(Collections.singletonList(StoreInfo.fromStore(TestUtils.createTestStore(STORE, "owner", 1))));
    doReturn(response).when(client).getClusterStores(CLUSTER);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
  }

  @Test
  public void testOrphanChecksAllConfiguredChildClusters() {
    ControllerClient client = configureChildController();
    doReturn(new LinkedHashSet<>(Arrays.asList(CLUSTER, "another-cluster"))).when(config).getClusters();
    doReturn(Collections.singletonMap("child", client)).when(admin).getControllerClientMap("another-cluster");
    MultiStoreInfoResponse response = new MultiStoreInfoResponse();
    response
        .setStoreInfoList(Collections.singletonList(StoreInfo.fromStore(TestUtils.createTestStore(STORE, "owner", 1))));
    doReturn(response).when(client).getClusterStores("another-cluster");
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
    verify(client).getClusterStores("another-cluster");
  }

  @Test
  public void testOrphanRequiresCompleteConfiguredControllerCoverage() {
    doReturn(true).when(clusterConfig).isMultiRegion();
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
    ControllerClient client = configureChildController();
    doReturn(new LinkedHashSet<>(Arrays.asList("child", "missing-child"))).when(clusterConfig).getChildDatacenters();
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
    doReturn(Collections.singleton("child")).when(clusterConfig).getChildDatacenters();
    assertTrue(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));

    doReturn(new MultiStoreInfoResponse()).when(client).getClusterStores(CLUSTER);
    expectThrows(
        VeniceException.class,
        () -> admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
    MultiStoreInfoResponse error = new MultiStoreInfoResponse();
    error.setError("Cannot read metadata");
    doReturn(error).when(client).getClusterStores(CLUSTER);
    expectThrows(
        VeniceException.class,
        () -> admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
    doThrow(new VeniceException("Child unavailable")).when(client).getClusterStores(CLUSTER);
    expectThrows(
        VeniceException.class,
        () -> admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
  }

  @Test
  public void testOrphanChecksMetadataAgainAfterCrossFabricLookup() {
    ControllerClient client = configureChildController();
    MultiStoreInfoResponse response = new MultiStoreInfoResponse();
    response.setStoreInfoList(Collections.emptyList());
    doAnswer(invocation -> {
      doReturn(true).when(zkClient).exists("/storeConfigs/" + STORE);
      return response;
    }).when(client).getClusterStores(CLUSTER);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), RT.getName()));
  }

  @Test
  public void testAdminTopicsAreNeverEligibleForRTDeletion() {
    String adminTopic = "venice_admin_orders_rt";
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(Optional.empty(), adminTopic));
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(KNOWN_CLUSTER, adminTopic));
  }

  private ControllerClient configureChildController() {
    ControllerClient client = mock(ControllerClient.class);
    doReturn(Collections.singleton("child")).when(clusterConfig).getChildDatacenters();
    doReturn(Collections.singletonMap("child", client)).when(admin).getControllerClientMap(CLUSTER);
    MultiStoreInfoResponse response = new MultiStoreInfoResponse();
    response.setStoreInfoList(Collections.emptyList());
    doReturn(response).when(client).getClusterStores(anyString());
    return client;
  }

  private HybridStoreConfigImpl hybridConfig(String realTimeTopic) {
    HybridStoreConfigImpl hybrid = new HybridStoreConfigImpl(10, 10, -1, BufferReplayPolicy.REWIND_FROM_EOP);
    hybrid.setRealTimeTopicName(realTimeTopic);
    return hybrid;
  }

  private void configureParent(TopicManager first, TopicManager second) {
    doReturn(true).when(admin).isParent();
    doReturn(new LinkedHashSet<>(Arrays.asList("first", "second"))).when(config).getParentFabrics();
    Map<String, String> addresses = new HashMap<>();
    addresses.put("first", "first-kafka");
    addresses.put("second", "second-kafka");
    doReturn(addresses).when(config).getChildDataCenterKafkaUrlMap();
    doReturn(first).when(admin).getTopicManager("first-kafka");
    doReturn(second).when(admin).getTopicManager("second-kafka");
  }

  private VeniceParentHelixAdmin parentWithStore(Store store, boolean deleting, String owningCluster) {
    VeniceParentHelixAdmin parent = mock(VeniceParentHelixAdmin.class);
    HelixReadOnlyStoreConfigRepository repository = mock(HelixReadOnlyStoreConfigRepository.class);
    StoreConfig storeConfig = new StoreConfig(STORE);
    storeConfig.setCluster(owningCluster);
    storeConfig.setDeleting(deleting);
    doReturn(repository).when(parent).getStoreConfigRepo();
    doReturn(Optional.of(storeConfig)).when(repository).getStoreConfig(STORE);
    doReturn(admin).when(parent).getVeniceHelixAdmin();
    doReturn(store).when(admin).getStore(CLUSTER, STORE);
    doCallRealMethod().when(parent).isRTTopicDeletionPermittedByAllControllers(any(), anyString());
    return parent;
  }
}
