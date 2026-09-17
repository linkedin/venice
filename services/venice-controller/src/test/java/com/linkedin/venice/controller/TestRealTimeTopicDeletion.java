package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.StoreInfo;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestRealTimeTopicDeletion {
  private static final String CLUSTER = "test-cluster";
  private static final String STORE = "test-store";
  private static final long RETENTION = 1000;
  private static final long MAX_RETENTION = 2000;
  private static final PubSubTopicRepository TOPICS = new PubSubTopicRepository();
  private static final PubSubTopic RT = TOPICS.getTopic(STORE + "_rt_v1");

  private VeniceHelixAdmin admin;
  private VeniceControllerMultiClusterConfig config;
  private TopicManager local;

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
    doReturn(RETENTION).when(config).getDeprecatedJobTopicRetentionMs();
    doReturn(MAX_RETENTION).when(config).getDeprecatedJobTopicMaxRetentionMs();
    doReturn(Collections.singleton(RT)).when(local).listTopics();
    doReturn(false, true).when(local).isTopicTruncated(RT, MAX_RETENTION);
  }

  @Test
  public void testAlreadyMarkedAndAbsentTopicsAreSuccessful() {
    doReturn(true).when(local).isTopicTruncated(RT, MAX_RETENTION);
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);
    verify(local, never()).updateTopicRetention(RT, RETENTION);

    doReturn(false, true).when(local).isTopicTruncated(RT, MAX_RETENTION);
    doReturn(false).when(local).updateTopicRetention(RT, RETENTION);
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);

    doReturn(false).when(local).isTopicTruncated(RT, MAX_RETENTION);
    doThrow(new PubSubTopicDoesNotExistException(RT)).when(local).updateTopicRetention(RT, RETENTION);
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);
  }

  @Test
  public void testUnconfirmedRetentionIsRetried() {
    doReturn(true).when(local).updateTopicRetention(RT, RETENTION);
    doReturn(false).when(local).isTopicTruncated(RT, MAX_RETENTION);
    VeniceRetriableException failure = expectThrows(
        VeniceRetriableException.class,
        () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
    assertEquals(failure.getSuppressed().length, 1);

    doReturn(true).when(local).isTopicTruncated(RT, MAX_RETENTION);
    admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false);
  }

  @Test
  public void testReadbackFailureIsNotTreatedAsAbsence() {
    doThrow(new PubSubClientException("Cannot read retention")).when(local).isTopicTruncated(RT, MAX_RETENTION);
    expectThrows(
        VeniceRetriableException.class,
        () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
  }

  @Test
  public void testOnlyExactStoreRealTimeTopicsAreMarked() {
    PubSubTopic separate = TOPICS.getTopic(STORE + "_rt_v2_sep");
    PubSubTopic similarStore = TOPICS.getTopic(STORE + "-other_rt");
    PubSubTopic systemStore = TOPICS.getTopic("venice_system_store_meta_store_" + STORE + "_rt");
    PubSubTopic version = TOPICS.getTopic(STORE + "_v1");
    doReturn(new LinkedHashSet<>(Arrays.asList(RT, separate, similarStore, systemStore, version))).when(local)
        .listTopics();
    doReturn(false, true).when(local).isTopicTruncated(separate, MAX_RETENTION);

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
    doReturn(false, true).when(first).isTopicTruncated(RT, MAX_RETENTION);
    doReturn(false, true).when(second).isTopicTruncated(RT, MAX_RETENTION);

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
    doReturn(false, true).when(second).isTopicTruncated(RT, MAX_RETENTION);

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
    doReturn(false, true).when(second).isTopicTruncated(RT, MAX_RETENTION);

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
  public void testInvalidRetentionFailsBeforeTopicMutation() {
    doReturn(MAX_RETENTION + 1).when(config).getDeprecatedJobTopicRetentionMs();
    expectThrows(VeniceException.class, () -> admin.cleanupRealTimeTopicsForStoreDeletion(CLUSTER, STORE, false));
    verify(local, never()).updateTopicRetention(any(), anyLong());
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
    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName());

    assertEquals(parent.isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName()), allowed);
    if (!allowed) {
      verify(admin, never()).isRTTopicDeletionPermittedByAllControllers(anyString(), anyString());
    }
  }

  @Test
  public void testParentCanCleanPendingDeletionWithoutStoreMetadata() {
    VeniceParentHelixAdmin parent = parentWithStore(null, true, CLUSTER);
    doReturn(false).when(admin).isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName());
    assertFalse(parent.isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName()));

    doReturn(true).when(admin).isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName());
    assertTrue(parent.isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName()));
  }

  @Test
  public void testCrossFabricEligibilityPreservesHybridToBatchProtection() {
    ControllerClient client = mock(ControllerClient.class);
    doReturn(Collections.singletonMap("child", client)).when(admin).getControllerClientMap(CLUSTER);
    doCallRealMethod().when(admin).isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName());
    StoreInfo store = new StoreInfo();
    Version hybrid = new VersionImpl(STORE, 1, "hybrid-push");
    hybrid.setHybridStoreConfig(new HybridStoreConfigImpl(10, 10, -1, BufferReplayPolicy.REWIND_FROM_EOP));
    store.setVersions(Arrays.asList(hybrid, new VersionImpl(STORE, 2, "batch-push")));
    StoreResponse response = new StoreResponse();
    response.setStore(store);
    doReturn(response).when(client).getStore(STORE);

    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName()));
    store
        .setVersions(Arrays.asList(new VersionImpl(STORE, 2, "batch-push"), new VersionImpl(STORE, 3, "batch-push-2")));
    assertTrue(admin.isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName()));

    doThrow(new VeniceException("Controller unavailable")).when(client).getStore(STORE);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName()));
    StoreResponse error = new StoreResponse();
    error.setError("Failed to read metadata");
    doReturn(error).when(client).getStore(STORE);
    assertFalse(admin.isRTTopicDeletionPermittedByAllControllers(CLUSTER, RT.getName()));
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
    doCallRealMethod().when(parent).isRTTopicDeletionPermittedByAllControllers(anyString(), anyString());
    return parent;
  }
}
