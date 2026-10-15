package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.placement.TopicPlacement;
import com.linkedin.venice.controller.placement.TopicPlacementContext;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.DataRecoveryVersionConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TopicPlacementTest {
  private VeniceHelixAdmin admin;
  private TopicManager manager;
  private VeniceControllerClusterConfig config;
  private TopicPlacement placement;
  private PubSubTopicRepository topics;
  private Store store;

  @BeforeMethod
  public void setUp() {
    config = new VeniceControllerClusterConfig(
        new VeniceProperties(TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false)));
    admin = mock(VeniceHelixAdmin.class);
    manager = mock(TopicManager.class);
    topics = new PubSubTopicRepository();
    store = mock(Store.class, RETURNS_DEEP_STUBS);
    placement = mock(TopicPlacement.class);
    when(placement.isEnabled(config.getClusterName())).thenReturn(true);
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.CONTROL);
    doReturn(store).when(admin).getStore(eq(config.getClusterName()), any());
    doReturn(topics).when(admin).getPubSubTopicRepository();
    doCallRealMethod().when(admin).setTopicPlacement(any());
    doCallRealMethod().when(admin).createBatchTopics(any(), any(), any(), anyInt(), any(), anyBoolean());
    admin.setTopicPlacement(placement);
    when(manager.getPubSubClusterAddress()).thenReturn("destination.example:9092");
  }

  private void create(String name, PushType type) {
    admin.createBatchTopics(new VersionImpl(name, 1, "push", 2), type, manager, 2, config, false);
  }

  private void verifyCreate(String name, boolean alternative) {
    verify(manager).createTopic(
        topics.getTopic(name + "_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        alternative);
  }

  private String preparePushStatusStore() {
    String name = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("batch-store");
    Store pushStatusStore = mock(Store.class);
    when(pushStatusStore.isHybrid()).thenReturn(true);
    doReturn(pushStatusStore).when(admin).getStore(config.getClusterName(), name);
    return name;
  }

  private void configureRetiredStaticSettings(String name) {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put("controller.pubsub.alternative.backend.all", "true");
    props.put("controller.pubsub.alternative.backend.batch.user.store.vt", "false");
    props.put("controller.pubsub.alternative.backend.push.status.system.store.vt", "false");
    props.put("controller.pubsub.alternative.backend.inclusion.list", name);
    props.put("controller.pubsub.alternative.backend.exclusion.list", name);
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
  }

  @DataProvider
  public Object[][] selections() {
    return new Object[][] { { TopicPlacement.Selection.CONTROL, false }, { TopicPlacement.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "selections")
  public void testNewTopicUsesSelectionWithoutStaticEnablement(
      TopicPlacement.Selection selection,
      boolean alternative) {
    when(placement.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement).select(captured.capture());
    assertEquals(captured.getValue().getClusterName(), config.getClusterName());
    assertEquals(captured.getValue().getStoreName(), "batch-store");
    assertEquals(captured.getValue().getTopicName(), "batch-store_v1");
    assertEquals(captured.getValue().getDestinationAddress(), "destination.example:9092");
    verifyCreate("batch-store", alternative);
  }

  @Test(dataProvider = "selections")
  public void testPushStatusUsesNonHybridOwnerCohort(TopicPlacement.Selection selection, boolean alternative) {
    String name = preparePushStatusStore();
    when(placement.select(any())).thenReturn(selection);
    create(name, PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement).select(captured.capture());
    assertEquals(captured.getValue().getStoreName(), "batch-store");
    assertEquals(captured.getValue().getTopicName(), name + "_v1");
    verifyCreate(name, alternative);
  }

  @Test(dataProvider = "selections")
  public void testRetiredStaticSettingsDoNotOverrideSelection(TopicPlacement.Selection selection, boolean alternative) {
    String pushStatus = preparePushStatusStore();
    configureRetiredStaticSettings("batch-store," + pushStatus);
    when(placement.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    create(pushStatus, PushType.BATCH);
    verifyCreate("batch-store", alternative);
    verifyCreate(pushStatus, alternative);
  }

  @Test
  public void testHybridOwnerPushStatusUsesDefault() {
    String name = preparePushStatusStore();
    when(store.isHybrid()).thenReturn(true);
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    create(name, PushType.BATCH);
    verify(placement, never()).select(any());
    verifyCreate(name, false);
  }

  @Test
  public void testPushStatusMissingOwnerFailsBeforeCreate() {
    String name = preparePushStatusStore();
    doReturn(null).when(admin).getStore(config.getClusterName(), "batch-store");
    assertThrows(VeniceNoStoreException.class, () -> create(name, PushType.BATCH));
    verify(placement, never()).select(any());
    verifyNoInteractions(manager);
  }

  @Test
  public void testMissingStoreFailsBeforeCreate() {
    doReturn(null).when(admin).getStore(config.getClusterName(), "batch-store");
    assertThrows(VeniceNoStoreException.class, () -> create("batch-store", PushType.BATCH));
    verifyNoInteractions(manager, placement);
  }

  @Test(dataProvider = "selections")
  public void testShadowUsesDefaultForUserAndPushStatusTopics(TopicPlacement.Selection selection, boolean alternative) {
    String pushStatus = preparePushStatusStore();
    configureRetiredStaticSettings("batch-store," + pushStatus);
    when(placement.isShadowMode(config.getClusterName())).thenReturn(true);
    when(placement.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    create(pushStatus, PushType.BATCH);
    verify(placement, times(2)).select(any());
    verify(manager, never()).containsTopic(any());
    verifyCreate("batch-store", false);
    verifyCreate(pushStatus, false);
  }

  @DataProvider
  public Object[][] disabledPlacements() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "disabledPlacements")
  public void testDisabledOrMissingPlacementUsesDefault(boolean missing) {
    String pushStatus = preparePushStatusStore();
    configureRetiredStaticSettings("batch-store," + pushStatus);
    if (missing) {
      admin.setTopicPlacement(null);
    } else {
      when(placement.isEnabled(config.getClusterName())).thenReturn(false);
    }
    create(pushStatus, PushType.BATCH);
    verify(admin, never()).getStore(config.getClusterName(), "batch-store");
    create("batch-store", PushType.BATCH);
    verify(placement, never()).select(any());
    verifyCreate("batch-store", false);
    verifyCreate(pushStatus, false);
  }

  @DataProvider
  public Object[][] excludedScope() {
    return new Object[][] { { "hybrid-store", PushType.BATCH, true },
        { "batch-store", PushType.STREAM_REPROCESSING, false }, { "batch-store", PushType.INCREMENTAL, false },
        { VeniceSystemStoreUtils.getParticipantStoreNameForCluster("cluster"), PushType.BATCH, false },
        { VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushType.BATCH, false },
        { VeniceSystemStoreUtils.getMetaStoreName("batch-store"), PushType.BATCH, false },
        { VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix(), PushType.BATCH, false } };
  }

  @Test(dataProvider = "excludedScope")
  public void testUnsupportedScopeUsesDefault(String name, PushType type, boolean hybrid) {
    configureRetiredStaticSettings(name);
    when(store.isHybrid()).thenReturn(hybrid);
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    create(name, type);
    verify(placement, never()).select(any());
    verifyCreate(name, false);
    if (type.isStreamReprocessing()) {
      verify(manager).createTopic(
          topics.getTopic(Version.composeStreamReprocessingTopic(name, 1)),
          2,
          config.getKafkaReplicationFactor(),
          true,
          false,
          config.getMinInSyncReplicas(),
          false,
          false);
    }
  }

  @DataProvider
  public Object[][] realTimeStores() {
    return new Object[][] { { "hybrid-store" }, { VeniceSystemStoreUtils.getMetaStoreName("batch-store") },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("batch-store") },
        { VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix() } };
  }

  @Test(dataProvider = "realTimeStores")
  public void testRealTimeTopicsUseDefault(String name) {
    configureRetiredStaticSettings(name);
    when(store.getName()).thenReturn(name);
    when(admin.getTopicManager()).thenReturn(manager);
    when(admin.getControllerConfig(config.getClusterName())).thenReturn(config);
    doCallRealMethod().when(admin).createOrUpdateRealTimeTopic(any(), any(), any(), any());
    Version version = new VersionImpl(name, 1, "push", 2);
    admin.createOrUpdateRealTimeTopic(config.getClusterName(), store, version, topics.getTopic(name + "_rt"));
    verifyNoInteractions(placement);
    verify(manager).createTopic(
        topics.getTopic(name + "_rt"),
        2,
        config.getKafkaReplicationFactorRTTopics(),
        StoreUtils.getExpectedRetentionTimeInMs(store, store.getHybridStoreConfig()),
        false,
        config.getMinInSyncReplicasRealTimeTopics(),
        false,
        false,
        config.getUncleanLeaderElectionEnableRealTimeTopics());
  }

  @DataProvider
  public Object[][] recoveryOperations() {
    return new Object[][] { { true, false, TopicPlacement.Selection.CONTROL, false },
        { true, false, TopicPlacement.Selection.TREATMENT, true },
        { false, true, TopicPlacement.Selection.CONTROL, false },
        { false, true, TopicPlacement.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "recoveryOperations")
  public void testMigrationOrDataRecoveryUsesPlacement(
      boolean migrating,
      boolean recovering,
      TopicPlacement.Selection selection,
      boolean alternative) {
    when(store.isMigrating()).thenReturn(migrating);
    when(placement.select(any())).thenReturn(selection);
    Version version = new VersionImpl("batch-store", 1, "push", 2);
    if (recovering) {
      version.setDataRecoveryVersionConfig(new DataRecoveryVersionConfigImpl("source-region", false, 1));
    }
    admin.createBatchTopics(version, PushType.BATCH, manager, 2, config, false);
    verify(placement).select(any());
    verifyCreate("batch-store", alternative);
  }

  @Test(dataProvider = "selections")
  public void testPushStatusOfMigratingOwnerUsesPlacement(TopicPlacement.Selection selection, boolean alternative) {
    String name = preparePushStatusStore();
    when(store.isMigrating()).thenReturn(true);
    when(placement.select(any())).thenReturn(selection);
    create(name, PushType.BATCH);
    verify(placement).select(any());
    verifyCreate(name, alternative);
  }

  @DataProvider
  public Object[][] existingTopics() {
    return new Object[][] { { false, true, Long.MAX_VALUE }, { false, false, Long.MAX_VALUE }, { false, true, 0L },
        { false, true, PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION }, { true, true, Long.MAX_VALUE },
        { true, false, Long.MAX_VALUE }, { true, true, 0L },
        { true, true, PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION } };
  }

  @Test(dataProvider = "existingTopics")
  public void testExistingTopicHandlingIsDelegatedToTopicManager(boolean pushStatus, boolean online, long retention) {
    String name = pushStatus ? preparePushStatusStore() : "batch-store";
    when(manager.containsTopic(topics.getTopic(name + "_v1"))).thenReturn(true);
    when(manager.containsTopicAndAllPartitionsAreOnline(topics.getTopic(name + "_v1"), 2)).thenReturn(online);
    when(manager.getTopicRetention(topics.getTopic(name + "_v1"))).thenReturn(retention);
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    create(name, PushType.BATCH);
    verify(placement).select(any());
    verify(manager, never()).containsTopic(any());
    verify(manager, never()).containsTopicAndAllPartitionsAreOnline(any(), any());
    verify(manager, never()).getTopicRetention(any(PubSubTopic.class));
    verifyCreate(name, true);
  }

  @Test
  public void testCreationFailurePropagates() {
    doThrow(new VeniceException("creation failed")).when(manager)
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
    assertThrows(VeniceException.class, () -> create("batch-store", PushType.BATCH));
    verify(placement).select(any());
  }

  @Test
  public void testNullSelectionFailsBeforeCreate() {
    when(placement.select(any())).thenReturn(null);
    assertThrows(NullPointerException.class, () -> create("batch-store", PushType.BATCH));
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testPlacementIsOptionalAtContextConstruction() {
    assertNull(new VeniceControllerContext.Builder().setMetricsRepository(null).build().getTopicPlacement());
  }
}
