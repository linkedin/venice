package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_ALL;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_EXCLUSION_LIST;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_PUSH_STATUS_SYSTEM_STORE_VT;
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
import static org.testng.Assert.assertTrue;

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
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.InMemoryLogAppender;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.filter.LevelRangeFilter;
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

  private void configureStaticSettings(String name, boolean alternative) {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_ALL, "true");
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT, "false");
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_PUSH_STATUS_SYSTEM_STORE_VT, "false");
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST, name);
    if (!alternative) {
      props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_EXCLUSION_LIST, name);
    }
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
    assertEquals(captured.getValue().getMigrationCategory(), TopicPlacementContext.MigrationCategory.BATCH_VT);
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
    assertEquals(captured.getValue().getMigrationCategory(), TopicPlacementContext.MigrationCategory.BATCH_VT);
    verifyCreate(name, alternative);
  }

  @DataProvider
  public Object[][] selectionsAndStaticSettings() {
    return new Object[][] { { TopicPlacement.Selection.CONTROL, false }, { TopicPlacement.Selection.CONTROL, true },
        { TopicPlacement.Selection.TREATMENT, false }, { TopicPlacement.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "selectionsAndStaticSettings")
  public void testStaticSettingsDoNotOverrideActiveSelection(
      TopicPlacement.Selection selection,
      boolean staticAlternative) {
    String pushStatus = preparePushStatusStore();
    configureStaticSettings("batch-store," + pushStatus, staticAlternative);
    when(placement.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    create(pushStatus, PushType.BATCH);
    verifyCreate("batch-store", selection == TopicPlacement.Selection.TREATMENT);
    verifyCreate(pushStatus, selection == TopicPlacement.Selection.TREATMENT);
  }

  @DataProvider
  public Object[][] metadataExclusions() {
    List<Object[]> cases = new ArrayList<>();
    for (String marker: new String[] { "hybrid", "cdc", "pushStatus", "heartbeat", "blob", "malformed" }) {
      for (boolean pushStatus: new boolean[] { false, true }) {
        for (String policy: new String[] { "ACTIVE", "SHADOW", "LEGACY" }) {
          for (boolean staticAlternative: new boolean[] { false, true }) {
            cases.add(new Object[] { marker, pushStatus, policy, staticAlternative });
          }
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "metadataExclusions")
  public void testMetadataAdmission(String marker, boolean pushStatus, String policy, boolean staticAlternative) {
    String name = pushStatus ? preparePushStatusStore() : "batch-store";
    configureStaticSettings(name, staticAlternative);
    when(store.isHybrid()).thenReturn(marker.equals("hybrid"));
    when(store.isDaVinciPushStatusStoreEnabled()).thenReturn(marker.equals("pushStatus"));
    when(store.getIsDavinciHeartbeatReported()).thenReturn(marker.equals("heartbeat"));
    when(store.isBlobTransferEnabled()).thenReturn(marker.equals("blob"));
    if (marker.equals("cdc") || marker.equals("malformed")) {
      ViewConfigImpl view = new ViewConfigImpl(
          marker.equals("cdc") ? "com.linkedin.venice.views.ChangeCaptureView" : null,
          Collections.emptyMap());
      when(store.getViewConfigs()).thenReturn(Collections.singletonMap("view", view));
    }
    when(placement.isEnabled(config.getClusterName())).thenReturn(!policy.equals("LEGACY"));
    when(placement.isShadowMode(config.getClusterName())).thenReturn(policy.equals("SHADOW"));
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    create(name, PushType.BATCH);
    verify(placement, never()).select(any());
    verifyCreate(name, !policy.equals("ACTIVE") && staticAlternative);
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

  @Test(dataProvider = "selectionsAndStaticSettings")
  public void testShadowPreservesStaticPlacement(TopicPlacement.Selection selection, boolean staticAlternative) {
    String pushStatus = preparePushStatusStore();
    configureStaticSettings("batch-store," + pushStatus, staticAlternative);
    when(placement.isShadowMode(config.getClusterName())).thenReturn(true);
    when(placement.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    create(pushStatus, PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement, times(2)).select(captured.capture());
    for (TopicPlacementContext context: captured.getAllValues()) {
      assertEquals(context.isLegacyAlternative(), staticAlternative);
    }
    verify(manager, never()).containsTopic(any());
    verifyCreate("batch-store", staticAlternative);
    verifyCreate(pushStatus, staticAlternative);
  }

  @Test
  public void testPushStatusShadowComparesItsOwnStaticSettingRatherThanOwners() {
    String name = preparePushStatusStore();
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_PUSH_STATUS_SYSTEM_STORE_VT, "true");
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(placement.isShadowMode(config.getClusterName())).thenReturn(true);
    create(name, PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement).select(captured.capture());
    assertEquals(captured.getValue().getStoreName(), "batch-store");
    assertEquals(captured.getValue().isLegacyAlternative(), true);
    verifyCreate(name, true);
  }

  @Test(dataProvider = "selectionsAndStaticSettings")
  public void testShadowDecisionIsLogged(TopicPlacement.Selection selection, boolean staticAlternative) {
    configureStaticSettings("batch-store", staticAlternative);
    when(placement.isShadowMode(config.getClusterName())).thenReturn(true);
    when(placement.select(any())).thenReturn(selection);
    Logger logger = (Logger) LogManager.getLogger(VeniceHelixAdmin.class);
    InMemoryLogAppender logAppender = new InMemoryLogAppender.Builder().build();
    logAppender
        .addFilter(LevelRangeFilter.createFilter(Level.INFO, Level.INFO, Filter.Result.ACCEPT, Filter.Result.DENY));
    logAppender.start();
    logger.addAppender(logAppender);
    try {
      create("batch-store", PushType.BATCH);
      boolean differsFromStatic = staticAlternative != (selection == TopicPlacement.Selection.TREATMENT);
      assertTrue(
          logAppender.getLogs()
              .contains(
                  "Topic placement shadow: cluster=" + config.getClusterName()
                      + " store=batch-store topic=batch-store_v1 destination=destination.example:9092 staticAlternative="
                      + staticAlternative + " proposed=" + selection + " appliedAlternative=" + staticAlternative
                      + " differsFromStatic=" + differsFromStatic),
          "Shadow must log both matching and differing decisions at INFO");
      verifyCreate("batch-store", staticAlternative);
    } finally {
      logger.removeAppender(logAppender);
      logAppender.stop();
    }
  }

  @DataProvider
  public Object[][] disabledPlacements() {
    return new Object[][] { { true, false }, { true, true }, { false, false }, { false, true } };
  }

  @Test(dataProvider = "disabledPlacements")
  public void testDisabledOrMissingPlacementUsesStaticSettings(boolean missing, boolean staticAlternative) {
    String pushStatus = preparePushStatusStore();
    configureStaticSettings("batch-store," + pushStatus, staticAlternative);
    if (missing) {
      admin.setTopicPlacement(null);
    } else {
      when(placement.isEnabled(config.getClusterName())).thenReturn(false);
    }
    create(pushStatus, PushType.BATCH);
    verify(admin, never()).getStore(config.getClusterName(), "batch-store");
    create("batch-store", PushType.BATCH);
    verify(placement, never()).select(any());
    verifyCreate("batch-store", staticAlternative);
    verifyCreate(pushStatus, staticAlternative);
  }

  @DataProvider
  public Object[][] excludedScope() {
    return new Object[][] { { "batch-store", PushType.STREAM_REPROCESSING, false },
        { "batch-store", PushType.INCREMENTAL, false },
        { VeniceSystemStoreUtils.getParticipantStoreNameForCluster("cluster"), PushType.BATCH, false },
        { VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushType.BATCH, false },
        { VeniceSystemStoreUtils.getMetaStoreName("batch-store"), PushType.BATCH, false },
        { VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix(), PushType.BATCH, false } };
  }

  @Test(dataProvider = "excludedScope")
  public void testUnsupportedScopeUsesStaticPlacement(String name, PushType type, boolean hybrid) {
    when(store.isHybrid()).thenReturn(hybrid);
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    for (boolean staticAlternative: new boolean[] { false, true }) {
      configureStaticSettings(name, staticAlternative);
      create(name, type);
      verifyCreate(name, staticAlternative);
      if (type.isStreamReprocessing()) {
        verify(manager).createTopic(
            topics.getTopic(Version.composeStreamReprocessingTopic(name, 1)),
            2,
            config.getKafkaReplicationFactor(),
            true,
            false,
            config.getMinInSyncReplicas(),
            false,
            staticAlternative);
      }
    }
    verify(placement, never()).select(any());
  }

  @DataProvider
  public Object[][] realTimeStores() {
    return new Object[][] { { "hybrid-store" }, { VeniceSystemStoreUtils.getMetaStoreName("batch-store") },
        { VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("batch-store") },
        { VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix() } };
  }

  @Test(dataProvider = "realTimeStores")
  public void testRealTimeTopicsUseStaticPlacement(String name) {
    when(store.getName()).thenReturn(name);
    when(admin.getTopicManager()).thenReturn(manager);
    doCallRealMethod().when(admin).createOrUpdateRealTimeTopic(any(), any(), any(), any());
    Version version = new VersionImpl(name, 1, "push", 2);
    for (boolean staticAlternative: new boolean[] { false, true }) {
      configureStaticSettings(name, staticAlternative);
      when(admin.getControllerConfig(config.getClusterName())).thenReturn(config);
      admin.createOrUpdateRealTimeTopic(config.getClusterName(), store, version, topics.getTopic(name + "_rt"));
      verify(manager).createTopic(
          topics.getTopic(name + "_rt"),
          2,
          config.getKafkaReplicationFactorRTTopics(),
          StoreUtils.getExpectedRetentionTimeInMs(store, store.getHybridStoreConfig()),
          false,
          config.getMinInSyncReplicasRealTimeTopics(),
          false,
          staticAlternative,
          config.getUncleanLeaderElectionEnableRealTimeTopics());
    }
    verifyNoInteractions(placement);
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
