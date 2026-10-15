package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_EXCLUSION_LIST;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_PUSH_STATUS_SYSTEM_STORE_VT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.linkedin.venice.pubsub.manager.TopicManager;
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
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT, "true");
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    admin = mock(VeniceHelixAdmin.class);
    manager = mock(TopicManager.class);
    topics = new PubSubTopicRepository();
    store = mock(Store.class);
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

  private String preparePushStatusStore() {
    String name = VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getSystemStoreName("batch-store");
    Store pushStatusStore = mock(Store.class);
    when(pushStatusStore.isHybrid()).thenReturn(true);
    doReturn(pushStatusStore).when(admin).getStore(config.getClusterName(), name);
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_PUSH_STATUS_SYSTEM_STORE_VT, "true");
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    return name;
  }

  @Test(dataProvider = "selections")
  public void testPushStatusUsesOwnerCohort(TopicPlacement.Selection selection, boolean alternative) {
    String name = preparePushStatusStore();
    when(placement.select(any())).thenReturn(selection);
    create(name, PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement).select(captured.capture());
    assertEquals(captured.getValue().getStoreName(), "batch-store");
    assertEquals(captured.getValue().getTopicName(), name + "_v1");
    assertEquals(captured.getValue().getDestinationAddress(), "destination.example:9092");
    assertEquals(captured.getValue().isLegacyAlternative(), true);
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

  @Test
  public void testHybridOwnerPushStatusRetainsLegacyPlacement() {
    String name = preparePushStatusStore();
    when(store.isHybrid()).thenReturn(true);
    create(name, PushType.BATCH);
    verify(placement, never()).select(any());
    verify(manager, never()).containsTopic(any());
    verify(manager).createTopic(
        topics.getTopic(name + "_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        true);
  }

  @DataProvider
  public Object[][] pushStatusStaticVetoes() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "pushStatusStaticVetoes")
  public void testPushStatusStaticVetoWins(boolean excluded) {
    String name = preparePushStatusStore();
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT, "true");
    if (excluded) {
      props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_PUSH_STATUS_SYSTEM_STORE_VT, "true");
      props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST, name);
      props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_EXCLUSION_LIST, name);
    }
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    create(name, PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement).select(captured.capture());
    assertEquals(captured.getValue().isLegacyAlternative(), false);
    verify(manager).createTopic(
        topics.getTopic(name + "_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        false);
  }

  @Test(dataProvider = "selections")
  public void testPushStatusOfMigratingOwnerUsesPlacement(TopicPlacement.Selection selection, boolean alternative) {
    String name = preparePushStatusStore();
    when(store.isMigrating()).thenReturn(true);
    when(placement.select(any())).thenReturn(selection);
    create(name, PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement).select(captured.capture());
    assertEquals(captured.getValue().getStoreName(), "batch-store");
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

  @Test
  public void testPushStatusMissingOwnerFailsBeforeCreate() {
    String name = preparePushStatusStore();
    doReturn(null).when(admin).getStore(config.getClusterName(), "batch-store");
    assertThrows(VeniceNoStoreException.class, () -> create(name, PushType.BATCH));
    verify(placement, never()).select(any());
    verifyNoInteractions(manager);
  }

  @Test
  public void testExistingPushStatusTopicBypassesPlacement() {
    String name = preparePushStatusStore();
    when(manager.containsTopic(topics.getTopic(name + "_v1"))).thenReturn(true);
    when(manager.containsTopicAndAllPartitionsAreOnline(topics.getTopic(name + "_v1"), 2)).thenReturn(true);
    when(manager.getTopicRetention(topics.getTopic(name + "_v1"))).thenReturn(Long.MAX_VALUE);
    create(name, PushType.BATCH);
    verify(placement, never()).select(any());
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @Test(dataProvider = "invalidRetention")
  public void testInvalidExistingPushStatusTopicFailsWithoutCreate(long retention) {
    String name = preparePushStatusStore();
    when(manager.containsTopic(topics.getTopic(name + "_v1"))).thenReturn(true);
    when(manager.containsTopicAndAllPartitionsAreOnline(topics.getTopic(name + "_v1"), 2)).thenReturn(true);
    when(manager.getTopicRetention(topics.getTopic(name + "_v1"))).thenReturn(retention);
    assertThrows(VeniceException.class, () -> create(name, PushType.BATCH));
    verify(placement, never()).select(any());
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testPushStatusShadowPreservesLegacyPlacement() {
    String name = preparePushStatusStore();
    when(placement.isShadowMode(config.getClusterName())).thenReturn(true);
    create(name, PushType.BATCH);
    verify(placement).select(any());
    verify(manager, never()).containsTopic(any());
    verify(manager).createTopic(
        topics.getTopic(name + "_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        true);
  }

  @Test
  public void testDisabledPushStatusPlacementDoesNotReadOwner() {
    String name = preparePushStatusStore();
    when(placement.isEnabled(config.getClusterName())).thenReturn(false);
    create(name, PushType.BATCH);
    verify(admin, never()).getStore(config.getClusterName(), "batch-store");
    verify(placement, never()).select(any());
    verify(manager, never()).containsTopic(any());
    verify(manager).createTopic(
        topics.getTopic(name + "_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        true);
  }

  @Test
  public void testDestinationAndVeto() {
    create("batch-store", PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(placement).select(captured.capture());
    assertEquals(captured.getValue().getDestinationAddress(), "destination.example:9092");
    assertEquals(captured.getValue().getStoreName(), "batch-store");
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        false);
  }

  @Test
  public void testPlacementCannotOverrideLegacyVeto() {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    create("batch-store", PushType.BATCH);
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        false);
  }

  @Test
  public void testExclusionWinsOverInclusionAndPlacementAdmission() {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT, "true");
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST, "batch-store");
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_EXCLUSION_LIST, "batch-store");
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(placement.select(any())).thenReturn(TopicPlacement.Selection.TREATMENT);
    create("batch-store", PushType.BATCH);
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        false);
  }

  @Test
  public void testInclusionCannotOverridePlacementVeto() {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST, "batch-store");
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    create("batch-store", PushType.BATCH);
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        false);
  }

  @DataProvider
  public Object[][] recoveryOperations() {
    return new Object[][] { { true, false, TopicPlacement.Selection.CONTROL, false },
        { true, false, TopicPlacement.Selection.TREATMENT, true },
        { false, true, TopicPlacement.Selection.CONTROL, false },
        { false, true, TopicPlacement.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "recoveryOperations")
  public void testMissingTopicDuringMigrationOrDataRecoveryUsesPlacement(
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
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        alternative);
  }

  @DataProvider
  public Object[][] selections() {
    return new Object[][] { { TopicPlacement.Selection.CONTROL, false }, { TopicPlacement.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "selections")
  public void testNewTopicUsesSelection(TopicPlacement.Selection selection, boolean alternative) {
    when(placement.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    verify(placement).select(any());
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        alternative);
  }

  @DataProvider
  public Object[][] shadowSelections() {
    return new Object[][] { { TopicPlacement.Selection.CONTROL, false }, { TopicPlacement.Selection.CONTROL, true },
        { TopicPlacement.Selection.TREATMENT, false }, { TopicPlacement.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "shadowSelections")
  public void testShadowPreservesLegacy(TopicPlacement.Selection selection, boolean legacyAlternative) {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT, Boolean.toString(legacyAlternative));
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(placement.isShadowMode(config.getClusterName())).thenReturn(true);
    when(placement.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    verify(placement).select(any());
    verify(manager, never()).containsTopic(any());
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        legacyAlternative);
  }

  @DataProvider
  public Object[][] disabledPlacements() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "disabledPlacements")
  public void testDisabledOrMissingPlacementPreservesLegacy(boolean missing) {
    if (missing) {
      admin.setTopicPlacement(null);
    } else {
      when(placement.isEnabled(config.getClusterName())).thenReturn(false);
    }
    create("batch-store", PushType.BATCH);
    verify(placement, never()).select(any());
    verify(manager, never()).containsTopic(any());
    verify(manager).createTopic(
        topics.getTopic("batch-store_v1"),
        2,
        config.getKafkaReplicationFactor(),
        true,
        false,
        config.getMinInSyncReplicas(),
        false,
        true);
  }

  @DataProvider
  public Object[][] excludedScope() {
    return new Object[][] { { "hybrid-store", PushType.BATCH, true },
        { "batch-store", PushType.STREAM_REPROCESSING, false },
        { VeniceSystemStoreUtils.getParticipantStoreNameForCluster("cluster"), PushType.BATCH, false },
        { VeniceSystemStoreUtils.getPushJobDetailsStoreName(), PushType.BATCH, false },
        { VeniceSystemStoreUtils.getMetaStoreName("batch-store"), PushType.BATCH, false } };
  }

  @Test(dataProvider = "excludedScope")
  public void testUnsupportedScopePreservesLegacy(String name, PushType type, boolean hybrid) {
    when(store.isHybrid()).thenReturn(hybrid);
    create(name, type);
    verifyNoInteractions(placement);
  }

  @DataProvider
  public Object[][] existingOperations() {
    return new Object[][] { { false, false }, { true, false }, { false, true } };
  }

  @Test(dataProvider = "existingOperations")
  public void testExistingTopicIsNotRecreated(boolean migrating, boolean recovering) {
    when(store.isMigrating()).thenReturn(migrating);
    when(manager.containsTopic(topics.getTopic("batch-store_v1"))).thenReturn(true);
    when(manager.containsTopicAndAllPartitionsAreOnline(topics.getTopic("batch-store_v1"), 2)).thenReturn(true);
    when(manager.getTopicRetention(topics.getTopic("batch-store_v1"))).thenReturn(Long.MAX_VALUE);
    Version version = new VersionImpl("batch-store", 1, "push", 2);
    if (recovering) {
      version.setDataRecoveryVersionConfig(new DataRecoveryVersionConfigImpl("source-region", false, 1));
    }
    admin.createBatchTopics(version, PushType.BATCH, manager, 2, config, false);
    verify(placement, never()).select(any());
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @DataProvider
  public Object[][] invalidRetention() {
    return new Object[][] { { 0L }, { PubSubConstants.PUBSUB_TOPIC_UNKNOWN_RETENTION } };
  }

  @Test(dataProvider = "invalidRetention")
  public void testTruncatedOrUnknownExistingTopicIsNotRevived(long retention) {
    when(manager.containsTopic(topics.getTopic("batch-store_v1"))).thenReturn(true);
    when(manager.containsTopicAndAllPartitionsAreOnline(topics.getTopic("batch-store_v1"), 2)).thenReturn(true);
    when(manager.getTopicRetention(topics.getTopic("batch-store_v1"))).thenReturn(retention);
    assertThrows(VeniceException.class, () -> create("batch-store", PushType.BATCH));
    verify(placement, never()).select(any());
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testUnavailableExistingTopicFailsWithoutCreate() {
    when(manager.containsTopic(topics.getTopic("batch-store_v1"))).thenReturn(true);
    assertThrows(VeniceException.class, () -> create("batch-store", PushType.BATCH));
    verify(placement, never()).select(any());
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testPlacementIsOptionalAtContextConstruction() {
    assertNull(new VeniceControllerContext.Builder().setMetricsRepository(null).build().getTopicPlacement());
  }
}
