package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_EXCLUSION_LIST;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST;
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

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.placement.TopicPlacementContext;
import com.linkedin.venice.controller.placement.TopicPlacementPolicy;
import com.linkedin.venice.exceptions.VeniceException;
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


public class TopicPlacementPolicyTest {
  private VeniceHelixAdmin admin;
  private TopicManager manager;
  private VeniceControllerClusterConfig config;
  private TopicPlacementPolicy policy;
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
    policy = mock(TopicPlacementPolicy.class);
    when(policy.isEnabled(config.getClusterName())).thenReturn(true);
    when(policy.select(any())).thenReturn(TopicPlacementPolicy.Selection.CONTROL);
    doReturn(store).when(admin).getStore(eq(config.getClusterName()), any());
    doReturn(topics).when(admin).getPubSubTopicRepository();
    doCallRealMethod().when(admin).setTopicPlacementPolicy(any());
    doCallRealMethod().when(admin).createBatchTopics(any(), any(), any(), anyInt(), any(), anyBoolean(), any());
    admin.setTopicPlacementPolicy(policy);
    when(manager.getPubSubClusterAddress()).thenReturn("destination.example:9092");
  }

  private void create(String name, PushType type) {
    admin.createBatchTopics(
        new VersionImpl(name, 1, "push", 2),
        type,
        manager,
        2,
        config,
        false,
        TopicPlacementContext.Operation.NEW_VERSION);
  }

  @Test
  public void testDestinationAndVeto() {
    create("batch-store", PushType.BATCH);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(policy).select(captured.capture());
    assertEquals(captured.getValue().getDestinationAddress(), "destination.example:9092");
    assertEquals(captured.getValue().getStoreName(), "batch-store");
    assertEquals(captured.getValue().getOperation(), TopicPlacementContext.Operation.NEW_VERSION);
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
  public void testPolicyCannotOverrideLegacyVeto() {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(policy.select(any())).thenReturn(TopicPlacementPolicy.Selection.TREATMENT);
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
  public void testExclusionWinsOverInclusionAndPolicyAdmission() {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT, "true");
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_INCLUSION_LIST, "batch-store");
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_EXCLUSION_LIST, "batch-store");
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(policy.select(any())).thenReturn(TopicPlacementPolicy.Selection.TREATMENT);
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
  public void testInclusionCannotOverridePolicyVeto() {
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
    return new Object[][] { { true, false }, { false, true } };
  }

  @Test(dataProvider = "recoveryOperations")
  public void testMigrationAndDataRecoveryAreNotNewAdmission(boolean migrating, boolean recovering) {
    when(store.isMigrating()).thenReturn(migrating);
    Version version = new VersionImpl("batch-store", 1, "push", 2);
    if (recovering) {
      version.setDataRecoveryVersionConfig(new DataRecoveryVersionConfigImpl("source-region", false, 1));
    }
    admin.createBatchTopics(
        version,
        PushType.BATCH,
        manager,
        2,
        config,
        false,
        TopicPlacementContext.Operation.NEW_VERSION);
    ArgumentCaptor<TopicPlacementContext> captured = ArgumentCaptor.forClass(TopicPlacementContext.class);
    verify(policy).select(captured.capture());
    assertEquals(captured.getValue().getOperation(), TopicPlacementContext.Operation.RECOVERY);
  }

  @DataProvider
  public Object[][] selections() {
    return new Object[][] { { TopicPlacementPolicy.Selection.CONTROL, false },
        { TopicPlacementPolicy.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "selections")
  public void testNewTopicUsesSelection(TopicPlacementPolicy.Selection selection, boolean alternative) {
    when(policy.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    verify(policy).select(any());
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
    return new Object[][] { { TopicPlacementPolicy.Selection.CONTROL, false },
        { TopicPlacementPolicy.Selection.CONTROL, true }, { TopicPlacementPolicy.Selection.TREATMENT, false },
        { TopicPlacementPolicy.Selection.TREATMENT, true } };
  }

  @Test(dataProvider = "shadowSelections")
  public void testShadowPreservesLegacy(TopicPlacementPolicy.Selection selection, boolean legacyAlternative) {
    Properties props = TestVeniceControllerClusterConfig.getBaseSingleRegionProperties(false);
    props.put(CONTROLLER_PUBSUB_ALTERNATIVE_BACKEND_BATCH_USER_STORE_VT, Boolean.toString(legacyAlternative));
    config = new VeniceControllerClusterConfig(new VeniceProperties(props));
    when(policy.isShadowMode(config.getClusterName())).thenReturn(true);
    when(policy.select(any())).thenReturn(selection);
    create("batch-store", PushType.BATCH);
    verify(policy).select(any());
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
  public Object[][] disabledPolicies() {
    return new Object[][] { { true }, { false } };
  }

  @Test(dataProvider = "disabledPolicies")
  public void testDisabledOrMissingPolicyPreservesLegacy(boolean missing) {
    if (missing) {
      admin.setTopicPlacementPolicy(null);
    } else {
      when(policy.isEnabled(config.getClusterName())).thenReturn(false);
    }
    create("batch-store", PushType.BATCH);
    verify(policy, never()).select(any());
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
    verifyNoInteractions(policy);
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
    admin.createBatchTopics(
        new VersionImpl("batch-store", 1, "push", 2),
        PushType.BATCH,
        manager,
        2,
        config,
        false,
        recovering ? TopicPlacementContext.Operation.RECOVERY : TopicPlacementContext.Operation.NEW_VERSION);
    verify(policy, never()).select(any());
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
    verify(policy, never()).select(any());
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testUnavailableExistingTopicFailsWithoutCreate() {
    when(manager.containsTopic(topics.getTopic("batch-store_v1"))).thenReturn(true);
    assertThrows(VeniceException.class, () -> create("batch-store", PushType.BATCH));
    verify(policy, never()).select(any());
    verify(manager, never())
        .createTopic(any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testRequiredPolicyFailsAtContextConstruction() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new VeniceControllerContext.Builder().setMetricsRepository(null)
            .setTopicPlacementPolicyRequired(true)
            .build());
    assertNull(new VeniceControllerContext.Builder().setMetricsRepository(null).build().getTopicPlacementPolicy());
  }
}
