package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.controller.VeniceHelixAdmin.VERSION_ID_UNSET;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestClusterLevelConfigForActiveActiveReplication extends AbstractTestVeniceHelixAdmin {
  private static final long TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    setupCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    cleanupCluster();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewHybridStores() throws IOException {
    TopicManagerRepository originalTopicManagerRepository = prepareCluster(true, false, false);
    String storeNameHybrid = Utils.getUniqueString("test-store-hybrid");
    String pushJobId1 = "test-push-job-id-1";
    /**
     * Do not enable any store-level config for leader/follower mode or native replication feature.
     */
    veniceAdmin.createStore(clusterName, storeNameHybrid, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Add a version
     */
    veniceAdmin.addVersionAndTopicOnly(
        clusterName,
        storeNameHybrid,
        pushJobId1,
        VERSION_ID_UNSET,
        1,
        1,
        false,
        true,
        Version.PushType.STREAM,
        null,
        null,
        Optional.empty(),
        -1,
        1,
        Optional.empty(),
        false);

    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameHybrid).getVersions().size(), 1);
    // Check store level Active/Active is enabled or not
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeNameHybrid).isActiveActiveReplicationEnabled());
    veniceAdmin.updateStore(
        clusterName,
        storeNameHybrid,
        new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L));
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeNameHybrid).isActiveActiveReplicationEnabled());

    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewIncrementalPushStores() throws IOException {
    TopicManagerRepository originalTopicManagerRepository = prepareCluster(false, true, false);
    String storeNameIncremental = Utils.getUniqueString("test-store-incremental");
    String pushJobId1 = "test-push-job-id-1";
    /**
     * Do not enable any store-level config for leader/follower mode or native replication feature.
     */
    veniceAdmin.createStore(clusterName, storeNameIncremental, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Add a version
     */
    veniceAdmin.addVersionAndTopicOnly(
        clusterName,
        storeNameIncremental,
        pushJobId1,
        VERSION_ID_UNSET,
        1,
        1,
        false,
        true,
        Version.PushType.STREAM,
        null,
        null,
        Optional.empty(),
        -1,
        1,
        Optional.empty(),
        false);

    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameIncremental).getVersions().size(), 1);

    // Check store level Active/Active is enabled or not
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeNameIncremental, false);
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeNameIncremental).isActiveActiveReplicationEnabled());
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeNameIncremental, true);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeNameIncremental).isActiveActiveReplicationEnabled());
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeNameIncremental, false);
    // After inc push is disabled, even default A/A config for pure hybrid store is false, original store A/A config is
    // enabled.
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeNameIncremental).isActiveActiveReplicationEnabled());

    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewBatchOnlyStores() throws IOException {
    TopicManagerRepository originalTopicManagerRepository = prepareCluster(false, false, true);
    String storeNameBatchOnly = Utils.getUniqueString("test-store-batch-only");
    String pushJobId1 = "test-push-job-id-1";
    /**
     * Do not enable any store-level config for leader/follower mode or native replication feature.
     */
    veniceAdmin.createStore(clusterName, storeNameBatchOnly, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Add a version
     */
    veniceAdmin.addVersionAndTopicOnly(
        clusterName,
        storeNameBatchOnly,
        pushJobId1,
        VERSION_ID_UNSET,
        1,
        1,
        false,
        true,
        Version.PushType.STREAM,
        null,
        null,
        Optional.empty(),
        -1,
        1,
        Optional.empty(),
        false);

    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameBatchOnly).getVersions().size(), 1);

    // Store level Active/Active replication should be enabled since this store is a batch-only store by default
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isActiveActiveReplicationEnabled());

    // After updating the store to have incremental push enabled, it's A/A is still enabled
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeNameBatchOnly, true);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isActiveActiveReplicationEnabled());

    // Let's disable the A/A config for the store.
    veniceAdmin.setActiveActiveReplicationEnabled(clusterName, storeNameBatchOnly, false);
    Assert.assertFalse(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isActiveActiveReplicationEnabled());

    // After updating the store back to a batch-only store, it's A/A becomes enabled again
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeNameBatchOnly, false);
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isActiveActiveReplicationEnabled());

    // After updating the store to be a hybrid store, it's A/A should still be enabled.
    veniceAdmin.updateStore(
        clusterName,
        storeNameBatchOnly,
        new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L));
    Assert.assertTrue(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isActiveActiveReplicationEnabled());

    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  private TopicManagerRepository prepareCluster(
      boolean enableActiveActiveForHybrid,
      boolean enableActiveActiveForIncrementalPush,
      boolean enableActiveActiveForBatchOnly) throws IOException {
    veniceAdmin.stop(clusterName);
    veniceAdmin.close();
    Properties controllerProperties = getActiveActiveControllerProperties(
        clusterName,
        enableActiveActiveForHybrid,
        enableActiveActiveForIncrementalPush,
        enableActiveActiveForBatchOnly);
    veniceAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(
            new VeniceControllerConfig(new VeniceProperties(controllerProperties))),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress),
        pubSubTopicRepository,
        pubSubBrokerWrapper.getPubSubClientsFactory());

    veniceAdmin.initStorageCluster(clusterName);
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(anyString());
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    TestUtils
        .waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> veniceAdmin.isLeaderControllerFor(clusterName));
    Object createParticipantStoreFromProp = controllerProperties.get(PARTICIPANT_MESSAGE_STORE_ENABLED);
    if (createParticipantStoreFromProp != null && Boolean.parseBoolean(createParticipantStoreFromProp.toString())) {
      // Wait for participant store to finish materializing
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Store store =
            veniceAdmin.getStore(clusterName, VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName));
        Assert.assertNotNull(store);
        Assert.assertEquals(store.getCurrentVersion(), 1);
      });
    }
    return originalTopicManagerRepository;
  }

  private Properties getActiveActiveControllerProperties(
      String clusterName,
      boolean enableActiveActiveForHybrid,
      boolean enableActiveActiveForIncrementalPush,
      boolean enableActiveActiveForBatchOnly) throws IOException {
    Properties props = super.getControllerProperties(clusterName);
    props.setProperty(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, "true");
    // Enable Active/Active replication for hybrid stores through cluster-level config
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE,
        Boolean.toString(enableActiveActiveForHybrid));
    // Enable Active/Active replication for incremental stores through cluster-level config
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORE,
        Boolean.toString(enableActiveActiveForIncrementalPush));
    // Enable Active/Active replication for batch-only stores through cluster-level config
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE,
        Boolean.toString(enableActiveActiveForBatchOnly));
    return props;
  }
}
