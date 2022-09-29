package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES;
import static com.linkedin.venice.ConfigKeys.ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.controller.VeniceHelixAdmin.VERSION_ID_UNSET;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
    TopicManagerRepository originalTopicManagerRepository = prepareCluster(true, false);
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
    assertEquals(veniceAdmin.getStore(clusterName, storeNameHybrid).getVersions().size(), 1);
    // L/F should be enabled by cluster-level config
    assertTrue(veniceAdmin.getStore(clusterName, storeNameHybrid).isLeaderFollowerModelEnabled());

    // Check store level active active is enabled or not
    assertFalse(veniceAdmin.getStore(clusterName, storeNameHybrid).isActiveActiveReplicationEnabled());
    veniceAdmin.updateStore(
        clusterName,
        storeNameHybrid,
        new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L));
    assertTrue(veniceAdmin.getStore(clusterName, storeNameHybrid).isActiveActiveReplicationEnabled());

    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testClusterLevelActiveActiveReplicationConfigForNewBatchOnlyStores() throws IOException {
    TopicManagerRepository originalTopicManagerRepository = prepareCluster(false, true);
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
    assertEquals(veniceAdmin.getStore(clusterName, storeNameBatchOnly).getVersions().size(), 1);
    // L/F should be enabled by cluster-level config
    assertTrue(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isLeaderFollowerModelEnabled());

    // Store level active active should be enabled since this store is a batch-only store by default
    assertTrue(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isActiveActiveReplicationEnabled());

    // After updating the store to be a hybrid store, its A/A is disabled is again
    veniceAdmin.updateStore(
        clusterName,
        storeNameBatchOnly,
        new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L));
    assertFalse(veniceAdmin.getStore(clusterName, storeNameBatchOnly).isActiveActiveReplicationEnabled());
    // Set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  private TopicManagerRepository prepareCluster(
      boolean enableActiveActiveForHybrid,
      boolean enableActiveActiveForBatchOnly) throws IOException {
    veniceAdmin.stop(clusterName);
    veniceAdmin.close();
    Properties controllerProperties =
        getActiveActiveControllerProperties(clusterName, enableActiveActiveForHybrid, enableActiveActiveForBatchOnly);
    veniceAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(
            new VeniceControllerConfig(new VeniceProperties(controllerProperties))),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress));

    veniceAdmin.initStorageCluster(clusterName);
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(Pair.class));
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    TestUtils
        .waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> veniceAdmin.isLeaderControllerFor(clusterName));
    Object createParticipantStoreFromProp = controllerProperties.get(PARTICIPANT_MESSAGE_STORE_ENABLED);
    if (createParticipantStoreFromProp != null && Boolean.parseBoolean(createParticipantStoreFromProp.toString())) {
      // Wait for participant store to finish materializing
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        Store store =
            veniceAdmin.getStore(clusterName, VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName));
        assertNotNull(store);
        assertEquals(store.getCurrentVersion(), 1);
      });
    }
    return originalTopicManagerRepository;
  }

  private Properties getActiveActiveControllerProperties(
      String clusterName,
      boolean enableActiveActiveForHybrid,
      boolean enableActiveActiveForBatchOnly) throws IOException {
    Properties props = super.getControllerProperties(clusterName);
    // enable L/F mode for all stores through cluster-level config
    props.setProperty(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, "true");
    props.setProperty(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, "true");
    // enable active active replication for hybrid stores stores through cluster-level config
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE,
        Boolean.toString(enableActiveActiveForHybrid));
    // enable active active replication for batch-only stores through cluster-level config
    props.setProperty(
        ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY_STORE,
        Boolean.toString(enableActiveActiveForBatchOnly));
    return props;
  }
}
