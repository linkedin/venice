package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
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

import static com.linkedin.venice.ConfigKeys.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


public class TestClusterLevelConfigForActiveActiveReplication extends AbstractTestVeniceHelixAdmin {
  @BeforeClass(alwaysRun = true)
  public void setup() throws Exception {
    setupCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanup() {
    cleanupCluster();
  }


  Properties getActiveActiveControllerProperties(String clusterName, boolean enableActiveActiveForHybrid,
      boolean enableActiveActiveForIncrementalPush) throws IOException {
    Properties props = super.getControllerProperties(clusterName);
    // enable L/F mode for all stores through cluster-level config
    props.setProperty(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, "true");
    // enable active active replication for hybrid stores stores through cluster-level config
    props.setProperty(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_HYBRID_STORE, Boolean.toString(enableActiveActiveForHybrid));
    // enable active active replication for incremental stores through cluster-level config
    props.setProperty(ENABLE_ACTIVE_ACTIVE_REPLICATION_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORE, Boolean.toString(enableActiveActiveForIncrementalPush));
    return props;
  }

  private TopicManagerRepository prepareCluster(boolean enableActiveActiveForHybrid, boolean enableActiveActiveForIncrementalPush) throws IOException {
    veniceAdmin.stop(clusterName);
    veniceAdmin.close();
    Properties properties = getActiveActiveControllerProperties(clusterName, enableActiveActiveForHybrid, enableActiveActiveForIncrementalPush);
    veniceAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(new VeniceControllerConfig(new VeniceProperties(properties))),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress)
    );

    veniceAdmin.start(clusterName);
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(Pair.class));
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    TestUtils.waitForNonDeterministicCompletion(5000, TimeUnit.MILLISECONDS, ()->veniceAdmin.isMasterController(clusterName));
    return originalTopicManagerRepository;
  }

  @Test
  public void testClusterLevelActiveActiveReplicationConfigForNewHybridStores() throws IOException {
    TopicManagerRepository originalTopicManagerRepository = prepareCluster(true, false);
    String storeNameHybrid = TestUtils.getUniqueString("test-store-hybrid");
    String pushJobId1 = "test-push-job-id-1";
    /**
     * Do not enable any store-level config for leader/follower mode or native replication feature.
     */
    veniceAdmin.addStore(clusterName, storeNameHybrid, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Add a version
     */
    veniceAdmin.addVersionAndTopicOnly(clusterName, storeNameHybrid, pushJobId1, 1, 1,
        false, true, Version.PushType.STREAM, null, null, Optional.empty(), -1, 1);
    // Version 1 should exist.

    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameHybrid).getVersions().size(), 1);
    // L/F should be enabled by cluster-level config
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameHybrid).isLeaderFollowerModelEnabled(), true);

    // Check store level active active is enabled or not
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameHybrid).isActiveActiveReplicationEnabled(), false);
    veniceAdmin.updateStore(clusterName, storeNameHybrid, new UpdateStoreQueryParams().setHybridRewindSeconds(1000L)
          .setHybridOffsetLagThreshold(1000L));
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameHybrid).isActiveActiveReplicationEnabled(), true);

    //set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }

  @Test
  public void testClusterLevelActiveActiveReplicationConfigForNewIncrementalPushStores() throws IOException {
    TopicManagerRepository originalTopicManagerRepository =  prepareCluster(false, true);
    String storeNameIncremental = TestUtils.getUniqueString("test-store-incremental");
    String pushJobId1 = "test-push-job-id-1";
    /**
     * Do not enable any store-level config for leader/follower mode or native replication feature.
     */

    veniceAdmin.addStore(clusterName, storeNameIncremental, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);
    /**
     * Add a version
     */
    veniceAdmin.addVersionAndTopicOnly(clusterName, storeNameIncremental, pushJobId1, 1, 1,
        false, true, Version.PushType.STREAM, null, null, Optional.empty(), -1, 1);
    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameIncremental).getVersions().size(), 1);
    // L/F should be enabled by cluster-level config
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameIncremental).isLeaderFollowerModelEnabled(), true);

    // Check store level active active is enabled or not
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeNameIncremental,false);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameIncremental).isActiveActiveReplicationEnabled(), false);
    veniceAdmin.setIncrementalPushEnabled(clusterName, storeNameIncremental,true);
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeNameIncremental).isActiveActiveReplicationEnabled(), true);

    //set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }
}
