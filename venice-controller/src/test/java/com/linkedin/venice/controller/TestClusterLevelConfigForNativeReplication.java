package com.linkedin.venice.controller;

import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.TopicManagerRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class TestClusterLevelConfigForNativeReplication extends AbstractTestVeniceHelixAdmin {
  @BeforeClass(alwaysRun = true)
  public void setup() throws Exception {
    setupCluster();
  }

  @AfterClass(alwaysRun = true)
  public void cleanup() {
    cleanupCluster();
  }

  @Override
  Properties getControllerProperties(String clusterName) throws IOException {
    Properties props = super.getControllerProperties(clusterName);
    // enable native replication for batch-only stores through cluster-level config
    props.setProperty(ENABLE_NATIVE_REPLICATION_AS_DEFAULT_FOR_BATCH_ONLY, "true");
    // enable L/F mode for all stores through cluster-level config
    props.setProperty(ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, "true");
    return props;
  }

  @Test
  public void testClusterLevelNativeReplicationConfigForNewStores() {
    TopicManagerRepository originalTopicManagerRepository = veniceAdmin.getTopicManagerRepository();

    TopicManager mockedTopicManager = mock(TopicManager.class);
    TopicManagerRepository mockedTopicManageRepository = mock(TopicManagerRepository.class);
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager();
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(String.class));
    doReturn(mockedTopicManager).when(mockedTopicManageRepository).getTopicManager(any(Pair.class));
    veniceAdmin.setTopicManagerRepository(mockedTopicManageRepository);
    String storeName = TestUtils.getUniqueString("test-store");
    String pushJobId1 = "test-push-job-id-1";
    /**
     * Do not enable any store-level config for leader/follower mode or native replication feature.
     */
    veniceAdmin.addStore(clusterName, storeName, "test-owner", KEY_SCHEMA, VALUE_SCHEMA);

    /**
     * Add a version
     */
    veniceAdmin.addVersionAndTopicOnly(clusterName, storeName, pushJobId1, 1, 1,
        false, true, Version.PushType.BATCH, null, null, Optional.empty(), -1, 1);
    // Version 1 should exist.
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).getVersions().size(), 1);
    // L/F should be enabled by cluster-level config
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).isLeaderFollowerModelEnabled(), true);
    // native replication should be enabled by cluster-level config
    Assert.assertEquals(veniceAdmin.getStore(clusterName, storeName).isNativeReplicationEnabled(), true);

    //set topic original topic manager back
    veniceAdmin.setTopicManagerRepository(originalTopicManagerRepository);
  }
}
