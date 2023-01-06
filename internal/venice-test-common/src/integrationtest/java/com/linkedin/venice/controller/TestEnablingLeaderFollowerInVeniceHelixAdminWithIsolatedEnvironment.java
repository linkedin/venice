package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Venice Helix Admin tests that run in isolated cluster. This suite is pretty time-consuming.
 * Please consider adding cases to {@link TestVeniceHelixAdminWithSharedEnvironment}.
 */
public class TestEnablingLeaderFollowerInVeniceHelixAdminWithIsolatedEnvironment extends AbstractTestVeniceHelixAdmin {
  private static final String STORE_NAME_1 = "testEnableLeaderFollowerForHybridStores";
  private static final String STORE_NAME_2 = "testEnableLeaderFollowerForIncrementalPushStores";

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    setupCluster(false);
  }

  @AfterMethod(alwaysRun = true)
  public void cleanUp() {
    cleanupCluster();
  }

  @Override
  protected Properties getControllerProperties(String clusterName) throws IOException {
    Properties properties = super.getControllerProperties(clusterName);
    properties.put(ConfigKeys.ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_HYBRID_STORES, true);
    properties.put(ConfigKeys.ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_INCREMENTAL_PUSH_STORES, true);
    return properties;
  }

  @Test
  public void testEnableLeaderFollower() throws IOException {
    veniceAdmin.createStore(clusterName, STORE_NAME_1, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.createStore(clusterName, STORE_NAME_2, "test", KEY_SCHEMA, VALUE_SCHEMA);
    // Store1 is a hybrid store.
    veniceAdmin.updateStore(
        clusterName,
        STORE_NAME_1,
        new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L));
    // Store2 is an incremental push store.
    veniceAdmin.updateStore(
        clusterName,
        STORE_NAME_2,
        new UpdateStoreQueryParams().setIncrementalPushEnabled(true)
            .setHybridRewindSeconds(1L)
            .setHybridOffsetLagThreshold(10));

    Assert.assertTrue(
        veniceAdmin.getStore(clusterName, STORE_NAME_1).isLeaderFollowerModelEnabled(),
        "Store1 is a hybrid store and L/F for hybrid stores config is true. L/F should be enabled.");
    Assert.assertTrue(
        veniceAdmin.getStore(clusterName, STORE_NAME_2).isLeaderFollowerModelEnabled(),
        "Store2 is an incremental push store and L/F for incremental push stores config is true. L/F should be enabled.");

    veniceAdmin.stop(clusterName);
    veniceAdmin.close();
    String storeName3 = "testEnableLeaderFollowerForAllStores";

    Properties properties = getControllerProperties(clusterName);
    properties.put(ConfigKeys.ENABLE_LEADER_FOLLOWER_AS_DEFAULT_FOR_ALL_STORES, true);

    veniceAdmin = new VeniceHelixAdmin(
        TestUtils.getMultiClusterConfigFromOneCluster(new VeniceControllerConfig(new VeniceProperties(properties))),
        new MetricsRepository(),
        D2TestUtils.getAndStartD2Client(zkAddress));
    veniceAdmin.initStorageCluster(clusterName);

    TestUtils
        .waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> veniceAdmin.isLeaderControllerFor(clusterName));
    // Store3 is a batch store
    veniceAdmin.createStore(clusterName, storeName3, "test", KEY_SCHEMA, VALUE_SCHEMA);

    Assert.assertTrue(
        veniceAdmin.getStore(clusterName, storeName3).isLeaderFollowerModelEnabled(),
        "Store3 is a batch store and L/F for all stores config is true. L/F should be enabled.");
  }
}
