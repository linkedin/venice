package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.utils.TestUtils;
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
public class TestEnablingSSLPushInVeniceHelixAdminWithIsolatedEnvironment extends AbstractTestVeniceHelixAdmin {
  private static final String STORE_NAME_1 = "testEnableSSLForPush1";
  private static final String STORE_NAME_2 = "testEnableSSLForPush2";
  private static final String STORE_NAME_3 = "testEnableSSLForPush3";

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
    properties.put(ConfigKeys.SSL_TO_KAFKA_LEGACY, true);
    properties.put(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS, pubSubBrokerWrapper.getSSLAddress());
    properties.put(ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_ALLOWLIST, true);
    properties.put(ConfigKeys.ENABLE_HYBRID_PUSH_SSL_ALLOWLIST, false);
    properties.put(ConfigKeys.PUSH_SSL_ALLOWLIST, STORE_NAME_1);
    return properties;
  }

  @Test
  public void testEnableSSLForPush() throws IOException {
    TestUtils
        .waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> veniceAdmin.isLeaderControllerFor(clusterName));
    veniceAdmin.createStore(clusterName, STORE_NAME_1, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.createStore(clusterName, STORE_NAME_2, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.createStore(clusterName, STORE_NAME_3, "test", KEY_SCHEMA, VALUE_SCHEMA);
    // store3 is hybrid store.
    veniceAdmin.updateStore(
        clusterName,
        STORE_NAME_3,
        new UpdateStoreQueryParams().setHybridRewindSeconds(1000L).setHybridOffsetLagThreshold(1000L));

    Assert.assertTrue(
        veniceAdmin.isSSLEnabledForPush(clusterName, STORE_NAME_1),
        "Store1 is in the allowlist, ssl should be enabled.");
    Assert.assertFalse(
        veniceAdmin.isSSLEnabledForPush(clusterName, STORE_NAME_2),
        "Store2 is not in the allowlist, ssl should be disabled.");
    Assert.assertTrue(
        veniceAdmin.isSSLEnabledForPush(clusterName, STORE_NAME_3),
        "Store3 is hybrid store, and ssl for nearline push is disabled, so by default ssl should be enabled because we turned on the cluster level ssl switcher.");
  }
}
