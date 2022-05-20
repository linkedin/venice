package com.linkedin.venice.controller;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoClusterException;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.helix.SafeHelixManager;
import com.linkedin.venice.helix.ZkStoreConfigAccessor;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Venice Helix Admin tests that run in isolated cluster. This suite is pretty time-consuming.
 * Please consider adding cases to {@link TestVeniceHelixAdminWithSharedEnvironment}.
 */
public class TestEnablingSSLPushInVeniceHelixAdminWithIsolatedEnvironment extends AbstractTestVeniceHelixAdmin {

  private static final String storeName1 = "testEnableSSLForPush1";
  private static final String storeName2 = "testEnableSSLForPush2";
  private static final String storeName3 = "testEnableSSLForPush3";

  @BeforeMethod(alwaysRun = true)
  public void setUp() throws Exception {
    setupCluster(false);
  }

  @AfterMethod(alwaysRun =  true)
  public void cleanUp() {
    cleanupCluster();
  }

  @Override
  protected Properties getControllerProperties(String clusterName) throws IOException {
    Properties properties = super.getControllerProperties(clusterName);
    properties.put(ConfigKeys.SSL_TO_KAFKA, true);
    properties.put(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getSSLAddress());
    properties.put(ConfigKeys.ENABLE_OFFLINE_PUSH_SSL_ALLOWLIST, true);
    properties.put(ConfigKeys.ENABLE_HYBRID_PUSH_SSL_ALLOWLIST,  false);
    properties.put(ConfigKeys.PUSH_SSL_ALLOWLIST, storeName1);
    return properties;
  }

  @Test
  public void testEnableSSLForPush() throws IOException {
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, ()->veniceAdmin.isLeaderControllerFor(clusterName));
    veniceAdmin.createStore(clusterName, storeName1, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.createStore(clusterName, storeName2, "test", KEY_SCHEMA, VALUE_SCHEMA);
    veniceAdmin.createStore(clusterName, storeName3, "test", KEY_SCHEMA, VALUE_SCHEMA);
    //store3 is hybrid store.
    veniceAdmin.updateStore(clusterName, storeName3, new UpdateStoreQueryParams()
        .setHybridRewindSeconds(1000L)
        .setHybridOffsetLagThreshold(1000L));

    Assert.assertTrue(veniceAdmin.isSSLEnabledForPush(clusterName, storeName1),
        "Store1 is in the allowlist, ssl should be enabled.");
    Assert.assertFalse(veniceAdmin.isSSLEnabledForPush(clusterName, storeName2),
        "Store2 is not in the allowlist, ssl should be disabled.");
    Assert.assertTrue(veniceAdmin.isSSLEnabledForPush(clusterName, storeName3),
        "Store3 is hybrid store, and ssl for nearline push is disabled, so by default ssl should be enabled because we turned on the cluster level ssl switcher.");
  }
}
