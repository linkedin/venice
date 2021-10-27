package com.linkedin.venice.controller;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.Arg;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;


public class TestAdminToolEndToEnd {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;

  String clusterName;
  VeniceClusterWrapper venice;

  @BeforeClass
  public void setup() {
    Properties properties = new Properties();
    // Disable topic deletion
    properties.setProperty(TOPIC_CLEANUP_SLEEP_INTERVAL_BETWEEN_TOPIC_LIST_FETCH_MS, String.valueOf(Long.MAX_VALUE));
    venice = ServiceFactory.getVeniceCluster(1, 1, 1, 1, 100000, false, false, properties);
    clusterName = venice.getClusterName();
  }

  @AfterClass
  public void cleanup() {
    venice.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateClusterConfig() throws Exception {
    ZkClient zkClient = ZkClientFactory.newZkClient(venice.getZk().getAddress());
    HelixAdapterSerializer adapterSerializer = new HelixAdapterSerializer();
    HelixReadOnlyLiveClusterConfigRepository liveClusterConfigRepository =
        new HelixReadOnlyLiveClusterConfigRepository(zkClient, adapterSerializer, clusterName);

    String regionName = "region0";
    int kafkaFetchQuota = 1000;

    Assert.assertNotEquals(
        liveClusterConfigRepository.getConfigs().getServerKafkaFetchQuotaRecordsPerSecondForRegion(regionName),
        kafkaFetchQuota);

    String[] adminToolArgs =
        {"--update-cluster-config", "--url", venice.getMasterVeniceController().getControllerUrl(),
            "--cluster", clusterName,
            "--fabric", regionName,
            "--" + Arg.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND.getArgName(), String.valueOf(kafkaFetchQuota)};
    AdminTool.main(adminToolArgs);

    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      liveClusterConfigRepository.refresh();
      Assert.assertEquals(
          liveClusterConfigRepository.getConfigs().getServerKafkaFetchQuotaRecordsPerSecondForRegion(regionName),
          kafkaFetchQuota);
      Assert.assertTrue(liveClusterConfigRepository.getConfigs().isStoreMigrationAllowed());
    });

    String[] disallowStoreMigrationArg =
        {"--update-cluster-config", "--url", venice.getMasterVeniceController().getControllerUrl(),
            "--cluster", clusterName,
            "--" + Arg.ALLOW_STORE_MIGRATION.getArgName(), String.valueOf(false)};
    AdminTool.main(disallowStoreMigrationArg);

    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      liveClusterConfigRepository.refresh();
      Assert.assertFalse(liveClusterConfigRepository.getConfigs().isStoreMigrationAllowed());
    });

    try {
      String[] startMigrationArgs =
          {"--migrate-store", "--url", venice.getMasterVeniceController().getControllerUrl(),
              "--store", "anyStore",
              "--cluster-src", clusterName,
              "--cluster-dest", "anyCluster"};
      AdminTool.main(startMigrationArgs);
      Assert.fail("Store migration should be denied");
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("does not allow store migration"));
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testIncrementalPushBatchMigrationCommand() throws Exception {
    System.out.println("venice.getMasterVeniceController().getControllerUrl() = " + venice.getMasterVeniceController().getControllerUrl());
    try (ControllerClient controllerClient = new ControllerClient(clusterName, venice.getMasterVeniceController().getControllerUrl())) {
      // Create 2 inc push stores with Incremental Push to VT policy
      String testStoreName1 = TestUtils.getUniqueString("test-store");
      NewStoreResponse newStoreResponse = controllerClient.createNewStore(testStoreName1, "test",
          "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      ControllerResponse updateStoreResponse = controllerClient.updateStore(testStoreName1, new UpdateStoreQueryParams()
          .setIncrementalPushEnabled(true)
          .setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC));
      Assert.assertFalse(updateStoreResponse.isError());

      String testStoreName2 = TestUtils.getUniqueString("test-store");
      newStoreResponse = controllerClient.createNewStore(testStoreName2, "test",
          "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());
      updateStoreResponse = controllerClient.updateStore(testStoreName2, new UpdateStoreQueryParams()
          .setIncrementalPushEnabled(true)
          .setIncrementalPushPolicy(IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC));
      Assert.assertFalse(updateStoreResponse.isError());

      // Migrate all of them to Incremental Push to RT policy
      String[] adminToolArgs =
          {"--configure-incremental-push-for-cluster", "--url", venice.getMasterVeniceController().getControllerUrl(),
              "--cluster", clusterName,
              "--incremental-push-policy-to-filter", "PUSH_TO_VERSION_TOPIC",
              "--incremental-push-policy-to-apply", "INCREMENTAL_PUSH_SAME_AS_REAL_TIME"};
      AdminTool.main(adminToolArgs);

      // Wait and check whether all incremental push stores have been migrate to the right incremental push policy
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        StoreResponse storeResponse = controllerClient.getStore(testStoreName1);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertEquals(storeResponse.getStore().getIncrementalPushPolicy(), IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);
        Assert.assertNotNull(storeResponse.getStore().getHybridStoreConfig());
        storeResponse = controllerClient.getStore(testStoreName2);
        Assert.assertFalse(storeResponse.isError());
        Assert.assertEquals(storeResponse.getStore().getIncrementalPushPolicy(), IncrementalPushPolicy.INCREMENTAL_PUSH_SAME_AS_REAL_TIME);
        Assert.assertNotNull(storeResponse.getStore().getHybridStoreConfig());
      });
    }
  }
}
