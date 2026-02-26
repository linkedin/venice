package com.linkedin.venice.controller;

import static com.linkedin.venice.ConfigKeys.ALLOW_CLUSTER_WIPE;
import static com.linkedin.venice.ConfigKeys.IS_DARK_CLUSTER;
import static com.linkedin.venice.ConfigKeys.LOCAL_REGION_NAME;
import static com.linkedin.venice.ConfigKeys.LOG_COMPACTION_ENABLED;
import static com.linkedin.venice.ConfigKeys.REPUSH_ORCHESTRATOR_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.TOPIC_CLEANUP_DELAY_FACTOR;

import com.linkedin.venice.AdminTool;
import com.linkedin.venice.Arg;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.endToEnd.TestHybrid;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixAdapterSerializer;
import com.linkedin.venice.helix.HelixReadOnlyDarkClusterConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyLiveClusterConfigRepository;
import com.linkedin.venice.helix.ZkClientFactory;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAdminToolClusterConfig {
  private static final int TEST_TIMEOUT = 30 * Time.MS_PER_SECOND;

  String clusterName;
  VeniceClusterWrapper venice;

  @BeforeClass
  public void setUp() {
    Properties properties = new Properties();
    String regionName = "dc-0";
    properties.setProperty(LOCAL_REGION_NAME, regionName);
    properties.setProperty(ALLOW_CLUSTER_WIPE, "true");
    properties.setProperty(TOPIC_CLEANUP_DELAY_FACTOR, "0");

    // repushStore() configs
    properties.setProperty(REPUSH_ORCHESTRATOR_CLASS_NAME, TestHybrid.TestRepushOrchestratorImpl.class.getName());
    properties.setProperty(LOG_COMPACTION_ENABLED, "true");

    // dark cluster configs
    properties.setProperty(IS_DARK_CLUSTER, "true");

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .regionName(regionName)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(100000)
        .sslToStorageNodes(false)
        .sslToKafka(false)
        .extraProperties(properties)
        .build();
    venice = ServiceFactory.getVeniceCluster(options);
    clusterName = venice.getClusterName();
  }

  @AfterClass
  public void cleanUp() {
    venice.close();
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateClusterConfig() throws Exception {
    ZkClient zkClient = ZkClientFactory.newZkClient(venice.getZk().getAddress());
    HelixAdapterSerializer adapterSerializer = new HelixAdapterSerializer();
    HelixReadOnlyLiveClusterConfigRepository liveClusterConfigRepository =
        new HelixReadOnlyLiveClusterConfigRepository(zkClient, adapterSerializer, clusterName);

    String regionName = "dc-0";
    int kafkaFetchQuota = 1000;

    Assert.assertNotEquals(
        liveClusterConfigRepository.getConfigs().getServerKafkaFetchQuotaRecordsPerSecondForRegion(regionName),
        kafkaFetchQuota);

    String[] adminToolArgs = { "--update-cluster-config", "--url",
        venice.getLeaderVeniceController().getControllerUrl(), "--cluster", clusterName, "--fabric", regionName,
        "--" + Arg.SERVER_KAFKA_FETCH_QUOTA_RECORDS_PER_SECOND.getArgName(), String.valueOf(kafkaFetchQuota) };
    AdminTool.main(adminToolArgs);

    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      liveClusterConfigRepository.refresh();
      Assert.assertEquals(
          liveClusterConfigRepository.getConfigs().getServerKafkaFetchQuotaRecordsPerSecondForRegion(regionName),
          kafkaFetchQuota);
      Assert.assertTrue(liveClusterConfigRepository.getConfigs().isStoreMigrationAllowed());
    });

    String[] disallowStoreMigrationArg =
        { "--update-cluster-config", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--cluster",
            clusterName, "--" + Arg.ALLOW_STORE_MIGRATION.getArgName(), String.valueOf(false) };
    AdminTool.main(disallowStoreMigrationArg);

    TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
      liveClusterConfigRepository.refresh();
      Assert.assertFalse(liveClusterConfigRepository.getConfigs().isStoreMigrationAllowed());
    });

    try {
      String[] startMigrationArgs = { "--migrate-store", "--url", venice.getLeaderVeniceController().getControllerUrl(),
          "--store", "anyStore", "--cluster-src", clusterName, "--cluster-dest", "anyCluster" };
      AdminTool.main(startMigrationArgs);
      Assert.fail("Store migration should be denied");
    } catch (VeniceException e) {
      Assert.assertTrue(e.getMessage().contains("does not allow store migration"));
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdateDarkClusterConfig() throws Exception {
    ZkClient zkClient = ZkClientFactory.newZkClient(venice.getZk().getAddress());
    HelixAdapterSerializer adapterSerializer = new HelixAdapterSerializer();
    HelixReadOnlyDarkClusterConfigRepository darkClusterConfigRepository =
        new HelixReadOnlyDarkClusterConfigRepository(zkClient, adapterSerializer, clusterName);

    try (ControllerClient controllerClient =
        new ControllerClient(clusterName, venice.getLeaderVeniceController().getControllerUrl())) {
      String testStoreName1 = Utils.getUniqueString("test-store");
      NewStoreResponse newStoreResponse =
          controllerClient.createNewStore(testStoreName1, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());

      String testStoreName2 = Utils.getUniqueString("test-store");
      newStoreResponse = controllerClient.createNewStore(testStoreName2, "test", "\"string\"", "\"string\"");
      Assert.assertFalse(newStoreResponse.isError());

      String storesParam = testStoreName1 + "," + testStoreName2;
      String[] adminToolArgs =
          { "--update-dark-cluster-config", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--cluster",
              clusterName, "--" + Arg.STORES_TO_REPLICATE.getArgName(), storesParam };
      AdminTool.main(adminToolArgs);

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        darkClusterConfigRepository.refresh();
        Assert.assertTrue(darkClusterConfigRepository.getConfigs().getStoresToReplicate().contains(testStoreName1));
        Assert.assertTrue(darkClusterConfigRepository.getConfigs().getStoresToReplicate().contains(testStoreName2));
        Assert.assertEquals(darkClusterConfigRepository.getConfigs().getStoresToReplicate().size(), 2);
      });

      // Update the list to contain only test store 1
      String[] adminToolArgsWithOnlyOneStore =
          { "--update-dark-cluster-config", "--url", venice.getLeaderVeniceController().getControllerUrl(), "--cluster",
              clusterName, "--" + Arg.STORES_TO_REPLICATE.getArgName(), testStoreName1 };
      AdminTool.main(adminToolArgsWithOnlyOneStore);

      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, () -> {
        darkClusterConfigRepository.refresh();
        Assert.assertEquals(darkClusterConfigRepository.getConfigs().getStoresToReplicate().size(), 1);
        Assert.assertTrue(darkClusterConfigRepository.getConfigs().getStoresToReplicate().contains(testStoreName1));
        Assert.assertFalse(darkClusterConfigRepository.getConfigs().getStoresToReplicate().contains(testStoreName2));
      });
    }
  }
}
