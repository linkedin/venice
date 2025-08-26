package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.DARK_CLUSTER_CONFIG;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.DarkClusterConfig;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.TimeUnit;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestHelixReadOnlyDarkClusterConfigRepository {
  private ZkClient zkClient;
  private String cluster = "test-cluster";
  private String clusterPath = "/" + cluster;
  private String clusterConfigPath = "/" + DARK_CLUSTER_CONFIG;
  private ZkServerWrapper zkServerWrapper;
  private HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  HelixReadOnlyDarkClusterConfigRepository darkClusterConfigRORepo;

  @BeforeClass
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
  }

  @AfterClass
  public void zkCleanup() {
    zkServerWrapper.close();
  }

  @BeforeMethod
  public void configureTest() {
    String zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    zkClient.create(clusterPath + clusterConfigPath, null, CreateMode.PERSISTENT);

    darkClusterConfigRORepo = new HelixReadOnlyDarkClusterConfigRepository(zkClient, adapter, cluster);
    darkClusterConfigRORepo.refresh();
  }

  @AfterMethod
  public void teardownTest() {
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
  }

  @Test
  public void testDarkClusterConfigGetsPropagated() {
    DarkClusterConfig darkClusterConfig = new DarkClusterConfig();
    darkClusterConfig.setStoresToReplicate(java.util.Arrays.asList("store1", "store2"));

    // Serialize dark configs and store in Zk
    zkClient.writeData(clusterPath + clusterConfigPath, darkClusterConfig);

    // Verify the data gets read by in HelixReadOnlyDarkClusterConfigRepository
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> darkClusterConfigRORepo.getConfigs() != null && darkClusterConfigRORepo.getConfigs()
            .getStoresToReplicate()
            .equals(java.util.Arrays.asList("store1", "store2")));
  }

  @Test
  public void testDeletedZNodeReturnsDefaultDarkClusterConfig() {
    DarkClusterConfig darkClusterConfig = new DarkClusterConfig();

    // Serialize dark configs and store in Zk
    zkClient.writeData(clusterPath + clusterConfigPath, darkClusterConfig);

    // Verify the data got persisted in Zk
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> darkClusterConfigRORepo.getConfigs() != null
            && darkClusterConfigRORepo.getConfigs().getStoresToReplicate().isEmpty());

    // Trigger a deletion of the dark config ZNode
    zkClient.delete(clusterPath + clusterConfigPath);

    // Verify that the repository returns default values for the configs
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> darkClusterConfigRORepo.getConfigs() != null
            && darkClusterConfigRORepo.getConfigs().getStoresToReplicate().isEmpty());
  }

  @Test
  public void testDarkClusterConfigGetsPropagatedOnServiceStart() {
    DarkClusterConfig darkClusterConfig = new DarkClusterConfig();
    darkClusterConfig.setStoresToReplicate(java.util.Arrays.asList("store1", "store2"));

    // Serialize dark configs and store in Zk
    zkClient.writeData(clusterPath + clusterConfigPath, darkClusterConfig);

    // Verify the data gets read by in HelixReadOnlyDarkClusterConfigRepository
    TestUtils.waitForNonDeterministicCompletion(
        3,
        TimeUnit.SECONDS,
        () -> darkClusterConfigRORepo.getConfigs() != null
            && darkClusterConfigRORepo.getConfigs().getStoresToReplicate().size() == 2);

    // Create a HelixReadOnlyDarkClusterConfigRepository to simulate service start up
    HelixReadOnlyDarkClusterConfigRepository darkClusterConfigRORepo2 =
        new HelixReadOnlyDarkClusterConfigRepository(zkClient, adapter, cluster);
    darkClusterConfigRORepo2.refresh();

    // Verify the data gets pulled in on service start up
    TestUtils.waitForNonDeterministicCompletion(
        30,
        TimeUnit.SECONDS,
        () -> darkClusterConfigRORepo2.getConfigs() != null && darkClusterConfigRORepo2.getConfigs()
            .getStoresToReplicate()
            .equals(java.util.Arrays.asList("store1", "store2")));
  }
}
